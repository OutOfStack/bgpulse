package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/outofstack/bgpulse/internal/event"
	"github.com/outofstack/bgpulse/internal/ris"
	"golang.org/x/sync/semaphore"
)

const subscriptionAck = `{"type":"ris_subscribe_ok","data":{"subscription":{"host":"rrc21"},"socketOptions":{"acknowledge":true,"includeRaw":false}}}`

type testBroker struct {
	last func(context.Context) ([]byte, error)
	send func(context.Context, string, []byte) error
}

func (b testBroker) Last(ctx context.Context) ([]byte, error) { return b.last(ctx) }
func (b testBroker) Publish(ctx context.Context, id string, raw []byte) error {
	return b.send(ctx, id, raw)
}

func TestIngestReconnectAndLostAck(t *testing.T) {
	fixture, err := os.ReadFile("../../testdata/ris/rrc21-update.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := event.Normalize(fixture)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var lookups, connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if lookups.Load() < 2 {
			t.Error("connected before successful tail lookup")
		}
		connections.Add(1)
		conn, wsErr := websocket.Accept(w, r, nil)
		if wsErr != nil {
			t.Error(wsErr)
			return
		}
		defer conn.CloseNow()
		_, sub, wsErr := conn.Read(r.Context())
		if wsErr != nil || string(sub) != ris.Subscription {
			t.Errorf("subscription %q: %v", sub, wsErr)
			return
		}
		for _, raw := range [][]byte{[]byte(subscriptionAck), []byte(`{"type":"pong","data":null}`), fixture} {
			if wsErr = conn.Write(r.Context(), websocket.MessageText, raw); wsErr != nil {
				t.Error(wsErr)
				return
			}
		}
	}))
	defer server.Close()
	start := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	tail, _ := json.Marshal(event.Envelope{Version: 1, Kind: "source_message", Source: "ris", Namespace: "live", Collector: ris.Collector, ReceivedAt: start})
	var events []event.Envelope
	var uncertain []byte
	var uncertainID string
	dataCount := 0
	b := testBroker{
		last: func(context.Context) ([]byte, error) {
			if lookups.Add(1) == 1 {
				return nil, errors.New("lookup unavailable")
			}
			return tail, nil
		},
		send: func(_ context.Context, id string, raw []byte) error {
			var e event.Envelope
			if decErr := json.Unmarshal(raw, &e); decErr != nil {
				return decErr
			}
			if e.MessageID != id {
				t.Errorf("deduplication ID %s, envelope %s", id, e.MessageID)
			}
			if uncertain != nil {
				if id != uncertainID || !bytes.Equal(raw, uncertain) {
					t.Error("overtook or changed uncertain publish")
				}
				uncertain = nil
			} else if e.Kind == "source_message" && dataCount == 0 {
				uncertain = bytes.Clone(raw)
				uncertainID = id
				return errors.New("lost PubAck")
			}
			tail = bytes.Clone(raw)
			events = append(events, e)
			if e.Kind == "source_message" {
				dataCount++
				if dataCount == 2 {
					cancel()
				}
			}
			return nil
		},
	}
	if err = Run(ctx, b, "live", server.URL, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	if dataCount != 2 || lookups.Load() != 3 || connections.Load() != 2 {
		t.Fatalf("data=%d lookups=%d connections=%d", dataCount, lookups.Load(), connections.Load())
	}
	want := []string{"gap_open", "subscribed", "gap_close", "source_message", "disconnected", "gap_open", "subscribed", "gap_close", "source_message", "disconnected", "gap_open"}
	var kinds []string
	for _, e := range events {
		if e.Lifecycle != nil {
			kinds = append(kinds, e.Lifecycle.Type)
			continue
		}
		kinds = append(kinds, e.Kind)
		if e.MessageID != m.MessageID || e.EventTimeUS == nil || *e.EventTimeUS != m.EventTimeUS || !bytes.Equal(e.Payload, fixture) || e.PayloadHash != fmt.Sprintf("%x", sha256.Sum256(fixture)) {
			t.Errorf("incorrect source envelope: %+v", e)
		}
	}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("record order %v", kinds)
	}
	old, closed := events[0].Lifecycle.Gap, events[2].Lifecycle.Gap
	if !old.Start.Equal(start) || closed.ID != old.ID || !closed.Start.Equal(start) || !closed.End.Equal(events[3].ReceivedAt) || closed.Resolved {
		t.Fatalf("restart bounds %+v -> %+v", old, closed)
	}
	next, closeNext := events[5].Lifecycle.Gap, events[7].Lifecycle.Gap
	if events[3].Epoch == events[8].Epoch || !next.Start.Equal(events[3].ReceivedAt) || next.ID != closeNext.ID || !closeNext.End.Equal(events[8].ReceivedAt) {
		t.Fatalf("reconnect bounds %+v -> %+v", next, closeNext)
	}
}

func TestRestartWithoutTailOrAfterInterruptedGapOpen(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	var tail []byte
	b := testBroker{last: func(context.Context) ([]byte, error) { return tail, nil }}
	g, open, err := restartGap(t.Context(), b, "live", log)
	if err != nil || open || g.ID == "" || !g.Start.IsZero() {
		t.Fatalf("unknown boundary %+v open=%v err=%v", g, open, err)
	}
	e := event.Envelope{Namespace: "live", Version: 1, Source: "ris", Collector: ris.Collector, ReceivedAt: time.Now().UTC()}
	e.Control(event.Lifecycle{Type: "disconnected", Gap: g})
	tail, _ = json.Marshal(e)
	recovered, open, err := restartGap(t.Context(), b, "live", log)
	if err != nil || open || recovered.ID != g.ID || !recovered.Start.IsZero() {
		t.Fatalf("lost gap across restart: %+v %v %v", recovered, open, err)
	}
	e.Version = 2
	tail, _ = json.Marshal(e)
	if _, _, err = restartGap(t.Context(), b, "live", log); err == nil {
		t.Fatal("accepted unknown envelope version")
	}
}

func TestBoundedQueueAndDrainTimeout(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	for _, tt := range []struct {
		name   string
		slots  int
		budget int64
	}{{"items", 1, 8}, {"bytes", 2, 4}} {
		t.Run(tt.name, func(t *testing.T) {
			s := session{log: log, queue: make(chan record, tt.slots), bytes: semaphore.NewWeighted(tt.budget)}
			r := record{"id", []byte("data")}
			if err := s.enqueue(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if err := s.enqueue(ctx, r); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("queue exceeded bound: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b := testBroker{last: func(context.Context) ([]byte, error) { return nil, nil },
		send: func(attempt context.Context, _ string, _ []byte) error {
			cancel()
			<-attempt.Done()
			return attempt.Err()
		}}
	err := run(ctx, b, "live", "ws://127.0.0.1:1", log, 30*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "1 unaccepted") {
		t.Fatalf("report forced stop: %v", err)
	}
}

func TestOversizeKeepsGapOpen(t *testing.T) {
	for _, size := range []int{950_000, ris.MaxFrame + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer c.CloseNow()
				if _, _, err = c.Read(r.Context()); err != nil {
					return
				}
				if err = c.Write(r.Context(), websocket.MessageText, []byte(subscriptionAck)); err != nil {
					return
				}
				_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"ris_message","data":{"host":"rrc21","id":"`+strings.Repeat("x", size)+`"}}`))
			}))
			defer server.Close()
			var records []string
			b := testBroker{last: func(context.Context) ([]byte, error) { return nil, nil }, send: func(_ context.Context, _ string, raw []byte) error {
				var e event.Envelope
				if err := json.Unmarshal(raw, &e); err != nil {
					return err
				}
				if e.Lifecycle == nil {
					t.Error("published oversized source record")
					return nil
				}
				records = append(records, e.Lifecycle.Type)
				if e.Lifecycle.Type == "disconnected" {
					if e.Lifecycle.Reason != "oversize" || !e.Lifecycle.Gap.End.IsZero() {
						t.Errorf("wrong oversize boundary: %+v", e.Lifecycle)
					}
					cancel()
				}
				return nil
			}}
			if err := Run(ctx, b, "live", server.URL, slog.New(slog.DiscardHandler)); err != nil {
				t.Fatal(err)
			}
			if strings.Join(records, ",") != "gap_open,subscribed,disconnected" {
				t.Errorf("unexpected lifecycle %v", records)
			}
		})
	}
}
