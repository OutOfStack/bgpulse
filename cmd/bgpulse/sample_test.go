package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/outofstack/bgpulse/internal/ris"
)

const sampleAck = `{"type":"ris_subscribe_ok","data":{"subscription":{"host":"rrc21"},"socketOptions":{"includeRaw":false,"acknowledge":true}}}`

func TestSampleReconnect(t *testing.T) {
	payload, err := os.ReadFile("../../testdata/ris/rrc21-update.json")
	if err != nil {
		t.Fatal(err)
	}
	payload = bytes.TrimSpace(payload)
	// Close each connection after one UPDATE. The second subscription gets an explicit source error.
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, acceptErr := websocket.Accept(w, r, nil)
		if acceptErr != nil {
			t.Error(acceptErr)
			return
		}
		defer conn.CloseNow()
		_, sub, readErr := conn.Read(r.Context())
		if readErr != nil || string(sub) != ris.Subscription {
			t.Errorf("subscription %s: %v", sub, readErr)
			return
		}
		for _, frame := range [][]byte{[]byte(sampleAck), payload} {
			if writeErr := conn.Write(r.Context(), websocket.MessageText, frame); writeErr != nil {
				t.Error(writeErr)
				return
			}
		}
		if connections.Add(1) == 2 {
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"ris_error","data":{"message":"overload"}}`))
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	s := sampleStats{sizes: histogram{}, fanout: histogram{}, types: map[string]int64{}}
	windows, err := s.captureHalf(ctx, server.URL, 1)
	if err == nil || !strings.Contains(err.Error(), "RIS error: overload") {
		t.Fatalf("got %v", err)
	}
	if len(windows) != 2 || s.sizes[len(payload)] != 2 || s.fanout[3] != 2 || s.types["UPDATE"] != 2 {
		t.Fatalf("windows=%+v stats=%+v", windows, s)
	}
	for _, w := range windows {
		if w.Acknowledged.IsZero() || w.First.Before(w.Acknowledged) || w.Messages != 1 || w.Last != w.First {
			t.Errorf("bad session boundaries: %+v", w)
		}
	}
}

func TestSampleOutcomes(t *testing.T) {
	s := sampleStats{sizes: histogram{}, fanout: histogram{}, types: map[string]int64{}}
	var w sampleWindow
	now := time.Now()
	const peer = `{"type":"ris_message","data":{"host":"rrc21","timestamp":1,"id":"peer-state","peer":"192.0.2.1","peer_asn":"123","type":"RIS_PEER_STATE","state":"down"}}`
	for _, frame := range []string{peer, strings.Replace(sampleAck, "rrc21", "rrc00", 1), strings.Replace(sampleAck, "false", "true", 1)} {
		if err := s.observe([]byte(frame), &w, now); err == nil {
			t.Errorf("accepted %s", frame)
		}
	}
	for _, frame := range []string{sampleAck, `{"type":"pong","data":null}`, peer, `{"type":"ris_message","data":{"host":"rrc21"}}`} {
		if err := s.observe([]byte(frame), &w, now); err != nil {
			t.Fatal(err)
		}
	}
	if w.Messages != 2 || s.invalid != 1 || s.fanout[0] != 1 || s.types["RIS_PEER_STATE"] != 1 {
		t.Fatalf("window=%+v stats=%+v", w, s)
	}
	if err := s.observe([]byte(strings.Replace(peer, "rrc21", "rrc20", 1)), &w, now); err == nil {
		t.Fatal("accepted another collector")
	}
	if retrySample(websocket.ErrMessageTooBig) {
		t.Fatal("retried an oversized message")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.captureHalf(ctx, sampleURL, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestSamplePercentiles(t *testing.T) {
	h := histogram{0: 50, 1: 45, 3: 4, 8: 1}
	want := map[string]int64{"count": 100, "sum": 65, "p50": 0, "p95": 1, "p99": 3, "max": 8}
	if got := h.summary(); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSampleTypeNames(t *testing.T) {
	s := sampleStats{sizes: histogram{}, fanout: histogram{}, types: map[string]int64{}}
	now := time.Now()
	w := sampleWindow{Acknowledged: now}
	observe := func(name string) {
		t.Helper()
		frame := `{"type":"ris_message","data":{"host":"rrc21","timestamp":1,"id":"x","peer":"192.0.2.1","peer_asn":"1","type":"` + name + `"}}`
		if err := s.observe([]byte(frame), &w, now); err != nil {
			t.Fatal(err)
		}
	}
	observe(strings.Repeat("X", 65))
	observe("other")
	for i := 1; i <= 15; i++ {
		observe(strings.Repeat("A", i))
	}
	observe("EXTRA")
	observe("other")
	if len(s.types) != 16 || s.types["other"] != 2 || s.typeOverflow != 2 || s.fanout[0] != 19 {
		t.Fatalf("types=%v overflow=%d fanout=%v", s.types, s.typeOverflow, s.fanout)
	}
}

func TestSampleDeadlineDuringReconnect(t *testing.T) {
	// Each connection ends after one observed frame. Both half-deadlines fall in the one-second retry delay.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		if _, _, err = conn.Read(r.Context()); err != nil {
			t.Error(err)
			return
		}
		for _, raw := range []string{sampleAck, `{"type":"ris_message","data":{"host":"rrc21"}}`} {
			if err = conn.Write(r.Context(), websocket.MessageText, []byte(raw)); err != nil {
				t.Error(err)
				return
			}
		}
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := measureSample(t.Context(), server.URL, time.Second, &out); err != nil {
		t.Fatal(err)
	}
	var report struct {
		Complete bool           `json:"complete"`
		Windows  []sampleWindow `json:"windows"`
		Invalid  int64          `json:"invalid_messages"`
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Complete || len(report.Windows) != 2 || report.Invalid != 2 || report.Windows[1].Messages != 1 {
		t.Fatalf("unexpected completed report: %s", out.Bytes())
	}
}

func TestSampleCanceledReport(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var out bytes.Buffer
	if err := measureSample(ctx, sampleURL, time.Minute, &out); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var report struct {
		Complete *bool `json:"complete"`
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.Complete == nil || *report.Complete {
		t.Fatalf("partial report: %s, error %v", out.Bytes(), err)
	}
}
