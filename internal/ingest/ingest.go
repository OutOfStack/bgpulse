// Package ingest publishes a single live RIS subscription in arrival order.
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/coder/websocket"
	"github.com/outofstack/bgpulse/internal/event"
	"github.com/outofstack/bgpulse/internal/ris"
	"golang.org/x/sync/semaphore"
)

const (
	// URL identifies this ingester to RIS Live.
	URL      = "wss://ris-live.ripe.net/v1/ws/?client=bgpulse-ingest"
	maxItems = 4096
	maxBytes = 32 << 20
	// Leave room for NATS headers in the server's 2 MiB payload limit.
	maxEnvelope    = event.RawRecordLimit - 512
	publishTimeout = 10 * time.Second
)

// Broker is the durable boundary for one collector subject.
type Broker interface {
	// Last returns nil only when the subject has no surviving record.
	Last(ctx context.Context) ([]byte, error)
	// Publish returns nil only after a PubAck for the expected stream.
	Publish(ctx context.Context, id string, data []byte) error
}

type record struct {
	id   string
	data []byte
}

type session struct {
	ns, epoch, subscriptionHash     string
	ordinal                         uint64
	log                             *slog.Logger
	queue                           chan record
	bytes                           *semaphore.Weighted
	pending                         atomic.Int64
	lastReceive, lastAck, lastEvent atomic.Int64
	lastPressureLog                 time.Time
}

func (s *session) backpressured() {
	if time.Since(s.lastPressureLog) >= 10*time.Second {
		s.lastPressureLog = time.Now()
		s.log.Warn("source backpressured", "pending", s.pending.Load())
	}
}

func (s *session) base(now time.Time) event.Envelope {
	return event.Envelope{Version: 1, Source: "ris", Namespace: s.ns, Collector: ris.Collector, Epoch: s.epoch, ReceivedAt: now}
}

func (s *session) control(ctx context.Context, kind, reason string, now time.Time, gap *event.Gap) error {
	e := s.base(now)
	e.Control(event.Lifecycle{Type: kind, Ordinal: s.ordinal, SubscriptionHash: s.subscriptionHash, Reason: reason, Gap: gap,
		LastReceiveUS: s.lastReceive.Load(), LastPubAckUS: s.lastAck.Load(), LastSourceUS: s.lastEvent.Load()})
	s.ordinal++
	s.log.Info("source lifecycle", "type", kind, "reason", reason, "received_at", now)
	r, err := encode(&e)
	if err != nil {
		return err
	}
	return s.enqueue(ctx, r)
}

var errEnvelopeSize = errors.New("envelope exceeds transport limit")

func encode(e *event.Envelope) (record, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return record{}, err
	}
	if len(data) > maxEnvelope {
		return record{}, errEnvelopeSize
	}
	return record{e.MessageID, data}, nil
}

func (s *session) enqueue(ctx context.Context, r record) error {
	s.pending.Add(1)
	n := int64(len(r.data))
	if !s.bytes.TryAcquire(n) {
		s.backpressured()
		if err := s.bytes.Acquire(ctx, n); err != nil {
			return err
		}
	}
	select {
	case s.queue <- r:
		return nil
	default:
		s.backpressured()
	}
	select {
	case s.queue <- r:
		return nil
	case <-ctx.Done():
		s.bytes.Release(n)
		return ctx.Err()
	}
}

func backoff(ctx context.Context, window *time.Duration) error {
	// Reconnect jitter is for load spreading.
	d := rand.N(*window) //nolint:gosec // No security-sensitive randomness.
	*window = min(*window*2, time.Minute)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func (s *session) publish(ctx context.Context, b Broker) error {
	for r := range s.queue {
		delay := time.Second
		for {
			attempt, cancel := context.WithTimeout(ctx, publishTimeout)
			err := b.Publish(attempt, r.id, r.data)
			cancel()
			if err == nil {
				break
			}
			s.log.Warn("waiting for PubAck", "error", err, "message_id", r.id, "pending", s.pending.Load())
			if err = backoff(ctx, &delay); err != nil {
				return err
			}
		}
		s.lastAck.Store(time.Now().UnixMicro())
		s.pending.Add(-1)
		s.bytes.Release(int64(len(r.data)))
	}
	return nil
}

func restartGap(ctx context.Context, b Broker, ns string, log *slog.Logger) (*event.Gap, bool, error) {
	delay := time.Second
	for {
		attempt, cancel := context.WithTimeout(ctx, publishTimeout)
		data, err := b.Last(attempt)
		cancel()
		if err != nil {
			log.Warn("last subject lookup failed", "error", err)
			if err = backoff(ctx, &delay); err != nil {
				return nil, false, err
			}
			continue
		}
		var last event.Envelope
		if data != nil {
			if err = json.Unmarshal(data, &last); err != nil {
				return nil, false, fmt.Errorf("decode last RIS envelope: %w", err)
			}
			if last.Version != 1 || last.Source != "ris" || last.Namespace != ns || last.Collector != ris.Collector || last.ReceivedAt.IsZero() {
				return nil, false, errors.New("incompatible last RIS envelope")
			}
			switch last.Kind {
			case event.SourceMessage:
			case event.SourceLifecycle:
				if last.Lifecycle == nil || last.Lifecycle.Gap == nil || last.Lifecycle.Gap.ID == "" {
					return nil, false, errors.New("missing lifecycle gap in last RIS envelope")
				}
				if last.Lifecycle.Gap.End.IsZero() {
					return last.Lifecycle.Gap, last.Lifecycle.Type != "disconnected", nil
				}
			default:
				return nil, false, fmt.Errorf("unknown last RIS envelope kind %q", last.Kind)
			}
		}
		return &event.Gap{ID: uuid.New().String(), Start: last.ReceivedAt, Reason: "startup_or_reconnect"}, false, nil
	}
}

func (s *session) capture(ctx, out context.Context, b Broker, url string, gap *event.Gap, open bool) (bool, error) {
	done := make(chan error, 1)
	go func() { done <- s.publish(out, b) }()
	err := s.read(ctx, out, url, gap, open)
	close(s.queue)
	pubErr := <-done
	s.log.Info("epoch ended", "pending_unaccepted", s.pending.Load(), "last_received_us", s.lastReceive.Load(), "last_puback_us", s.lastAck.Load(), "last_source_event_us", s.lastEvent.Load())
	return s.lastReceive.Load() != 0, errors.Join(err, pubErr)
}

func (s *session) read(ctx, out context.Context, url string, gap *event.Gap, open bool) error {
	if !open {
		if err := s.control(out, "gap_open", gap.Reason, time.Now().UTC(), gap); err != nil {
			return err
		}
	}
	acknowledged := false
	var last time.Time
	err := ris.Read(ctx, url, func(raw []byte, now time.Time) error {
		f, err := ris.Decode(raw, acknowledged)
		if err != nil {
			return err
		}
		switch f.Type {
		case "ris_subscribe_ok":
			acknowledged = true
			return s.control(out, "subscribed", "subscription_ack", now, gap)
		case "ris_message":
			e := s.base(now)
			e.Data(f.Data.ID, f.Data.Timestamp, raw)
			r, encodeErr := encode(&e)
			if encodeErr != nil {
				return encodeErr
			}
			s.lastReceive.Store(now.UnixMicro())
			if e.EventTimeUS != nil {
				s.lastEvent.Store(*e.EventTimeUS)
			}
			if gap != nil {
				gap.End = now
				if err = s.control(out, "gap_close", "data_resumed", now, gap); err != nil {
					s.pending.Add(1) // The received data also missed the drain deadline.
					return err
				}
				gap = nil
			}
			if err = s.enqueue(out, r); err != nil {
				return err
			}
			last = now
		}
		return nil
	})
	reason := "source_error"
	switch {
	case ctx.Err() != nil:
		reason = "shutdown"
	case errors.Is(err, websocket.ErrMessageTooBig), errors.Is(err, errEnvelopeSize):
		reason = "oversize"
	case errors.Is(err, context.DeadlineExceeded):
		reason = "read_timeout"
	}
	s.log.Info("source disconnected", "reason", reason, "error", err)
	if !acknowledged {
		return nil
	}
	newGap := gap == nil
	if newGap {
		gap = &event.Gap{ID: uuid.New().String(), Start: last, Reason: reason}
	}
	now := time.Now().UTC()
	if err = s.control(out, "disconnected", reason, now, gap); err != nil {
		return err
	}
	if newGap {
		return s.control(out, "gap_open", reason, now, gap)
	}
	return nil
}

// Run captures until cancellation and gives queued records 30 seconds to obtain PubAcks.
func Run(ctx context.Context, b Broker, ns, url string, log *slog.Logger) error {
	return run(ctx, b, ns, url, log, 30*time.Second)
}

func run(ctx context.Context, b Broker, ns, url string, log *slog.Logger, drain time.Duration) error {
	out, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() {
		select {
		case <-out.Done():
		case <-time.After(drain):
			cancel()
		}
	})
	defer stop()
	delay := time.Second
	subHash := fmt.Sprintf("%x", sha256.Sum256([]byte(ris.Subscription)))
	for ctx.Err() == nil {
		gap, open, err := restartGap(ctx, b, ns, log)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		epoch := uuid.New().String()
		s := session{ns: ns, epoch: epoch, subscriptionHash: subHash, log: log.With("connection_epoch", epoch, "subscription_sha256", subHash), queue: make(chan record, maxItems), bytes: semaphore.NewWeighted(maxBytes)}
		s.log.Info("source connecting", "collector", ris.Collector)
		dataSeen, err := s.capture(ctx, out, b, url, gap, open)
		if err != nil {
			return fmt.Errorf("ingest stopped with %d unaccepted records: %w", s.pending.Load(), err)
		}
		if dataSeen {
			delay = time.Second
		}
		if backoff(ctx, &delay) != nil {
			break
		}
	}
	return nil
}
