// Package persist runs the durable history consumer: it projects each raw record to Scylla and ACKs it only after
// every required write succeeds or the record is durably quarantined.
package persist

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"time"

	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/outofstack/bgpulse/internal/event"
	"github.com/outofstack/bgpulse/internal/jetstream"
	"github.com/outofstack/bgpulse/internal/store"
	"golang.org/x/sync/errgroup"
)

const (
	// Workers bounds messages in flight. Each writes its rows one at a time, which stays inside the 64-write budget.
	workers    = 8
	fetchBatch = 32
	maxBackoff = 30 * time.Second
)

const badEnvelope = "invalid_envelope"

// errIncompatible stops the consumer: this binary cannot project the record, and it is not bad source data.
var errIncompatible = errors.New("incompatible raw record")

// invalid is supported-schema input that can never be projected. It goes to quarantine.
type invalid struct {
	category string
	err      error
}

func (e *invalid) Error() string { return e.category + ": " + e.err.Error() }

// Consumer projects one namespace's raw stream.
type Consumer struct {
	In      *jetstream.Input
	History *store.History
	Sources *store.Sources
	Log     *slog.Logger
}

// Run consumes until ctx ends or a record is incompatible. Unacknowledged records are redelivered after AckWait.
func (c *Consumer) Run(ctx context.Context) error {
	it, err := c.In.Messages(natsjs.PullMaxMessages(fetchBatch),
		natsjs.WithMessagesErrOnMissingHeartbeat(false)) // a missed heartbeat re-pulls instead of stopping
	if err != nil {
		return err
	}
	defer it.Stop()
	var g errgroup.Group
	g.SetLimit(workers)
	for {
		msg, err := it.Next(natsjs.NextContext(ctx))
		if err != nil {
			if fatal := g.Wait(); fatal != nil {
				return fatal
			}
			if ctx.Err() != nil || errors.Is(err, natsjs.ErrMsgIteratorClosed) {
				return nil
			}
			return err
		}
		g.Go(func() error {
			err := c.handle(ctx, msg)
			if err != nil {
				it.Stop()
			}
			return err
		})
	}
}

// handle settles one record. It returns an error only when the consumer must stop.
func (c *Consumer) handle(ctx context.Context, msg natsjs.Msg) error {
	md, err := msg.Metadata()
	if err != nil {
		return err
	}
	err = c.project(ctx, msg.Data(), md.Sequence.Stream)
	log := c.Log
	if err != nil {
		log = log.With("raw_seq", md.Sequence.Stream, "deliveries", md.NumDelivered)
	}
	var bad *invalid
	switch {
	case errors.Is(err, errIncompatible):
		log.Error("stopping on incompatible record", "error", err)
		return err
	case errors.Is(err, store.ErrExpired):
		log.Warn("expired_before_projection", "error", err)
	case errors.As(err, &bad):
		if err = c.In.Quarantine(ctx, msg, bad.category, bad.err); err != nil {
			log.Warn("quarantine failed, record stays pending", "category", bad.category, "error", err)
			nak(msg, md, log)
			return nil
		}
		log.Warn("quarantined", "category", bad.category, "error", bad.err)
	case err != nil:
		log.Warn("projection failed, will retry", "error", err)
		nak(msg, md, log)
		return nil
	}
	if err = msg.DoubleAck(ctx); err != nil {
		log.Warn("ack not confirmed, record may be redelivered", "raw_seq", md.Sequence.Stream, "error", err)
	}
	return nil
}

// nak asks for redelivery after 1 s, doubling per delivery up to 30 s.
func nak(msg natsjs.Msg, md *natsjs.MsgMetadata, log *slog.Logger) {
	if err := msg.NakWithDelay(backoff(md.NumDelivered)); err != nil {
		log.Warn("nak failed, redelivery waits for AckWait", "error", err)
	}
}

func backoff(deliveries uint64) time.Duration {
	return min(time.Second<<min(max(deliveries, 1)-1, 5), maxBackoff)
}

func (c *Consumer) project(ctx context.Context, data []byte, seq uint64) error {
	e, m, err := decode(data, c.In.Namespace)
	if err != nil {
		return err
	}
	if m == nil {
		return c.Sources.Apply(ctx, e, c.In.Created, seq)
	}
	if err = c.History.Write(ctx, m, e.Payload); errors.Is(err, store.ErrFutureEvent) {
		return &invalid{"future_event", err}
	}
	return err
}

// decode validates a raw record. It returns the normalized message of a source record, or a nil message for a
// lifecycle record.
func decode(data []byte, ns string) (*event.Envelope, *event.Message, error) {
	var e event.Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		// A newer schema may not fit this struct; only its version decides.
		if json.Unmarshal(data, &struct {
			Version *int `json:"schema_version"`
		}{&e.Version}) != nil || e.Version <= 1 {
			return nil, nil, &invalid{badEnvelope, err}
		}
	}
	if e.Version < 1 {
		return nil, nil, &invalid{badEnvelope, errors.New("missing schema_version")}
	}
	if e.Version > 1 {
		return nil, nil, fmt.Errorf("%w: schema_version %d", errIncompatible, e.Version)
	}
	if e.Namespace != ns || e.ReceivedAt.IsZero() {
		return nil, nil, &invalid{badEnvelope, fmt.Errorf("namespace %q, received_at %v", e.Namespace, e.ReceivedAt)}
	}
	switch e.Kind {
	case event.SourceMessage:
		m, err := event.Normalize(e.Payload)
		if err != nil {
			return nil, nil, &invalid{"invalid_source", err}
		}
		if m.MessageID != e.MessageID {
			return nil, nil, &invalid{"invalid_source", errors.New("message_id does not match the payload")}
		}
		return &e, m, nil
	case event.SourceLifecycle:
		if l := e.Lifecycle; l == nil || l.Type == "" || (l.Gap != nil && l.Gap.ID == "") {
			return nil, nil, &invalid{"invalid_lifecycle", errors.New("missing type or gap ID")}
		}
		return &e, nil, nil
	}
	return nil, nil, fmt.Errorf("%w: kind %q", errIncompatible, e.Kind)
}
