// Package jetstream declares the JetStream resources of one namespace and
// provisions or checks them.
package jetstream

import (
	"context"
	"fmt"
	"strings"
	"time"

	natsjs "github.com/nats-io/nats.go/jetstream"
)

// HistoryConsumer is the durable consumer that persists raw messages to history.
const HistoryConsumer = "history-v1"

// RawStream is the declared raw stream for namespace ns.
func RawStream(ns string) natsjs.StreamConfig {
	return natsjs.StreamConfig{
		Name:       "BGPULSE_" + strings.ToUpper(ns) + "_RAW_V1",
		Subjects:   []string{RawSubject(ns, "*")},
		Storage:    natsjs.FileStorage,
		Retention:  natsjs.LimitsPolicy,
		Replicas:   1,
		MaxAge:     24 * time.Hour,
		MaxBytes:   10 << 30,
		Discard:    natsjs.DiscardNew,
		MaxMsgSize: 2 << 20,
		Duplicates: 10 * time.Minute,
	}
}

// RawSubject is the raw subject for one RIS collector in namespace ns.
func RawSubject(ns, collector string) string {
	return "bgpulse." + ns + ".raw.v1.ris." + collector
}

// History is the declared history consumer.
func History() natsjs.ConsumerConfig {
	return natsjs.ConsumerConfig{
		Durable:       HistoryConsumer,
		DeliverPolicy: natsjs.DeliverAllPolicy,
		AckPolicy:     natsjs.AckExplicitPolicy,
		AckWait:       60 * time.Second,
		MaxAckPending: 256,
	}
}

// Ensure creates missing resources. The server accepts a create with an
// identical config and rejects a different one, so drift fails here instead of
// being silently overwritten.
func Ensure(ctx context.Context, js natsjs.JetStream, ns string) error {
	cfg := RawStream(ns)
	if _, err := js.CreateStream(ctx, cfg); err != nil {
		return fmt.Errorf("stream %s: %w", cfg.Name, err)
	}
	if _, err := js.CreateConsumer(ctx, cfg.Name, History()); err != nil {
		return fmt.Errorf("consumer %s/%s: %w", cfg.Name, HistoryConsumer, err)
	}
	return nil
}

// Check reports an error if the stream or consumer is missing. It changes nothing.
func Check(ctx context.Context, js natsjs.JetStream, ns string) error {
	name := RawStream(ns).Name
	if _, err := js.Consumer(ctx, name, HistoryConsumer); err != nil {
		return fmt.Errorf("consumer %s/%s: %w", name, HistoryConsumer, err)
	}
	return nil
}
