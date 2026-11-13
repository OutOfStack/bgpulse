// Package jetstream declares the JetStream resources of one namespace and
// provisions or checks them.
package jetstream

import (
	"context"
	"fmt"
	"reflect"
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

// Check reports an error if the stream or consumer is missing or a declared
// field differs. It changes nothing. Only declared fields are compared,
// because the server fills in defaults for the rest.
func Check(ctx context.Context, js natsjs.JetStream, ns string) error {
	want := RawStream(ns)
	s, err := js.Stream(ctx, want.Name)
	if err != nil {
		return fmt.Errorf("stream %s: %w", want.Name, err)
	}
	got := s.CachedInfo().Config
	err = compare("stream "+want.Name, []field{
		{"Subjects", want.Subjects, got.Subjects},
		{"Storage", want.Storage, got.Storage},
		{"Retention", want.Retention, got.Retention},
		{"Replicas", want.Replicas, got.Replicas},
		{"MaxAge", want.MaxAge, got.MaxAge},
		{"MaxBytes", want.MaxBytes, got.MaxBytes},
		{"Discard", want.Discard, got.Discard},
		{"MaxMsgSize", want.MaxMsgSize, got.MaxMsgSize},
		{"Duplicates", want.Duplicates, got.Duplicates},
	})
	if err != nil {
		return err
	}

	c, err := s.Consumer(ctx, HistoryConsumer)
	if err != nil {
		return fmt.Errorf("consumer %s/%s: %w", want.Name, HistoryConsumer, err)
	}
	wantC, gotC := History(), c.CachedInfo().Config
	return compare("consumer "+want.Name+"/"+HistoryConsumer, []field{
		{"DeliverPolicy", wantC.DeliverPolicy, gotC.DeliverPolicy},
		{"AckPolicy", wantC.AckPolicy, gotC.AckPolicy},
		{"AckWait", wantC.AckWait, gotC.AckWait},
		{"MaxAckPending", wantC.MaxAckPending, gotC.MaxAckPending},
	})
}

type field struct {
	name      string
	want, got any
}

func compare(resource string, fields []field) error {
	for _, f := range fields {
		if !reflect.DeepEqual(f.want, f.got) {
			return fmt.Errorf("%s: %s is %v, want %v (setup does not change existing resources)",
				resource, f.name, f.got, f.want)
		}
	}
	return nil
}
