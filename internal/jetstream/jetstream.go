// Package jetstream declares the JetStream resources of one namespace and provisions or checks them.
package jetstream

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/outofstack/bgpulse/internal/event"
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
		MaxAge:     20 * time.Minute,
		MaxBytes:   10 << 30,
		Discard:    natsjs.DiscardNew,
		MaxMsgSize: event.RawRecordLimit,
		Duplicates: 10 * time.Minute,
	}
}

// QuarantineStream holds records the history consumer could not project, for 7 days or 1 GiB.
func QuarantineStream(ns string) natsjs.StreamConfig {
	return natsjs.StreamConfig{
		Name:       "BGPULSE_" + strings.ToUpper(ns) + "_QUARANTINE_V1",
		Subjects:   []string{QuarantineSubject(ns, "*")},
		Storage:    natsjs.FileStorage,
		Retention:  natsjs.LimitsPolicy,
		Replicas:   1,
		MaxAge:     7 * 24 * time.Hour,
		MaxBytes:   1 << 30,
		Discard:    natsjs.DiscardNew,
		MaxMsgSize: event.RawRecordLimit,
		Duplicates: 10 * time.Minute,
	}
}

// RawSubject is the raw subject for one RIS collector in namespace ns.
func RawSubject(ns, collector string) string {
	return "bgpulse." + ns + ".raw.v1.ris." + collector
}

// QuarantineSubject is the quarantine subject for one collector in namespace ns.
func QuarantineSubject(ns, collector string) string {
	return "bgpulse." + ns + ".quarantine.v1." + collector
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

// Ensure creates missing resources. The server accepts a create with an identical config and rejects a different one,
// so drift fails here instead of being silently overwritten.
func Ensure(ctx context.Context, js natsjs.JetStream, ns string) error {
	for _, cfg := range []natsjs.StreamConfig{RawStream(ns), QuarantineStream(ns)} {
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			return fmt.Errorf("stream %s: %w", cfg.Name, err)
		}
	}
	name := RawStream(ns).Name
	if _, err := js.CreateConsumer(ctx, name, History()); err != nil {
		return fmt.Errorf("consumer %s/%s: %w", name, HistoryConsumer, err)
	}
	return nil
}

// Check reports an error if the stream or consumer is missing or a declared field differs. It changes nothing. Only
// declared fields are compared, because the server fills in defaults for the rest.
func Check(ctx context.Context, js natsjs.JetStream, ns string) error {
	_, err := OpenInput(ctx, js, ns)
	return err
}

// Input is the history consumer of a checked raw stream, with its checked quarantine stream.
type Input struct {
	natsjs.Consumer
	// Created is when the raw stream was created. Sequences restart in a recreated stream.
	Created   time.Time
	Namespace string
	js        natsjs.JetStream
}

// OpenInput checks the raw stream, the quarantine stream and the history consumer without changing them.
func OpenInput(ctx context.Context, js natsjs.JetStream, ns string) (*Input, error) {
	s, err := checkStream(ctx, js, RawStream(ns))
	if err != nil {
		return nil, err
	}
	if _, err = checkStream(ctx, js, QuarantineStream(ns)); err != nil {
		return nil, err
	}
	info := s.CachedInfo()
	name := info.Config.Name
	c, err := s.Consumer(ctx, HistoryConsumer)
	if err != nil {
		return nil, fmt.Errorf("consumer %s/%s: %w", name, HistoryConsumer, err)
	}
	wantC, gotC := History(), c.CachedInfo().Config
	err = compare("consumer "+name+"/"+HistoryConsumer, []field{
		{"DeliverPolicy", wantC.DeliverPolicy, gotC.DeliverPolicy},
		{"AckPolicy", wantC.AckPolicy, gotC.AckPolicy},
		{"AckWait", wantC.AckWait, gotC.AckWait},
		{"MaxAckPending", wantC.MaxAckPending, gotC.MaxAckPending},
	})
	if err != nil {
		return nil, err
	}
	return &Input{Consumer: c, Created: info.Created, Namespace: ns, js: js}, nil
}

// Quarantine publishes the exact bytes of raw record msg to the collector's quarantine subject and waits for the
// PubAck. Headers carry the category, cause, raw sequence and worker build. The raw stream position deduplicates
// retries.
func (in *Input) Quarantine(ctx context.Context, msg natsjs.Msg, category string, cause error) error {
	md, err := msg.Metadata()
	if err != nil {
		return err
	}
	collector := msg.Subject()[strings.LastIndexByte(msg.Subject(), '.')+1:]
	reason := strings.Join(strings.Fields(cause.Error()), " ") // headers cannot hold line breaks
	if len(reason) > 160 {                                     // stay inside the header room ingest leaves under the 2 MiB payload limit
		reason = reason[:160]
	}
	q := nats.NewMsg(QuarantineSubject(in.Namespace, collector))
	q.Data = msg.Data()
	q.Header.Set("Bgpulse-Category", category)
	q.Header.Set("Bgpulse-Error", reason)
	q.Header.Set("Bgpulse-Raw-Seq", strconv.FormatUint(md.Sequence.Stream, 10))
	q.Header.Set("Bgpulse-Worker", worker)
	_, err = in.js.PublishMsg(ctx, q, natsjs.WithMsgID(fmt.Sprintf("%d:%d", in.Created.UnixMicro(), md.Sequence.Stream)),
		natsjs.WithExpectStream(QuarantineStream(in.Namespace).Name))
	return err
}

// worker identifies the build that quarantined a record.
var worker = func() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return s.Value
		}
	}
	return info.Main.Version
}()

func checkStream(ctx context.Context, js natsjs.JetStream, want natsjs.StreamConfig) (natsjs.Stream, error) {
	s, err := js.Stream(ctx, want.Name)
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", want.Name, err)
	}
	got := s.CachedInfo().Config
	err = compare("stream "+want.Name, []field{
		{"AllowDirect", want.AllowDirect, got.AllowDirect},
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
		return nil, err
	}
	return s, nil
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
