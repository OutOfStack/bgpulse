//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/outofstack/bgpulse/internal/jetstream"
)

// BenchmarkSequentialPubAck measures one outstanding publish against an isolated RF=1 file stream.
func BenchmarkSequentialPubAck(b *testing.B) {
	payload, err := os.ReadFile("../../testdata/ris/rrc21-update.json")
	if err != nil {
		b.Fatal(err)
	}
	payload = bytes.TrimSpace(payload)
	nc, err := nats.Connect(getenv("BGPULSE_NATS_URL", nats.DefaultURL))
	if err != nil {
		b.Fatal(err)
	}
	defer nc.Close()
	js, err := natsjs.New(nc)
	if err != nil {
		b.Fatal(err)
	}
	ns := "bench_" + rand.Text()
	cfg := jetstream.RawStream(ns)
	stream, err := js.CreateStream(b.Context(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if deleteErr := js.DeleteStream(ctx, cfg.Name); deleteErr != nil {
			b.Error(deleteErr)
		}
	}()
	subject := jetstream.RawSubject(ns, "rrc21")
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		if _, err = js.Publish(b.Context(), subject, payload); err != nil {
			b.Fatal(err)
		}
	}
	info, err := stream.Info(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	if info.State.Msgs == 0 {
		b.Fatal("no retained messages to measure")
	}
	b.ReportMetric(float64(info.State.Bytes)/float64(info.State.Msgs), "retained-B/msg")
}
