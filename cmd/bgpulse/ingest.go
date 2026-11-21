package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/outofstack/bgpulse/internal/event"
	"github.com/outofstack/bgpulse/internal/ingest"
	"github.com/outofstack/bgpulse/internal/jetstream"
	"github.com/outofstack/bgpulse/internal/ris"
)

func ingestLive(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	url := fs.String("nats", envOr("BGPULSE_NATS_URL", nats.DefaultURL), "NATS server URL (env BGPULSE_NATS_URL)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("ingest takes no positional arguments")
	}

	// The pilot has one live subscription per host. The OS releases this guard even on a crash.
	guard, err := new(net.ListenConfig).Listen(ctx, "tcp4", "127.0.0.1:7421")
	if err != nil {
		return fmt.Errorf("acquire local rrc21 ingest guard: %w", err)
	}
	defer func() { _ = guard.Close() }()
	nc, err := nats.Connect(*url, nats.Name("bgpulse-ingest"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	defer nc.Close()
	if nc.MaxPayload() < event.RawRecordLimit {
		return fmt.Errorf("ingest needs NATS max_payload >= 2 MiB, got %d", nc.MaxPayload())
	}
	js, err := natsjs.New(nc)
	if err != nil {
		return err
	}
	check, cancel := context.WithTimeout(ctx, 10*time.Second)
	b, err := jetstream.OpenCapture(check, js, "live", ris.Collector)
	cancel()
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	return ingest.Run(ctx, b, "live", ingest.URL, log)
}
