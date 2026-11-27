package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/outofstack/bgpulse/internal/jetstream"
	"github.com/outofstack/bgpulse/internal/persist"
	"github.com/outofstack/bgpulse/internal/store"
	"github.com/outofstack/bgpulse/migrations"
)

func persistLive(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("persist", flag.ContinueOnError)
	natsURL := fs.String("nats", envOr("BGPULSE_NATS_URL", nats.DefaultURL), "NATS server URL (env BGPULSE_NATS_URL)")
	hosts := fs.String("scylla", envOr("BGPULSE_SCYLLA_HOSTS", "127.0.0.1:9042"), "comma-separated Scylla contact points (env BGPULSE_SCYLLA_HOSTS)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("persist takes no positional arguments")
	}
	const ns = "live"
	migs, err := store.Load(migrations.FS)
	if err != nil {
		return err
	}
	js, sess, closeAll, err := connect(*natsURL, *hosts, "bgpulse-persist")
	if err != nil {
		return err
	}
	defer closeAll()

	check, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err = store.Check(check, sess, ns, migs); err != nil {
		return err
	}
	in, err := jetstream.OpenInput(check, js, ns)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	log.Info("persist starting", "namespace", ns, "consumer", jetstream.HistoryConsumer,
		"keyspace", store.Keyspace(ns), "history_retention", store.HistoryRetention.String())
	c := persist.Consumer{In: in, History: store.NewHistory(sess, ns), Sources: store.NewSources(sess, ns), Log: log}
	return c.Run(ctx)
}
