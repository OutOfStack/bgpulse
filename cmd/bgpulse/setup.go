package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/outofstack/bgpulse/internal/jetstream"
	"github.com/outofstack/bgpulse/internal/store"
	"github.com/outofstack/bgpulse/migrations"
)

// A namespace becomes part of NATS subjects, a stream name and a keyspace name.
var namespaceRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

func setup(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("setup", flag.ContinueOnError)
	natsURL := fs.String("nats", envOr("BGPULSE_NATS_URL", nats.DefaultURL), "NATS server URL (env BGPULSE_NATS_URL)")
	hosts := fs.String("scylla", envOr("BGPULSE_SCYLLA_HOSTS", "127.0.0.1:9042"), "comma-separated Scylla contact points (env BGPULSE_SCYLLA_HOSTS)")
	ns := fs.String("namespace", "live", "resource namespace")
	check := fs.Bool("check", false, "only verify that resources exist and match; change nothing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !namespaceRE.MatchString(*ns) {
		return fmt.Errorf("invalid namespace %q: want %s", *ns, namespaceRE)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	migs, err := store.Load(migrations.FS)
	if err != nil {
		return err
	}
	nc, err := nats.Connect(*natsURL, nats.Name("bgpulse-setup"))
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	defer nc.Close()
	js, err := natsjs.New(nc)
	if err != nil {
		return err
	}
	cluster := gocql.NewCluster(strings.Split(*hosts, ",")...)
	cluster.Timeout = 30 * time.Second // DDL is slow on a small dev node
	sess, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("scylla: %w", err)
	}
	defer sess.Close()

	stream, keyspace := jetstream.RawStream(*ns).Name, store.Keyspace(*ns)
	if *check {
		if err = jetstream.Check(ctx, js, *ns); err != nil {
			return err
		}
		if err = store.Check(ctx, sess, *ns, migs); err != nil {
			return err
		}
		logger.Info("resources ok", "namespace", *ns, "stream", stream, "keyspace", keyspace, "migrations", len(migs))
		return nil
	}
	if err = jetstream.Ensure(ctx, js, *ns); err != nil {
		return err
	}
	applied, err := store.Migrate(ctx, sess, *ns, migs)
	if err != nil {
		return err
	}
	names := make([]string, len(applied))
	for i, m := range applied {
		names[i] = fmt.Sprintf("%04d_%s", m.Version, m.Name)
	}
	logger.Info("setup complete", "namespace", *ns, "stream", stream, "keyspace", keyspace, "applied", names)
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
