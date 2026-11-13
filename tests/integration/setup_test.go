//go:build integration

// Package integration runs against the pinned NATS and Scylla containers from
// deploy/compose.yaml (`make up`). Each test uses its own random namespace.
package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/outofstack/bgpulse/internal/jetstream"
	"github.com/outofstack/bgpulse/internal/store"
	"github.com/outofstack/bgpulse/migrations"
)

type env struct {
	ns   string
	js   natsjs.JetStream
	sess *gocql.Session
	migs []store.Migration
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newEnv(t *testing.T) *env {
	t.Helper()
	nc, err := nats.Connect(getenv("BGPULSE_NATS_URL", nats.DefaultURL))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := natsjs.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	cluster := gocql.NewCluster(strings.Split(getenv("BGPULSE_SCYLLA_HOSTS", "127.0.0.1:9042"), ",")...)
	cluster.Timeout = 30 * time.Second
	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sess.Close)
	migs, err := store.Load(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}

	b := make([]byte, 4)
	_, _ = rand.Read(b)
	e := &env{ns: "it_" + hex.EncodeToString(b), js: js, sess: sess, migs: migs}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = js.DeleteStream(ctx, jetstream.RawStream(e.ns).Name)
		_ = sess.Query("DROP KEYSPACE IF EXISTS " + store.Keyspace(e.ns)).WithContext(ctx).Exec()
	})
	return e
}

func (e *env) setup(t *testing.T) []store.Migration {
	t.Helper()
	if err := jetstream.Ensure(t.Context(), e.js, e.ns); err != nil {
		t.Fatal(err)
	}
	applied, err := store.Migrate(t.Context(), e.sess, e.ns, e.migs)
	if err != nil {
		t.Fatal(err)
	}
	return applied
}

func TestSetupIsIdempotent(t *testing.T) {
	e := newEnv(t)
	if got := e.setup(t); len(got) != len(e.migs) {
		t.Fatalf("first setup applied %d migrations, want %d", len(got), len(e.migs))
	}
	if got := e.setup(t); len(got) != 0 {
		t.Fatalf("second setup applied %d migrations, want 0", len(got))
	}
	if err := jetstream.Check(t.Context(), e.js, e.ns); err != nil {
		t.Fatal(err)
	}
	if err := store.Check(t.Context(), e.sess, e.ns, e.migs); err != nil {
		t.Fatal(err)
	}
}

func TestCheckFailsOnEmptyNamespace(t *testing.T) {
	e := newEnv(t)
	if err := jetstream.Check(t.Context(), e.js, e.ns); err == nil {
		t.Error("jetstream check passed on a namespace without a stream")
	}
	if err := store.Check(t.Context(), e.sess, e.ns, e.migs); err == nil {
		t.Error("store check passed on a namespace without a keyspace")
	}
}

func TestStreamDriftFails(t *testing.T) {
	e := newEnv(t)
	cfg := jetstream.RawStream(e.ns)
	cfg.MaxAge = time.Hour
	if _, err := e.js.CreateStream(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	err := jetstream.Ensure(t.Context(), e.js, e.ns)
	if !errors.Is(err, natsjs.ErrStreamNameAlreadyInUse) {
		t.Fatalf("Ensure on a drifted stream: err = %v, want ErrStreamNameAlreadyInUse", err)
	}
}

func TestConsumerDriftFails(t *testing.T) {
	e := newEnv(t)
	cfg := jetstream.RawStream(e.ns)
	if _, err := e.js.CreateStream(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	cons := jetstream.History()
	cons.MaxAckPending = 1
	if _, err := e.js.CreateConsumer(t.Context(), cfg.Name, cons); err != nil {
		t.Fatal(err)
	}
	if err := jetstream.Ensure(t.Context(), e.js, e.ns); err == nil {
		t.Fatal("Ensure on a drifted consumer succeeded")
	}
}

func TestMigrationChecksumDriftFails(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	q := "UPDATE " + store.Keyspace(e.ns) + ".schema_migrations SET checksum = 'changed' WHERE version = 1"
	if err := e.sess.Query(q).WithContext(t.Context()).Exec(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Migrate(t.Context(), e.sess, e.ns, e.migs); err == nil {
		t.Fatal("Migrate succeeded after a recorded checksum changed")
	}
	if err := store.Check(t.Context(), e.sess, e.ns, e.migs); err == nil {
		t.Fatal("Check succeeded after a recorded checksum changed")
	}
}

// TestRoundTrip publishes one synthetic message, consumes it through the
// history consumer, writes it to raw_messages_v1 and reads it back.
func TestRoundTrip(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	ctx := t.Context()

	payload := []byte(`{"synthetic":true}`)
	const msgID = "sha256:roundtrip"
	subject := jetstream.RawSubject(e.ns, "rrc00")
	if _, err := e.js.Publish(ctx, subject, payload, natsjs.WithMsgID(msgID)); err != nil {
		t.Fatal(err)
	}
	ack, err := e.js.Publish(ctx, subject, payload, natsjs.WithMsgID(msgID))
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Duplicate {
		t.Error("republish with the same Nats-Msg-Id was not reported as a duplicate")
	}

	cons, err := e.js.Consumer(ctx, jetstream.RawStream(e.ns).Name, jetstream.HistoryConsumer)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cons.Fetch(1, natsjs.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var msg natsjs.Msg
	for m := range batch.Messages() {
		msg = m
	}
	if msg == nil {
		t.Fatalf("no message fetched: %v", batch.Error())
	}

	now := time.Now().UTC()
	bucket := now.Truncate(5 * time.Minute)
	insert := "INSERT INTO " + store.Keyspace(e.ns) + `.raw_messages_v1
		(source, collector, bucket_start, shard, event_time_us, message_id, payload, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`
	if err := e.sess.Query(insert, "ris", "rrc00", bucket, int16(0), now.UnixMicro(), msgID,
		msg.Data(), now.Add(time.Hour), 3600).WithContext(ctx).Exec(); err != nil {
		t.Fatal(err)
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}

	var got []byte
	sel := "SELECT payload FROM " + store.Keyspace(e.ns) + `.raw_messages_v1
		WHERE source = ? AND collector = ? AND bucket_start = ? AND shard = ?`
	if err := e.sess.Query(sel, "ris", "rrc00", bucket, int16(0)).WithContext(ctx).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

// TestSyncAlways checks the live profile from deploy/nats/nats.conf is in effect.
func TestSyncAlways(t *testing.T) {
	url := getenv("BGPULSE_NATS_MONITOR", "http://127.0.0.1:8222") + "/jsz"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var jsz struct {
		Config struct {
			SyncAlways bool `json:"sync_always"`
		} `json:"config"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&jsz); err != nil {
		t.Fatal(err)
	}
	if !jsz.Config.SyncAlways {
		t.Fatal("JetStream is not running with sync_interval: always")
	}
}
