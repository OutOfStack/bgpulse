//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/outofstack/bgpulse/internal/event"
	"github.com/outofstack/bgpulse/internal/jetstream"
	"github.com/outofstack/bgpulse/internal/persist"
	"github.com/outofstack/bgpulse/internal/store"
)

// rawShard is an independent copy of the raw shard contract (4 shards).
func rawShard(messageID string) int16 {
	sum := sha256.Sum256([]byte(messageID))
	return int16(binary.BigEndian.Uint64(sum[:8]) % 4)
}

// recorder builds envelopes for one test connection epoch.
type recorder struct {
	ns      string
	ordinal uint64
	now     time.Time
}

func (r *recorder) base() event.Envelope {
	return event.Envelope{Version: 1, Namespace: r.ns, Source: "ris", Collector: "rrc21", Epoch: "epoch-1", ReceivedAt: r.now}
}

func (r *recorder) control(typ string, gap event.Gap) *event.Envelope {
	e := r.base()
	e.Control(event.Lifecycle{Type: typ, Ordinal: r.ordinal, Reason: typ, Gap: &gap})
	r.ordinal++
	return &e
}

func (r *recorder) data(id string, raw []byte) *event.Envelope {
	e := r.base()
	e.Data(jsontext.Value(`"`+id+`"`), jsontext.Value(strconv.FormatInt(r.now.Unix(), 10)), raw)
	return &e
}

func update(id string, at time.Time, withdrawal string) []byte {
	return fmt.Appendf(nil, `{"type":"ris_message","data":{"timestamp":%d,"peer":"192.0.2.1","peer_asn":"64500",
		"id":%q,"host":"rrc21.ripe.net","type":"UPDATE","path":[64500,4200000000],"withdrawals":[%q],
		"announcements":[{"next_hop":"192.0.2.1","prefixes":["198.51.100.0/24","203.0.113.0/24"]}]}}`,
		at.Unix(), id, withdrawal)
}

func (e *env) publish(t *testing.T, envs ...*event.Envelope) [][]byte {
	t.Helper()
	c, err := jetstream.OpenCapture(t.Context(), e.js, e.ns, "rrc21")
	if err != nil {
		t.Fatal(err)
	}
	out := make([][]byte, len(envs))
	for i, env := range envs {
		if out[i], err = json.Marshal(env); err != nil {
			t.Fatal(err)
		}
		if err = c.Publish(t.Context(), env.MessageID, out[i]); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func (e *env) input(t *testing.T) *jetstream.Input {
	t.Helper()
	in, err := jetstream.OpenInput(t.Context(), e.js, e.ns)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func (e *env) consumer(t *testing.T, in *jetstream.Input) *persist.Consumer {
	t.Helper()
	return &persist.Consumer{In: in, History: store.NewHistory(e.sess, e.ns), Sources: store.NewSources(e.sess, e.ns),
		Log: slog.New(slog.NewTextHandler(t.Output(), nil))}
}

// startPersist runs the consumer until the returned stop function is called, which returns Run's error.
func (e *env) startPersist(t *testing.T, in *jetstream.Input) func() error {
	t.Helper()
	c := e.consumer(t, in)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return func() error { cancel(); return <-done }
}

// waitFor polls the history consumer until cond holds.
func waitFor(t *testing.T, in *jetstream.Input, what string, cond func(pending, ackPending, redelivered int) bool) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		info, err := in.Info(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if cond(int(info.NumPending), info.NumAckPending, info.NumRedelivered) {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func settled(pending, ackPending, _ int) bool { return pending == 0 && ackPending == 0 }

// TestPersist drives one epoch through the consumer: a message partly written before a simulated crash converges on
// its complete row set, invalid source data lands in quarantine, and the lifecycle controls leave a closed gap.
func TestPersist(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	ctx, ks := t.Context(), store.Keyspace(e.ns)
	r := &recorder{ns: e.ns, now: time.Now().UTC()}
	good, bad := update("p-1", r.now, "192.0.2.0/24"), update("p-2", r.now, "not-a-prefix")
	m, err := event.Normalize(good)
	if err != nil {
		t.Fatal(err)
	}
	partial := *m // state after a crash between the first and second child
	partial.Children = m.Children[:1]
	if err = store.NewHistory(e.sess, e.ns).Write(ctx, &partial, good); err != nil {
		t.Fatal(err)
	}
	gap := event.Gap{ID: "g-1", Start: r.now.Add(-time.Minute), Reason: "startup_or_reconnect"}
	closed := gap
	closed.End = r.now
	sent := e.publish(t, r.control("gap_open", gap), r.control("subscribed", gap), r.control("gap_close", closed),
		r.data("p-1", good), r.data("p-2", bad))

	in := e.input(t)
	stop := e.startPersist(t, in)
	waitFor(t, in, "all records settled", settled)
	if err = stop(); err != nil {
		t.Fatal(err)
	}

	var n int
	if err = e.sess.Query("SELECT COUNT(*) FROM "+ks+".raw_messages_v1 WHERE source = 'ris' AND collector = 'rrc21' AND bucket_start = ? AND shard = ? AND event_time_us = ? AND message_id = ?",
		r.now.Truncate(5*time.Minute), rawShard(m.MessageID), m.EventTimeUS, m.MessageID).WithContext(ctx).Scan(&n); err != nil || n != 1 {
		t.Fatalf("raw rows = %d, err %v", n, err)
	}
	for _, c := range m.Children {
		if err = e.sess.Query("SELECT COUNT(*) FROM "+ks+".prefix_events_v1 WHERE prefix = ? AND bucket_start = ? AND shard = 0",
			c.Prefix.String(), r.now.Truncate(time.Hour)).WithContext(ctx).Scan(&n); err != nil || n != 1 {
			t.Errorf("%s: %d rows, err %v", c.Prefix, n, err)
		}
	}

	s, err := e.js.Stream(ctx, jetstream.QuarantineStream(e.ns).Name)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.CachedInfo().State.Msgs; got != 1 {
		t.Fatalf("quarantine holds %d records, want 1", got)
	}
	q, err := s.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if q.Subject != jetstream.QuarantineSubject(e.ns, "rrc21") || q.Header.Get("Bgpulse-Category") != "invalid_source" ||
		q.Header.Get("Bgpulse-Raw-Seq") != "5" || string(q.Data) != string(sent[4]) {
		t.Errorf("quarantine record %s %v", q.Subject, q.Header)
	}

	var status string
	var active *string
	if err = e.sess.Query("SELECT status, active_gap_id FROM "+ks+".source_status_v1 WHERE source = 'ris' AND collector = 'rrc21'").
		WithContext(ctx).Scan(&status, &active); err != nil || status != "gap_close" || active != nil {
		t.Errorf("status %q, active gap %v, err %v", status, active, err)
	}
	var ended *time.Time
	if err = e.sess.Query("SELECT ended_at FROM "+ks+".source_gaps_v1 WHERE source = 'ris' AND collector = 'rrc21' AND day = ?",
		r.now.Truncate(24*time.Hour)).WithContext(ctx).Scan(&ended); err != nil || ended == nil || !ended.Equal(r.now.Truncate(time.Millisecond)) {
		t.Errorf("gap ended at %v, err %v", ended, err)
	}
}

// TestSourcesOrder applies controls newest first and from two stream incarnations. Status follows the latest
// accepted record, a late gap_open does not reopen its gap, and an unknown start reaches every retained day.
func TestSourcesOrder(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	ctx, ks := t.Context(), store.Keyspace(e.ns)
	src := store.NewSources(e.sess, e.ns)
	r := &recorder{ns: e.ns, now: time.Now().UTC()}
	gap := event.Gap{ID: "g-unknown", Reason: "startup_or_reconnect"}
	closed := gap
	closed.End = r.now
	open, subscribed, closing := r.control("gap_open", gap), r.control("subscribed", gap), r.control("gap_close", closed)
	old := r.now.Add(-time.Hour)
	ctl := []*event.Envelope{nil, open, subscribed, closing} // indexed by stream sequence
	for _, seq := range []uint64{3, 2, 1} {
		if err := src.Apply(ctx, ctl[seq], old, seq); err != nil {
			t.Fatal(err)
		}
	}
	status := func() (s string, active *string) {
		if err := e.sess.Query("SELECT status, active_gap_id FROM "+ks+".source_status_v1 WHERE source = 'ris' AND collector = 'rrc21'").
			WithContext(ctx).Scan(&s, &active); err != nil {
			t.Fatal(err)
		}
		return s, active
	}
	if s, active := status(); s != "gap_close" || active != nil {
		t.Fatalf("status %q, active gap %v after reordered controls", s, active)
	}
	today := r.now.Truncate(24 * time.Hour)
	for days, want := range map[int]int{0: 1, 30: 1, 31: 0} {
		var ended []*time.Time
		it := e.sess.Query("SELECT ended_at FROM "+ks+".source_gaps_v1 WHERE source = 'ris' AND collector = 'rrc21' AND day = ? AND started_at = 0",
			today.AddDate(0, 0, -days)).WithContext(ctx).Iter()
		for end := new(*time.Time); it.Scan(end); end = new(*time.Time) {
			ended = append(ended, *end)
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		if len(ended) != want || (want == 1 && ended[0] == nil) {
			t.Errorf("%d days ago: unknown-start copies %v, want %d closed", days, ended, want)
		}
	}

	// A recreated stream restarts sequences, yet its first control outranks the old stream's later ones.
	next := r.control("gap_open", event.Gap{ID: "g-next", Start: r.now, Reason: "source_error"})
	if err := src.Apply(ctx, next, r.now, 1); err != nil {
		t.Fatal(err)
	}
	if err := src.Apply(ctx, closing, old, 4); err != nil {
		t.Fatal(err)
	}
	if s, active := status(); s != "gap_open" || active == nil || *active != "g-next" {
		t.Fatalf("status %q, active gap %v, want the new stream's open gap", s, active)
	}
}

// TestPersistQuarantineUnavailable keeps an invalid record pending while its quarantine publish fails.
func TestPersistQuarantineUnavailable(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	r := &recorder{ns: e.ns, now: time.Now().UTC()}
	e.publish(t, r.data("p-3", update("p-3", r.now, "not-a-prefix")))
	in := e.input(t)
	if err := e.js.DeleteStream(t.Context(), jetstream.QuarantineStream(e.ns).Name); err != nil {
		t.Fatal(err)
	}
	stop := e.startPersist(t, in)
	waitFor(t, in, "a redelivery without ACK", func(_, ackPending, redelivered int) bool { return ackPending == 1 && redelivered == 1 })
	if err := jetstream.Ensure(t.Context(), e.js, e.ns); err != nil {
		t.Fatal(err)
	}
	waitFor(t, in, "quarantine after recovery", settled)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	s, err := e.js.Stream(t.Context(), jetstream.QuarantineStream(e.ns).Name)
	if err != nil || s.CachedInfo().State.Msgs != 1 {
		t.Fatalf("quarantine stream %v, err %v", s, err)
	}
}

// TestPersistStopsOnFutureSchema leaves a record from a newer schema pending and stops the consumer.
func TestPersistStopsOnFutureSchema(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	c, err := jetstream.OpenCapture(t.Context(), e.js, e.ns, "rrc21")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Publish(t.Context(), "v2", []byte(`{"schema_version":2}`)); err != nil {
		t.Fatal(err)
	}
	in := e.input(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err = e.consumer(t, in).Run(ctx); err == nil || !strings.Contains(err.Error(), "schema_version 2") {
		t.Fatalf("Run returned %v, want the schema_version error", err)
	}
	info, err := in.Info(t.Context())
	if err != nil || info.NumAckPending != 1 {
		t.Fatalf("ack pending %v, err %v", info, err)
	}
}

// TestPersistDeadlines ACKs a record past its history deadline without a write, and quarantines one too far ahead of
// the writer clock.
func TestPersistDeadlines(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	ctx, ks := t.Context(), store.Keyspace(e.ns)
	r := &recorder{ns: e.ns, now: time.Now().UTC()}
	in := e.input(t)
	for i, clock := range []time.Duration{time.Hour, -time.Hour} { // expired, then future
		id := fmt.Sprintf("d-%d", i)
		e.publish(t, r.data(id, update(id, r.now, "192.0.2.0/24")))
		c := e.consumer(t, in)
		c.History.Now = func() time.Time { return time.Now().Add(clock) }
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- c.Run(runCtx) }()
		waitFor(t, in, "record "+id+" settled", settled)
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := e.sess.Query("SELECT COUNT(*) FROM "+ks+".prefix_events_v1 WHERE prefix = '192.0.2.0/24' AND bucket_start = ? AND shard = 0",
		r.now.Truncate(time.Hour)).WithContext(ctx).Scan(&n); err != nil || n != 0 {
		t.Fatalf("prefix rows = %d, err %v", n, err)
	}
	s, err := e.js.Stream(ctx, jetstream.QuarantineStream(e.ns).Name)
	if err != nil {
		t.Fatal(err)
	}
	q, err := s.GetMsg(ctx, 1)
	if err != nil || s.CachedInfo().State.Msgs != 1 || q.Header.Get("Bgpulse-Category") != "future_event" || q.Header.Get("Bgpulse-Raw-Seq") != "2" {
		t.Fatalf("quarantine %v, err %v", s.CachedInfo().State, err)
	}
}
