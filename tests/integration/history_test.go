//go:build integration

package integration

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/outofstack/bgpulse/internal/event"
	"github.com/outofstack/bgpulse/internal/store"
)

// fixture normalizes a RIS frame and moves its event time to now, keeping its IDs.
func fixture(t *testing.T, raw []byte) *event.Message {
	t.Helper()
	m, err := event.Normalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	m.EventTimeUS = time.Now().UnixMicro()
	return m
}

// captured returns the checked-in rrc21 UPDATE and its fixture message.
func captured(t *testing.T) ([]byte, *event.Message) {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/ris/rrc21-update.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw, fixture(t, raw)
}

// TestHistorySchema checks the declared table options on the pinned image.
func TestHistorySchema(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	for _, table := range []string{"raw_messages_v1", "prefix_events_v1"} {
		var ks, typ, name, ddl string
		if err := e.sess.Query("DESCRIBE TABLE "+store.Keyspace(e.ns)+"."+table).WithContext(t.Context()).
			Scan(&ks, &typ, &name, &ddl); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"default_time_to_live = 1800", "TimeWindowCompactionStrategy",
			"'compaction_window_size': '1'", "'compaction_window_unit': 'HOURS'", "'mode': 'immediate'"} {
			if !strings.Contains(ddl, want) {
				t.Errorf("%s: no %s in\n%s", table, want, ddl)
			}
		}
	}
}

// TestHistoryWrite writes a captured UPDATE and a withdrawal on either side of a UTC hour
// boundary twice, then reads every row back by its full partition key.
func TestHistoryWrite(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	ctx, ks := t.Context(), store.Keyspace(e.ns)
	raw, update := captured(t)
	var err error
	wRaw := []byte(`{"type":"ris_message","data":{"timestamp":1,"peer":"2001:db8::1",
		"peer_asn":"4200000000","id":"w-1","host":"rrc21.ripe.net","type":"UPDATE","withdrawals":["2001:db8:10::/48"]}}`)
	hour := time.Date(2026, 11, 21, 11, 0, 0, 0, time.UTC)
	withdrawal := fixture(t, wRaw)
	update.EventTimeUS, withdrawal.EventTimeUS = hour.UnixMicro()-1, hour.UnixMicro()
	h := store.NewHistory(e.sess, e.ns)
	h.Now = func() time.Time { return hour }
	for range 2 { // a retry rewrites the same keys
		for m, b := range map[*event.Message][]byte{update: raw, withdrawal: wRaw} {
			if err = h.Write(ctx, m, b); err != nil {
				t.Fatal(err)
			}
		}
	}

	nonzero := false
	for m, want := range map[*event.Message][2]time.Time{
		update:     {hour.Add(-5 * time.Minute), hour.Add(-time.Hour)},
		withdrawal: {hour, hour},
	} {
		sum := sha256.Sum256([]byte(m.MessageID)) // independent copy of the raw shard contract (4 shards)
		shard := int16(binary.BigEndian.Uint64(sum[:8]) % 4)
		nonzero = nonzero || shard != 0
		var id string
		var ttl int
		if err = e.sess.Query("SELECT message_id, TTL(payload) FROM "+ks+".raw_messages_v1 WHERE source = 'ris' AND collector = 'rrc21' AND bucket_start = ? AND shard = ?",
			want[0], shard).WithContext(ctx).Scan(&id, &ttl); err != nil {
			t.Fatalf("raw row in bucket %s shard %d: %v", want[0].Format(time.TimeOnly), shard, err)
		}
		if id != m.MessageID || ttl <= 1790 || ttl > 1800 {
			t.Errorf("raw row %s, TTL %d", id, ttl)
		}

		sel := "SELECT action, next_hop, origin_asn, path_json, TTL(action) FROM " + ks + ".prefix_events_v1 WHERE prefix = ? AND bucket_start = ? AND shard = 0"
		for _, c := range m.Children {
			var action string
			var nextHop, path *string
			var origin *int64
			n := 0
			it := e.sess.Query(sel, c.Prefix.String(), want[1]).WithContext(ctx).Iter()
			for it.Scan(&action, &nextHop, &origin, &path, &ttl) {
				n++
			}
			if err = it.Close(); err != nil {
				t.Fatal(err)
			}
			if n != 1 || action != c.Action || ttl <= 1790 || ttl > 1800 {
				t.Errorf("%s: %d rows, action %q, TTL %d", c.Prefix, n, action, ttl)
			}
			if c.Action == event.Withdraw && (nextHop != nil || origin != nil || path != nil) {
				t.Errorf("%s: withdrawal has next hop %v, origin %v, path %v", c.Prefix, nextHop, origin, path)
			}
			if c.Action == event.Announce && (nextHop == nil || origin == nil || *origin != 50870) {
				t.Errorf("%s: announcement has next hop %v, origin %v", c.Prefix, nextHop, origin)
			}
		}
	}
	if !nonzero {
		t.Error("fixtures only exercise raw shard 0")
	}
}

// TestHistoryExpiry shows rows leave query results at their event-time deadline,
// whether or not compaction has removed them from disk yet.
func TestHistoryExpiry(t *testing.T) {
	e := newEnv(t)
	e.setup(t)
	ctx, ks := t.Context(), store.Keyspace(e.ns)
	raw, m := captured(t)
	var err error
	h := store.NewHistory(e.sess, e.ns)
	h.Retention = 10 * time.Second // headroom for a slow runner
	if err = h.Write(ctx, m, raw); err != nil {
		t.Fatal(err)
	}
	c := m.Children[0]
	count := func() (n int) {
		q := "SELECT COUNT(*) FROM " + ks + ".prefix_events_v1 WHERE prefix = ? AND bucket_start = ? AND shard = 0"
		if qErr := e.sess.Query(q, c.Prefix.String(), time.UnixMicro(m.EventTimeUS).Truncate(time.Hour)).WithContext(ctx).Scan(&n); qErr != nil {
			t.Fatal(qErr)
		}
		return n
	}
	if n := count(); n != 1 {
		t.Fatalf("rows before deadline = %d, want 1", n)
	}
	time.Sleep(time.Until(time.UnixMicro(m.EventTimeUS).Add(h.Retention + 1500*time.Millisecond)))
	if n := count(); n != 0 {
		t.Fatalf("rows after deadline = %d, want 0", n)
	}
	if err = h.Write(ctx, m, raw); !errors.Is(err, store.ErrExpired) {
		t.Fatalf("rewrite after the deadline: err = %v, want ErrExpired", err)
	}
}
