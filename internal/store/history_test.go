package store

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/outofstack/bgpulse/internal/event"
)

func TestTTLSeconds(t *testing.T) {
	deadline := time.Unix(1_000_000, 0)
	for _, tt := range []struct {
		left time.Duration
		want int
	}{
		{30 * time.Minute, 1800},
		{1800*time.Second - time.Nanosecond, 1799}, // rounds down: <1s early, never late
		{time.Second, 1},
		{time.Second - time.Nanosecond, 0},
		{0, 0},
		{-time.Second, 0},
	} {
		if got := ttlSeconds(deadline, deadline.Add(-tt.left)); got != tt.want {
			t.Errorf("ttl with %v left = %d, want %d", tt.left, got, tt.want)
		}
	}
}

// The shard is part of the raw partition key: changing it is a schema change.
func TestRawShard(t *testing.T) {
	for id, want := range map[string]int16{
		"": 0, // sha256 e3b0c44298fc1c14
		"0a3c5f8b0e1e2b3bb0f3f8c5fd2a6e1f6c2c2f9a8a8d6b6b1f3e0c9b7d2e1a00": 1, // sha256 856b674146909c29
	} {
		if got := rawShard(id); got != want {
			t.Errorf("rawShard(%q) = %d, want %d", id, got, want)
		}
	}
}

func TestPrefixArgs(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/ris/rrc21-update.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := event.Normalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	at := time.UnixMicro(m.EventTimeUS).UTC()
	args, err := prefixArgs(m, m.Children[0], at, at.Add(HistoryRetention))
	if err != nil {
		t.Fatal(err)
	}
	if args[0] != "94.243.212.0/23" || args[1] != time.Unix(1795255200, 0).UTC() { // 10:00Z, the hour of 10:28:15.73Z
		t.Errorf("partition = %v, %v", args[0], args[1])
	}
	if args[13] != "37.49.236.177" || args[14] != "[12779,31133,44843,50870]" || args[15] != int64(50870) ||
		args[16] != event.OriginUnique || args[17] != gocql.UnsetValue {
		t.Errorf("announcement columns = %v", args[13:18])
	}

	w := event.Child{EventID: "w", Action: event.Withdraw, Prefix: m.Children[0].Prefix}
	if args, err = prefixArgs(m, w, at, at); err != nil {
		t.Fatal(err)
	}
	for i := 13; i <= 17; i++ {
		if args[i] != gocql.UnsetValue {
			t.Errorf("withdrawal column %d = %v, want unset", i, args[i])
		}
	}

	set := m.Children[0]
	set.Path = event.Path{{ASNs: []uint32{64500}}, {ASNs: []uint32{4200000000, 64501}, Set: true}}
	set.OriginResolution, set.OriginASN = event.OriginASSet, 0
	set.NextHops = append(set.NextHops, set.NextHops[0])
	if args, err = prefixArgs(m, set, at, at); err != nil {
		t.Fatal(err)
	}
	if args[14] != "[64500,[4200000000,64501]]" || args[15] != gocql.UnsetValue ||
		args[17] != `{"next_hops":["37.49.236.177","37.49.236.177"]}` {
		t.Errorf("AS_SET columns = %v", args[13:18])
	}
}

func TestWriteRejectsOutOfWindow(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := &History{Retention: HistoryRetention, Now: func() time.Time { return now }} // no session: nothing may be sent
	for name, tt := range map[string]struct {
		event time.Time
		want  error
	}{
		"expired": {now.Add(-HistoryRetention), ErrExpired},
		"future":  {now.Add(maxClockSkew + time.Microsecond), ErrFutureEvent},
	} {
		m := &event.Message{EventTimeUS: tt.event.UnixMicro()}
		if err := h.Write(t.Context(), m, nil); !errors.Is(err, tt.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tt.want)
		}
	}
}
