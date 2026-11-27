package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/outofstack/bgpulse/internal/event"
)

// HistoryRetention is the pilot's raw and prefix history budget from source event time
// (docs/benchmarks/dev-capacity.md).
const HistoryRetention = 30 * time.Minute

// maxClockSkew bounds how far source event time may lead the writer's clock.
const maxClockSkew = 5 * time.Minute

// Partition layout of the v1 history tables. Changing any of these needs new tables.
const (
	rawBucket    = 5 * time.Minute
	rawShards    = 4
	prefixBucket = time.Hour
)

var (
	// ErrExpired means less than a second was left before the fixed history deadline when a row was due.
	// Rows already written target the same deadline.
	ErrExpired = errors.New("history deadline passed")
	// ErrFutureEvent means source event time leads the writer clock by more than maxClockSkew.
	ErrFutureEvent = errors.New("event time too far in the future")
)

// History writes raw and prefix history rows for one namespace. Rows are keyed on
// message_id and event_id, so rewriting a message after a retry yields the same rows
// with the same target deadline.
type History struct {
	sess      *gocql.Session
	raw       string
	prefix    string
	Retention time.Duration    // fixed lifetime from event time
	Now       func() time.Time // writer clock
}

// NewHistory returns a writer for namespace ns with the pilot retention.
func NewHistory(s *gocql.Session, ns string) *History {
	ks := Keyspace(ns)
	return &History{
		sess: s,
		raw: "INSERT INTO " + ks + `.raw_messages_v1 (source, collector, bucket_start, shard, event_time_us,
			message_id, source_message_id, observer_id, message_type, payload_hash, payload, expires_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`,
		prefix: "INSERT INTO " + ks + `.prefix_events_v1 (prefix, bucket_start, shard, event_time_us, event_id,
			message_id, child_ordinal, source, collector, observer_id, peer_ip, peer_asn, action, next_hop,
			path_json, origin_asn, origin_resolution, attributes_json, expires_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`,
		Retention: HistoryRetention,
		Now:       time.Now,
	}
}

// Write stores m's raw row with the exact source bytes, then one row per prefix child.
// Each insert carries the remaining TTL to event time + Retention. A failed Write may
// leave some rows written; retrying it converges on the complete set.
//
// ponytail: children are written one at a time, so persist's 8 workers keep at most 8 writes in flight, well
// inside the 64-write budget. Write children concurrently under a shared limit if consumer lag needs it.
func (h *History) Write(ctx context.Context, m *event.Message, payload []byte) error {
	at := time.UnixMicro(m.EventTimeUS).UTC()
	if at.Sub(h.Now()) > maxClockSkew {
		return fmt.Errorf("%w: %s", ErrFutureEvent, at.Format(time.RFC3339Nano))
	}
	deadline := at.Add(h.Retention)
	sum := sha256.Sum256(payload)
	if err := h.exec(ctx, deadline, h.raw, m.Source, m.Collector, at.Truncate(rawBucket), rawShard(m.MessageID),
		m.EventTimeUS, m.MessageID, m.SourceMessageID, m.ObserverID, m.Type, hex.EncodeToString(sum[:]),
		payload, deadline); err != nil {
		return fmt.Errorf("raw %s: %w", m.MessageID, err)
	}
	for _, c := range m.Children {
		args, err := prefixArgs(m, c, at, deadline)
		if err != nil {
			return err
		}
		if err = h.exec(ctx, deadline, h.prefix, args...); err != nil {
			return fmt.Errorf("prefix event %s: %w", c.EventID, err)
		}
	}
	return nil
}

func (h *History) exec(ctx context.Context, deadline time.Time, q string, args ...any) error {
	ttl := ttlSeconds(deadline, h.Now())
	if ttl <= 0 {
		return ErrExpired // TTL 0 would mean a row that never expires
	}
	return h.sess.Query(q, append(args, ttl)...).WithContext(ctx).Exec()
}

// ttlSeconds rounds the time left until deadline down to whole seconds. Scylla starts the
// TTL when it applies the write, so a row expires up to one second early plus however long
// the request took to arrive. It is 0 once less than a second is left.
func ttlSeconds(deadline, now time.Time) int {
	return int(max(deadline.Sub(now), 0) / time.Second)
}

// rawShard is the first 64 bits of SHA-256(message_id) mod rawShards.
func rawShard(messageID string) int16 {
	sum := sha256.Sum256([]byte(messageID))
	return int16(binary.BigEndian.Uint64(sum[:8]) % rawShards)
}

func prefixArgs(m *event.Message, c event.Child, at, deadline time.Time) ([]any, error) {
	unset := any(gocql.UnsetValue)
	nextHop, path, originASN, resolution, attrs := unset, unset, unset, unset, unset
	if c.Action == event.Announce {
		nextHop, resolution = c.NextHops[0].String(), c.OriginResolution
		b, err := json.Marshal(pathJSON(c.Path))
		if err != nil {
			return nil, err
		}
		path = string(b)
		if c.OriginResolution == event.OriginUnique {
			originASN = int64(c.OriginASN)
		}
		if len(c.NextHops) > 1 {
			if b, err = json.Marshal(map[string]any{"next_hops": c.NextHops}); err != nil {
				return nil, err
			}
			attrs = string(b)
		}
	}
	return []any{c.Prefix.String(), at.Truncate(prefixBucket), int16(0), m.EventTimeUS, c.EventID,
		m.MessageID, c.Ordinal, m.Source, m.Collector, m.ObserverID, m.PeerIP.String(), int64(m.PeerASN),
		c.Action, nextHop, path, originASN, resolution, attrs, deadline}, nil
}

// pathJSON mirrors the RIS form: an ASN per sequence hop and an array per AS_SET.
func pathJSON(p event.Path) []any {
	out := make([]any, len(p))
	for i, s := range p {
		if s.Set {
			out[i] = s.ASNs
		} else {
			out[i] = s.ASNs[0]
		}
	}
	return out
}
