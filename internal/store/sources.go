package store

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/outofstack/bgpulse/internal/event"
)

// GapRetention keeps each daily gap copy for 30 days after its UTC day ends.
const GapRetention = 30 * 24 * time.Hour

const day = 24 * time.Hour

// Sources projects lifecycle envelopes into source_status_v1 and source_gaps_v1.
type Sources struct {
	sess        *gocql.Session
	status, gap string
	Now         func() time.Time // writer clock for gap expiry
}

// NewSources returns the lifecycle projection for namespace ns.
func NewSources(s *gocql.Session, ns string) *Sources {
	ks := Keyspace(ns)
	return &Sources{
		sess: s,
		status: "INSERT INTO " + ks + `.source_status_v1 (source, collector, connection_epoch, stream_generation,
			last_control_seq, subscription_hash, status, reason, last_received_at, last_accepted_at,
			last_event_time_us, active_gap_id, active_gap_started_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		gap: "INSERT INTO " + ks + `.source_gaps_v1 (source, collector, day, started_at, gap_id, ended_at, reason,
			stream_generation, last_control_seq, subscription_hash, resolved)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ? AND TTL ?`,
		Now: time.Now,
	}
}

// Apply writes lifecycle envelope e, accepted at sequence seq of a raw stream created at created. Every write uses
// the CQL timestamp created + seq microseconds, so the latest accepted control wins in any delivery order and a
// recreated stream outranks the old one.
//
// ponytail: created + seq stays below the next stream's creation while the stream stores under one record per
// microsecond; give the tables an explicit (generation, seq) guard if that ever stops holding.
func (s *Sources) Apply(ctx context.Context, e *event.Envelope, created time.Time, seq uint64) error {
	l := e.Lifecycle
	ts := created.UnixMicro() + int64(seq) //nolint:gosec // a stream sequence is far below 2^63
	gen := created.UTC().Format(time.RFC3339Nano)
	var gapID, gapStart any
	if g := l.Gap; g != nil && g.End.IsZero() {
		gapID = g.ID
		if !g.Start.IsZero() {
			gapStart = g.Start
		}
	}
	if err := s.sess.Query(s.status, e.Source, e.Collector, e.Epoch, gen, int64(seq), l.SubscriptionHash, l.Type, //nolint:gosec // see ts
		l.Reason, micros(l.LastReceiveUS), micros(l.LastPubAckUS), orNil(l.LastSourceUS), gapID, gapStart,
		e.ReceivedAt, ts).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("status %s: %w", e.MessageID, err)
	}
	if l.Gap == nil {
		return nil
	}
	g, now := l.Gap, s.Now()
	end, ended := g.End, any(gocql.UnsetValue) // never null: a late gap_open cannot reopen a closed gap
	if !end.IsZero() {
		ended = end
	} else {
		end = e.ReceivedAt
	}
	start := g.Start
	if start.IsZero() {
		start = time.Unix(0, 0) // no boundary survived
	}
	start = start.UTC()
	// Skip days already out of retention, and clamp an end ahead of the writer clock.
	first := start.Truncate(day)
	if oldest := now.Add(-GapRetention - day).UTC().Truncate(day); first.Before(oldest) {
		first = oldest
	}
	if end.After(now) {
		end = now
	}
	for d := first; !d.After(end.UTC().Truncate(day)); d = d.Add(day) {
		ttl := ttlSeconds(d.Add(day+GapRetention), now)
		if ttl <= 0 {
			continue
		}
		if err := s.sess.Query(s.gap, e.Source, e.Collector, d, start, g.ID, ended, g.Reason, gen,
			int64(seq), l.SubscriptionHash, g.Resolved, ts, ttl).WithContext(ctx).Exec(); err != nil { //nolint:gosec // see ts
			return fmt.Errorf("gap %s day %s: %w", g.ID, d.Format(time.DateOnly), err)
		}
	}
	return nil
}

func orNil(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func micros(us int64) any {
	if us == 0 {
		return nil
	}
	return time.UnixMicro(us).UTC()
}
