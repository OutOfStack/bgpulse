# BGPulse

An Internet routing observatory. BGPulse ingests live BGP updates from public route collectors and reconstructs routing state for each observer. It then flags suspicious routing changes, such as unexpected origin ASNs and RPKI-invalid announcements.

## Why

BGP is how networks announce which IP prefixes they can reach. Routes change constantly. Links fail, providers change and policies shift, and sometimes there are misconfigurations or hijacks. Public collectors such as RIPE RIS and RouteViews publish what their peers see. BGPulse turns that stream into answers like these:

- What changed for this prefix, and which observers saw it?
- Which prefixes suddenly started originating from an unexpected ASN?
- Which networks are the noisiest, with the most route changes or withdrawals?
- Did one collector see a change before another?
- Which prefix/origin pairs fail RPKI validation?

It reports _observations from participating peers_, not the whole Internet, and flags _candidate_ anomalies, not confirmed hijacks.

## Try it

```bash
go run ./cmd/bgpulse -host rrc21
```

This streams live BGP UPDATEs from one RIPE RIS collector and prints each announcement (with its AS path and origin) and each withdrawal. Ctrl-C prints a summary.

## How

```
RIPE RIS Live ─▶ ingest ─▶ NATS JetStream ─▶ persist ─▶ ScyllaDB ─▶ HTTP API
 (RouteViews)               (durable buffer)  state          (history,
                                               analytics       state,
                                               RPKI            aggregates)
```

- **Go**, a single binary with subcommands (`setup`, `ingest`, `persist`, `state`, `api`, `replay`).
- **NATS JetStream** is the durable boundary: a message counts as accepted once JetStream has stored it, and consumers replay from there.
- **ScyllaDB** holds query-specific tables: time-bucketed raw history, per-prefix history, per-observer route state and windowed aggregates.
- At-least-once processing with deterministic IDs and idempotent writes. No exactly-once claims.
- Historical MRT archives are used for state bootstrap and for replay benchmarks (1k → 50k events/s).

## Roadmap

1. **Ingestion and history:** RIS Live → JetStream → Scylla, plus prefix history, gap tracking and a status API.
2. **Routing state:** per-observer RIB from MRT snapshots and live updates, with honest coverage labels.
3. **Analytics:** windowed churn and top lists, RPKI validation, a second collector or RouteViews, and candidate incidents.
4. **Performance and reliability:** replay benchmarks, an HA profile, and backup/restore.
