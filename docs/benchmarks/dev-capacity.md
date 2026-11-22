# Development capacity for rrc21

Use a **20-minute JetStream maximum age, 10 GiB byte cap and 30-minute event-time history budget** for one RF=1, all-types `rrc21` subscription. The budget covers one namespace. It uses the November 21 traffic sample and two short acknowledged captures from the pilot. Shorter coverage is possible during bursts, source gaps or outages. [Capture report](rrc21-sample.md) · [Sample JSON](rrc21-2026-11-21.json)

## Inputs

| Input | Value | Basis |
|---|---:|---|
| Source rate | 520.39 messages/s | 936,700 messages in 1,800 s including reconnect |
| Prefix rate | 1,434.17 occurrences/s | 2,581,508 occurrences in the same window |
| Source bytes | 234,071 B/s | 421,328,693 frame bytes, averaging 450 B/message |
| JetStream record | 1,280 B/message | Estimated mean including source bytes, serialized envelope, subject, ID header and broker framing |
| Scylla raw / prefix row | 1,280 / 768 B | Estimated mean logical row sizes including keys, payload or path and attributes |

The observed p95/p99 frames were 615/997 B, and p95/p99 fanout was 7/27 children. The largest frame was 18,042 B and the largest fanout was 1,006. Use aggregate bytes and child counts for storage sizing. Recheck both rates during bursts. Sustaining the sample mean for 24 hours would produce **18.83 GiB of source JSON alone**.

On November 22, the pinned NATS server retained **511 B/message** for 3,000 acknowledged copies of the 424-byte UPDATE on the benchmark subject. That gives 87 B of broker overhead for this subject and payload with no ID header. For the pilot estimate, rounding the mean frame to 450 B and adding 87 B leaves 743 B of the 1,280 B allowance for envelope fields, encoding and the ID header. Repeat the [sequential PubAck benchmark](rrc21-sample.md#sequential-jetstream-puback) and read its `retained-B/msg` metric. It uses JetStream's accounted stream bytes.

### Acknowledged capture on November 21 (UTC)

Two bounded runs of `bgpulse ingest` used a disposable live stream on the pinned always-sync broker. Each stopped on SIGTERM and drained with zero pending records. The table uses the change in broker bytes divided by the requested capture duration, including subscription setup. Source and prefix counts come from decoding all retained envelopes.

| Start UTC | Duration | Source messages | Prefix occurrences | Lifecycle records | Retained bytes | B/s |
|---|---:|---:|---:|---:|---:|---:|
| 22:51:39 | 8 s | 26,357 | 65,748 | 5 | 33,465,879 | 4,183,235 |
| 22:52:35 | 60 s | 61,076 | 149,415 | 4 | 76,755,539 | 1,279,259 |

To repeat on a disposable Linux stack, provision it with `bgpulse setup`, record the monitor's `/jsz` bytes and messages, run `timeout --preserve-status -s TERM 60 bgpulse ingest`, then read the counters after the drain. Use a single active stream and a pre-expiry interval. Both runs exceeded the 994,205 B/s budget for the original two-hour proposal. The shorter run sets the broker burst budget below. The minute-long run supplies the history workload for this estimate.

## Disk budget on your host

For each filesystem holding the volumes, calculate usable growth as free bytes minus 30% of total capacity. If a VM uses a growing disk image, also check the filesystem holding that image and use the smaller budget. Guest expiry and compaction can touch new blocks while the backing image retains its previous allocations.

## Broker replay and history

All sizes below use 2³⁰ bytes/GiB. The broker rates are measured. Scylla costs use estimated row sizes until the writer lands.

| Calculation | Result |
|---|---:|
| 20 minutes at the 8-second broker rate × 1.5 traffic margin | 7.01 GiB |
| Time to fill an empty 10 GiB stream at that rate, ignoring expiry | 42.78 min |
| Logical history growth = (61,076 × 1,280 + 149,415 × 768) / 60 | 3,215,467 B/s |
| 30 minutes of queryable raw and prefix rows | 5.39 GiB |
| Scylla disk allowance = growth × (0.5 h TTL + 1 h TWCS window + 1 h compaction backlog) × 1.5 for storage overhead and bursts | 40.43 GiB |
| Full broker cap + Scylla allowance | **50.43 GiB** estimated resident footprint |

At either measured rate, age limits replay to **at most 20 minutes**. With `DiscardNew`, reaching 10 GiB first rejects publishes until space becomes available. Consumer ACKs do not release space under limits retention. Inspect the oldest retained record's timestamp and publish rejections to establish actual coverage.

The planned history writer uses a fixed deadline of `event_time + 30m` for both raw and prefix rows. Each retry uses the remaining positive TTL and skips expired rows. Migration 0001 gives raw inserts that omit TTL a 24-hour lifetime. Such a row can delay reclaiming its whole SSTable beyond the allowance, so every history write must set TTL. Production history writes and prefix tables arrive in phase 1. Expiry bounds query visibility. Compaction timing, compression and concurrent writes determine physical disk usage.

## Validating the budget

Before sustained capture, repeat the stored byte-rate measurement over representative busy intervals. The 10 GiB cap supports a 20-minute age with the 1.5 margin up to **5,965,232 B/s**. If growth exceeds that threshold, shorten the age, narrow collection or allocate more storage before continuing. If the 8-second source and prefix rates persist, their estimated 10.53 MB/s of logical history would more than triple the modeled history growth rate.

When the writer lands, measure disk growth per raw and prefix row. Track volume usage and available space on the backing filesystem through at least one full expiry and compaction cycle under the pinned stack. Recalculate the 40.43 GiB allowance from the measured disk growth. Continue monitoring both filesystems throughout capture. Stop or resize before any affected filesystem falls below 30% free. Allow separate space for concurrent replay, snapshots or other namespaces.

## Existing streams

An existing 24-hour stream fails the new setup drift check. Shortening its age can immediately expire older records. For isolated testing, provision a fresh namespace with `bgpulse setup -namespace pilot` if there is sufficient additional disk. For an existing deployment, inspect pending consumer work and preserve any required older records before an operator changes MaxAge to 20 minutes. Verify the result with `bgpulse setup -check` for that namespace. Setup preserves existing streams on drift.
