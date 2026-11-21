# RIS Live capture and sequential PubAck measurements

## Observed on 2026-11-21

From 11:02:32 to 11:32:32 UTC, the probe received **936,700 source messages** from `rrc21` and normalized **2,581,508 prefix occurrences**. That averaged **520.4 messages/s** and **1,434.2 prefix occurrences/s**, including reconnect time. Source frames totaled 421,328,693 bytes. [Machine-readable result](rrc21-2026-11-21.json)

| Per source message | Mean | p50 | p95 | p99 | Maximum |
|---|---:|---:|---:|---:|---:|
| JSON payload bytes | 449.8 | 413 | 615 | 997 | 18,042 |
| Prefix occurrences | 2.76 | 1 | 7 | 27 | 1,006 |

The normalizer accepted all 936,700 messages. It counted 933,454 UPDATEs and 2,449 KEEPALIVEs. No messages carried the exact type names `OPEN`, `NOTIFICATION`, or `RIS_PEER_STATE`. The probe grouped 797 messages under `other` and discarded their type names, so their lifecycle significance is unknown. An offline fixture exercises the documented `RIS_PEER_STATE` type.

A separate 12-second verification run starting at 15:38:38 UTC retained exact type names. It recorded 1,811 `UPDATE`, 14 `KEEPALIVE`, and 2 `STATE` messages, with zero overflow. These counts belong only to that later run.

Both subscriptions were acknowledged. The first delivered 467,955 messages and the second delivered 468,745. The last observation before the planned reconnect arrived at 11:17:32.185 UTC and the first after it at 11:17:32.843 UTC, an interval of **658 ms**. There were no unplanned disconnects in this run. A separate preliminary connection ended with EOF after 77,518 messages and was excluded from these figures.

## Repeat the capture

```sh
go run ./cmd/bgpulse sample > sample.json
```

The probe requests every message type for `rrc21` over an acknowledged WebSocket subscription with `includeRaw=false`. Its default duration is 30 minutes of wall time, including connection setup and interruptions. It opens a fresh subscription halfway through and verifies the collector on incoming data. [RIPE RIS Live protocol](https://ris-live.ripe.net/manual/)

Session timestamps are UTC receiver times. Comparing the last observation before a disconnect with the first after resubscription gives an interval of uncertain coverage. RIS Live supplies no replay cursor to count missed messages. The probe records a session end on a transport disconnect or a 30-second read timeout. It retries those interruptions and network dial failures after one second, with up to 16 attempts per half. Each half must see source data. An oversized frame (1 MiB limit), source error, failed subscription check or exhausted retry budget ends the run with a nonzero exit and a partial JSON report marked `complete: false`.

New runs keep the first 16 distinct type names of up to 64 bytes in `valid_message_types`. Messages with longer names or additional untracked names increment `type_overflow_messages`. Their sizes and prefix counts still contribute to the distributions.

`payload_bytes` measures the complete JSON source frame. It includes source messages rejected by the normalizer. `prefixes_per_valid_message` counts normalized child occurrences, including zero for peer state and keepalives. Both distributions report nearest-rank percentiles. The rates include reconnect time. Control frames are excluded from both denominators.

## Sequential JetStream PubAck

With the pinned dev NATS server running, reproduce the measurement with

```sh
go test -tags=integration ./tests/integration -run '^TestSyncAlways$' -bench '^BenchmarkSequentialPubAck$' -benchtime=3000x -count=3
```

On 2026-11-21, a Linux/amd64 Go 1.27.4 client and NATS 2.15.2 ran in the same disposable Podman container on WSL2, on an AMD Ryzen 7 5700X. Go used two logical CPUs. The server used the repository's NATS config, its Linux filesystem and one file-backed replica. The monitor reported `sync_always: true`. The VM's cgroup controllers did not support separate container limits.

Each publish sent the same 424-byte captured UPDATE, with one request outstanding and no de-duplication header. Each pass created an empty stream and removed it afterward. Three passes of 3,000 acknowledged writes averaged **170.5, 170.0 and 168.6 µs per PubAck** (about 5,900 publishes/s). This measures local sequential publication of a single payload size. Live envelope overhead, sustained disk growth and Scylla writes have separate measurement gates. [Captured UPDATE](../../testdata/ris/rrc21-update.json)
