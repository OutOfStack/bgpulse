# AGENTS.md

Guidance for AI coding agents working in this repository. Agent-specific notes live in their own files (e.g. `CLAUDE.md`).

## Git

- No AI attribution: no `Co-Authored-By` trailers in commits, no "Generated with ..." lines in PR descriptions.
- Commit style is Conventional Commits with a scope: `feat(event): ...`, `fix(jetstream): ...`, `ci: ...`. Work goes on a branch and lands through a PR to `main`.
- Name branches `<type>/<short-kebab-case-description>`. Use the Conventional Commit type that matches the change, such as `feat/dev-retention-budget` or `fix/consumer-ack-timeout`.
- Before committing, update the docs the change affects in the same commit: `README.md`, `docs/architecture.md`, and this file (commands, invariants, package list).

## Commands

```bash
make build                 # go build -o bin/ ./cmd/bgpulse
make test                  # go test -race ./...
make lint                  # golangci-lint v2.15.0 (installs it if missing); config in .golangci.yaml
make vuln                  # govulncheck (not part of golangci-lint)
go test -run TestNormalize ./internal/event        # one unit test

make up                    # NATS + Scylla from deploy/compose.yaml (docker compose, else podman compose)
make setup                 # create stream/consumer + apply migrations; make setup ARGS=-check verifies only
make test-integration      # go test -race -tags=integration ./tests/integration/... (needs make up)
go test -tags=integration -run TestRoundTrip ./tests/integration   # one integration test
make down                  # keeps volumes; add -v via compose to wipe

go run ./cmd/bgpulse watch -host rrc21   # live RIS viewer, needs internet only
go run ./cmd/bgpulse sample -duration 30s   # rrc21 traffic summary as JSON (default 30m)
go run ./cmd/bgpulse ingest                 # live rrc21 capture after setup, needs NATS only
go run ./cmd/bgpulse persist                # history consumer after setup, needs NATS and Scylla
```

- Formatting is enforced by lint (gci import groups, gofmt, goimports). Fix with `golangci-lint fmt`. Integration code is behind a build tag: lint it with `golangci-lint run --build-tags=integration ./tests/...`.
- `go mod tidy -diff` runs in CI.
- Scylla defaults to `--smp 2 --memory 4G`; override with `SCYLLA_SMP` / `SCYLLA_MEMORY` on small hosts.
- Endpoints come from flags or env: `BGPULSE_NATS_URL` (default `nats://127.0.0.1:4222`) and `BGPULSE_SCYLLA_HOSTS` (default `127.0.0.1:9042`).

## Design source of truth

The full design is in `docs/local/plan.md`, with per-scope decisions in `docs/local/scope_NN.md`. `docs/local/` is gitignored: it exists only in the local checkout, not on GitHub or in CI. Read plan.md before changing identities, schemas, stream config or ack/retry behaviour. Section numbers (§6, §7.2, P1.2…) in code reviews and commits refer to it.

## Architecture

Target pipeline: RIPE RIS Live → `ingest` → NATS JetStream (raw stream, durable boundary) → `persist` → ScyllaDB (query-specific tables) → HTTP `api`. Later phases add `state`, analytics, RPKI and `replay`. One binary, `cmd/bgpulse`, with subcommands. Implemented so far: `watch` (live viewer), `sample` (rrc21 traffic measurement), `setup` (provisioning), `ingest` (durable capture) and `persist` (history consumer). Planned packages and their phases are listed in plan.md §2.

Invariants that span several files:

- **Deterministic identity.** `internal/event.Normalize` turns one RIS JSON frame into a message plus ordered prefix children. `message_id`, `observer_id` and `event_id` are SHA-256 hashes of length-prefixed parts and will become Scylla primary keys. The golden values in `internal/event/event_test.go` are a contract: changing them is a schema change, not a test fix.
- **At-least-once.** A message is accepted once JetStream PubAcks it. Consumers ACK only after all required writes, and writes must be idempotent on those IDs. There is no exactly-once.
- **Ingest order.** `internal/ris` verifies the pilot subscription. `internal/ingest` serializes source and gap controls through one bounded FIFO. An uncertain PubAck retries identical bytes and ID. Look up the authoritative subject tail before each epoch. Only a missing record gives an unknown restart boundary.
- **Persist ACK.** `internal/persist` ACKs a record only after all its rows are written, after a quarantine PubAck, or when the history deadline has already passed. Transient failures are NAKed with 1 s to 30 s backoff and no MaxDeliver. A `schema_version` above 1 or an unknown `kind` stops the consumer and leaves the record pending; it is never quarantined as bad data.
- **Lifecycle projection order.** `source_status_v1` and `source_gaps_v1` are written only with `USING TIMESTAMP` = raw stream creation time in µs + stream sequence, so the newest accepted control wins in any delivery order. Never write them with server timestamps. Gap `ended_at` is never written null.
- **Only `setup` mutates infrastructure.** `setup` creates the stream/consumer and applies CQL. Other roles call the same `Check` functions and refuse to start on drift; they never auto-migrate.
- **Namespace.** A namespace (`live`, later replay runs) is part of subjects (`bgpulse.<ns>.raw.v1.ris.<collector>`), the stream name (`BGPULSE_<NS>_RAW_V1`) and the keyspace (`bgpulse_<ns>`). It is validated once in `cmd/bgpulse/setup.go`; the packages assume a valid one, because it is interpolated into CQL.
- **JetStream resources** are declared in `internal/jetstream` (`RawStream`, `QuarantineStream`, `History`).
  - The RF=1 pilot budgets 20 minutes for raw replay (10 GiB cap) and 30 minutes from event time for history writes. Recheck the [capacity worksheet](docs/benchmarks/dev-capacity.md) when changing retention or collector scope.
  - `Ensure` relies on server Create semantics: an identical config is a no-op, a different one is rejected, never overwritten.
  - `Check` compares an explicit list of declared fields. When you add a field to a declared config, add it to `Check` too.
- **Migrations** in `migrations/NNNN_name.cql`:
  - Embedded into the binary and applied in order by `internal/store`. Checksums are recorded in `<keyspace>.schema_migrations`, and a changed applied file fails setup.
  - Never edit an applied file: add the next number.
  - Use `{{keyspace}}` instead of a keyspace name, and end statements with `;`. Whole-line `--` comments are dropped before splitting, so they may contain `;`. Elsewhere `;` may only end a statement.
  - `.gitattributes` forces LF so checksums match across OSes.
  - Key or column changes add new versioned tables (`*_v2`) rather than altering existing ones. Table options such as the default TTL may be altered.
- **History expiry.** Every raw and prefix insert sets the remaining TTL to event time + 30 minutes and never sends TTL 0 ([details](docs/architecture.md#history-rows)). Bucket widths and shard counts in `internal/store/history.go` are part of the v1 table schemas: changing them needs new tables.
- **Scylla driver.** Code imports `github.com/gocql/gocql`, and a `replace` in go.mod points it at Scylla's shard-aware fork (whose go.mod still declares the gocql path). `.golangci.yaml` allow-lists that replace.
- **Toolchain.** go.mod has `go 1.27.0` plus `toolchain go1.27.4`; the toolchain pin is what clears the stdlib advisories govulncheck reports. CI installs the latest `1.27.x` patch (`check-latest`), so bump the pin when govulncheck flags a newer stdlib fix.
- **NATS durability.** `deploy/nats/nats.conf` sets `sync_interval: always` (PubAck waits for fsync). An integration test asserts it, so don't relax it in the default profile.

## Tests

- Unit tests are offline and table-driven.
- Integration tests (`tests/integration`, build tag `integration`) run against the real pinned containers. Each test uses a random namespace and drops its stream and keyspace on cleanup.
- CI (`.github/workflows/ci.yml`) has two jobs:
  - `test`: tidy, build, lint, race tests, a 5s `FuzzNormalize` run, govulncheck.
  - `integration`: compose up, setup, integration tests, then restart + `setup -check` to prove volumes persist.
