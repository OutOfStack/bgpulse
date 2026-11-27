-- Lifecycle projections owned by persist. Every write uses
-- USING TIMESTAMP = raw stream creation (us) + stream sequence, so the newest
-- accepted control wins whatever order redeliveries arrive in. Nothing else
-- may write these tables.
--
-- One status row per collector: the latest control record. A null
-- active_gap_started_at with an active_gap_id means an unknown start.
CREATE TABLE IF NOT EXISTS {{keyspace}}.source_status_v1 (
    source text,
    collector text,
    connection_epoch text,
    stream_generation text,
    last_control_seq bigint,
    subscription_hash text,
    status text,
    reason text,
    last_received_at timestamp,
    last_accepted_at timestamp,
    last_event_time_us bigint,
    active_gap_id text,
    active_gap_started_at timestamp,
    updated_at timestamp,
    PRIMARY KEY ((source, collector))
);

-- A gap is copied into every UTC day it intersects, each copy expiring 30 days
-- after its day ends. started_at 1970-01-01 means no boundary survived, so the
-- gap covers every retained day up to its end. ended_at is never written null,
-- so a closed gap cannot reopen.
CREATE TABLE IF NOT EXISTS {{keyspace}}.source_gaps_v1 (
    source text,
    collector text,
    day date,
    started_at timestamp,
    gap_id text,
    ended_at timestamp,
    reason text,
    stream_generation text,
    last_control_seq bigint,
    subscription_hash text,
    resolved boolean,
    PRIMARY KEY ((source, collector, day), started_at, gap_id)
) WITH CLUSTERING ORDER BY (started_at DESC, gap_id ASC);
