CREATE TABLE IF NOT EXISTS {{keyspace}}.raw_messages_v1 (
    source text,
    collector text,
    bucket_start timestamp,
    shard smallint,
    event_time_us bigint,
    message_id text,
    source_message_id text,
    observer_id text,
    message_type text,
    payload_hash text,
    payload blob,
    expires_at timestamp,
    PRIMARY KEY ((source, collector, bucket_start, shard), event_time_us, message_id)
) WITH CLUSTERING ORDER BY (event_time_us DESC, message_id ASC)
  AND default_time_to_live = 86400
  AND tombstone_gc = {'mode': 'immediate'}
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'HOURS', 'compaction_window_size': '1'};
