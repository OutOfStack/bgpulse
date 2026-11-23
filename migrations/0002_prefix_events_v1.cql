-- One row per prefix child, partitioned by canonical prefix and UTC hour of
-- source event time. shard is 0 in the pilot. next_hop is the first next hop,
-- attributes_json carries {"next_hops": [...]} only when there are several.
-- Withdrawals leave next_hop, path_json, origin_* and attributes_json unset.
CREATE TABLE IF NOT EXISTS {{keyspace}}.prefix_events_v1 (
    prefix text,
    bucket_start timestamp,
    shard smallint,
    event_time_us bigint,
    event_id text,
    message_id text,
    child_ordinal int,
    source text,
    collector text,
    observer_id text,
    peer_ip inet,
    peer_asn bigint,
    action text,
    next_hop inet,
    path_json text,
    origin_asn bigint,
    origin_resolution text,
    attributes_json text,
    expires_at timestamp,
    PRIMARY KEY ((prefix, bucket_start, shard), event_time_us, event_id)
) WITH CLUSTERING ORDER BY (event_time_us DESC, event_id ASC)
  AND default_time_to_live = 1800
  AND tombstone_gc = {'mode': 'immediate'}
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'HOURS', 'compaction_window_size': '1'};

-- Every history write sets its own TTL. The safety-net default follows the
-- 30-minute pilot budget instead of the original 24 hours.
ALTER TABLE {{keyspace}}.raw_messages_v1 WITH default_time_to_live = 1800;
