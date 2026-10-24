CREATE TABLE IF NOT EXISTS analytics.page_stats (
    window_start DateTime,
    window_end   DateTime,
    page         String,
    type         String,
    event_count  UInt64,
    updated_at   DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (window_start, page, type);