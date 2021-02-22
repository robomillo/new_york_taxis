CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS datasets;
CREATE SCHEMA IF NOT EXISTS tmp;
CREATE SCHEMA IF NOT EXISTS utils;
CREATE TABLE IF NOT EXISTS utils.s3_incremental
(
    bucket TEXT,
    key    TEXT,
    _table TEXT,
    PRIMARY KEY (bucket, key, _table)
)