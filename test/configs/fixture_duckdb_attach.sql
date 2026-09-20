-- Non-destructive: rerun when the runner creates/recreates a database.
-- Destructive per-test cleanup belongs in fixture_duckdb_reset.inc.
CREATE SECRET (
    TYPE S3,
    KEY_ID 'admin',
    SECRET 'password',
    ENDPOINT '{S3_ENDPOINT}',
    URL_STYLE 'path',
    USE_SSL 0
);

ATTACH '' AS my_datalake (
    TYPE ICEBERG,
    CLIENT_ID 'admin',
    CLIENT_SECRET 'password',
    URI '{ICEBERG_ENDPOINT}',
    SUPPORT_NESTED_NAMESPACES true
);

CREATE SCHEMA IF NOT EXISTS my_datalake.main;
SET timezone = 'UTC';
SET iceberg_default_format_version = 3;
