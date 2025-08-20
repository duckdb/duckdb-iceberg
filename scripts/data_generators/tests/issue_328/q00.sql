CREATE OR REPLACE TABLE default.issue_328(
  name string,
  meta struct<
    ts timestamp
  >
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
)
partitioned by (day(meta.ts));
