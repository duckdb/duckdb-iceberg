# README

## Why this copy exists
This table was written by Spark 3.3.1 with Iceberg 1.0.0. The DELETED manifest entry in
`10eaca8a-...-m0.avro` has NULL `sequence_number` and `file_sequence_number`; the Iceberg spec only allows
inheriting a NULL sequence number for ADDED entries. Used as a regression test for metadata functions
(`iceberg_metadata`, `iceberg_column_stats`) failing on such entries.
