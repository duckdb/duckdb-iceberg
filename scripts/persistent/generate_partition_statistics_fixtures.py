#!/usr/bin/env python3

import json
import shutil
import subprocess
from pathlib import Path


ROOT = Path("data/persistent")
GOOD_DIR = ROOT / "table_partition_statistics"
BAD_SIZE_DIR = ROOT / "table_partition_statistics_bad_size"
DUCKDB_BIN = Path("build/debug/duckdb")


def run_sql(sql: str) -> None:
    subprocess.run([str(DUCKDB_BIN), "-c", sql], check=True)


def write_parquet(path: Path, select_sql: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    run_sql(f"COPY ({select_sql}) TO '{path.as_posix()}' (FORMAT PARQUET)")
    return path.stat().st_size


def base_metadata(location: str, partition_statistics) -> dict:
    return {
        "location": location,
        "table-uuid": "ffffffff-1111-2222-3333-444444444444",
        "last-updated-ms": 1700000001000,
        "last-column-id": 2,
        "schemas": [
            {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": False},
                    {"id": 2, "name": "region", "type": "string", "required": False},
                ],
                "schema-id": 0,
                "identifier-field-ids": [],
            }
        ],
        "current-schema-id": 0,
        "partition-specs": [
            {
                "spec-id": 1,
                "fields": [
                    {
                        "source-id": 2,
                        "field-id": 1000,
                        "name": "region",
                        "transform": "identity",
                    }
                ],
            }
        ],
        "default-spec-id": 1,
        "last-partition-id": 1000,
        "properties": {},
        "current-snapshot-id": 1002,
        "snapshots": [
            {
                "snapshot-id": 1001,
                "sequence-number": 1,
                "timestamp-ms": 1700000000000,
                "manifest-list": f"{location}/metadata/snap-1001.avro",
                "summary": {"operation": "append"},
                "schema-id": 0,
            },
            {
                "snapshot-id": 1002,
                "sequence-number": 2,
                "timestamp-ms": 1700000001000,
                "manifest-list": f"{location}/metadata/snap-1002.avro",
                "summary": {"operation": "append"},
                "schema-id": 0,
            },
        ],
        "snapshot-log": [
            {"snapshot-id": 1001, "timestamp-ms": 1700000000000},
            {"snapshot-id": 1002, "timestamp-ms": 1700000001000},
        ],
        "metadata-log": [],
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "refs": {"main": {"snapshot-id": 1002, "type": "branch"}},
        "statistics": [],
        "partition-statistics": partition_statistics,
        "format-version": 2,
        "last-sequence-number": 2,
    }


def write_metadata(table_dir: Path, metadata: dict) -> None:
    metadata_dir = table_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "v1.metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (metadata_dir / "version-hint.text").write_text("1")


def create_good_fixture() -> None:
    if GOOD_DIR.exists():
        shutil.rmtree(GOOD_DIR)

    metadata_dir = GOOD_DIR / "metadata"
    file1 = metadata_dir / "partition-stats-1001.parquet"
    file2 = metadata_dir / "partition-stats-1002.parquet"

    file1_size = write_parquet(
        file1,
        """
        SELECT *
        FROM (
            VALUES
                (STRUCT_PACK(region := 'eu'), 1, 10, 1, 1000, 2, 1, 0, 0, 0, 12, 1700000000000, 1001),
                (STRUCT_PACK(region := 'us'), 1, 20, 2, 2200, 0, 0, 1, 0, 0, 21, 1700000000000, 1001)
        ) AS t(
            partition,
            spec_id,
            data_record_count,
            data_file_count,
            total_data_file_size_in_bytes,
            position_delete_record_count,
            position_delete_file_count,
            dv_count,
            equality_delete_record_count,
            equality_delete_file_count,
            total_record_count,
            last_updated_at,
            last_updated_snapshot_id
        )
        """.strip(),
    )
    file2_size = write_parquet(
        file2,
        """
        SELECT *
        FROM (
            VALUES
                (STRUCT_PACK(region := 'apac'), 1, 7, 1, 700),
                (STRUCT_PACK(region := 'latam'), 1, 3, 1, 350)
        ) AS t(
            partition,
            spec_id,
            data_record_count,
            data_file_count,
            total_data_file_size_in_bytes
        )
        """.strip(),
    )

    metadata = base_metadata(
        "data/persistent/table_partition_statistics",
        [
            {
                "snapshot-id": 1001,
                "statistics-path": "metadata/partition-stats-1001.parquet",
                "file-size-in-bytes": file1_size,
            },
            {
                "snapshot-id": 1002,
                "statistics-path": "metadata/partition-stats-1002.parquet",
                "file-size-in-bytes": file2_size,
            },
        ],
    )
    write_metadata(GOOD_DIR, metadata)


def create_bad_size_fixture() -> None:
    if BAD_SIZE_DIR.exists():
        shutil.rmtree(BAD_SIZE_DIR)

    metadata_dir = BAD_SIZE_DIR / "metadata"
    file_path = metadata_dir / "partition-stats-bad-size.parquet"
    file_size = write_parquet(
        file_path,
        """
        SELECT *
        FROM (
            VALUES
                (STRUCT_PACK(region := 'oops'), 1, 1, 1, 11)
        ) AS t(
            partition,
            spec_id,
            data_record_count,
            data_file_count,
            total_data_file_size_in_bytes
        )
        """.strip(),
    )

    metadata = base_metadata(
        "data/persistent/table_partition_statistics_bad_size",
        [
            {
                "snapshot-id": 1001,
                "statistics-path": "metadata/partition-stats-bad-size.parquet",
                "file-size-in-bytes": file_size + 1,
            }
        ],
    )
    write_metadata(BAD_SIZE_DIR, metadata)


if __name__ == "__main__":
    if not DUCKDB_BIN.exists():
        raise SystemExit(f"Missing DuckDB binary at {DUCKDB_BIN}")
    create_good_fixture()
    create_bad_size_fixture()
