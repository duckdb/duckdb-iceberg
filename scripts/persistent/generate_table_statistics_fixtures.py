#!/usr/bin/env python3

import json
import shutil
import struct
from pathlib import Path


ROOT = Path("data/persistent")
GOOD_DIR = ROOT / "table_statistics"
BAD_DIR = ROOT / "table_statistics_bad_footer"

MAGIC = b"PFA1"


def write_puffin(path: Path, blob_metadata, blob_payload: bytes) -> tuple[int, int]:
    payload = json.dumps({"blobs": blob_metadata}, separators=(",", ":")).encode("utf-8")
    footer = MAGIC + payload + struct.pack("<iI", len(payload), 0) + MAGIC
    path.write_bytes(MAGIC + blob_payload + footer)
    return path.stat().st_size, len(footer)


def base_metadata(location: str, statistics) -> dict:
    return {
        "location": location,
        "table-uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
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
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 999,
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
        "statistics": statistics,
        "partition-statistics": [],
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
    metadata_dir.mkdir(parents=True, exist_ok=True)

    blob1 = {
        "type": "apache-datasketches-theta-v1",
        "fields": [1],
        "snapshot-id": 1001,
        "sequence-number": 1,
        "offset": 4,
        "length": 9,
        "properties": {"ndv": "42"},
    }
    blob2 = {
        "type": "apache-datasketches-theta-v1",
        "fields": [2],
        "snapshot-id": 1002,
        "sequence-number": 2,
        "offset": 4,
        "length": 9,
        "properties": {"ndv": "7"},
    }

    file1 = metadata_dir / "stats-1001.puffin"
    file2 = metadata_dir / "stats-1002.puffin"
    size1, footer1 = write_puffin(file1, [blob1], b"blob-1001")
    size2, footer2 = write_puffin(file2, [blob2], b"blob-1002")

    metadata = base_metadata(
        "data/persistent/table_statistics",
        [
            {
                "snapshot-id": 1001,
                "statistics-path": "metadata/stats-1001.puffin",
                "file-size-in-bytes": size1,
                "file-footer-size-in-bytes": footer1,
                "blob-metadata": [
                    {
                        "type": "apache-datasketches-theta-v1",
                        "snapshot-id": 1001,
                        "sequence-number": 1,
                        "fields": [1],
                        "properties": {"ndv": "42"},
                    }
                ],
            },
            {
                "snapshot-id": 1002,
                "statistics-path": "metadata/stats-1002.puffin",
                "file-size-in-bytes": size2,
                "file-footer-size-in-bytes": footer2,
                "blob-metadata": [
                    {
                        "type": "apache-datasketches-theta-v1",
                        "snapshot-id": 1002,
                        "sequence-number": 2,
                        "fields": [2],
                        "properties": {"ndv": "7"},
                    }
                ],
            },
        ],
    )
    write_metadata(GOOD_DIR, metadata)


def create_bad_footer_fixture() -> None:
    if BAD_DIR.exists():
        shutil.rmtree(BAD_DIR)
    metadata_dir = BAD_DIR / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    blob = {
        "type": "apache-datasketches-theta-v1",
        "fields": [1],
        "snapshot-id": 1001,
        "sequence-number": 1,
        "offset": 4,
        "length": 8,
        "properties": {"ndv": "3"},
    }
    file_path = metadata_dir / "stats-bad-footer.puffin"
    file_size, footer_size = write_puffin(file_path, [blob], b"blob-bad")

    metadata = base_metadata(
        "data/persistent/table_statistics_bad_footer",
        [
            {
                "snapshot-id": 1001,
                "statistics-path": "metadata/stats-bad-footer.puffin",
                "file-size-in-bytes": file_size,
                "file-footer-size-in-bytes": footer_size - 1,
                "blob-metadata": [
                    {
                        "type": "apache-datasketches-theta-v1",
                        "snapshot-id": 1001,
                        "sequence-number": 1,
                        "fields": [1],
                        "properties": {"ndv": "3"},
                    }
                ],
            }
        ],
    )
    write_metadata(BAD_DIR, metadata)


if __name__ == "__main__":
    create_good_fixture()
    create_bad_footer_fixture()
