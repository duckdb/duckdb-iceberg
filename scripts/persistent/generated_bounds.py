#!/usr/bin/env python3
"""
Generate a minimal iceberg dataset that triggers the use-after-free crash
in the AvroReader -> FinalizeChunk -> ReadChunk -> GetBounds -> Value::BLOB path.

Single manifest with 5000 rows. First 3000 rows have int-only bounds (1 MAP entry),
last 2000 rows have int+string bounds (2 MAP entries, string >12 bytes).
The mixed MAP sizes within a single manifest trigger VLB resize that causes
stale string_t pointers in GetBounds.
"""

import os
import shutil
import struct
import uuid

from pyiceberg.io import load_file_io
from pyiceberg.manifest import (
    DataFile, DataFileContent, FileFormat,
    ManifestEntry, ManifestEntryStatus,
    ManifestWriterV2, ManifestListWriterV2,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.refs import SnapshotRef
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.sorting import SortOrder
from pyiceberg.typedef import Record
from pyiceberg.types import NestedField, IntegerType, StringType

OUT_DIR = os.path.join("data/persistent", "generated_bounds")
META_DIR = os.path.join(OUT_DIR, "metadata")

if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)
os.makedirs(META_DIR)

SNAPSHOT_ID = 1000000000000000001
TOTAL_ROWS = 5000
TRANSITION_AT = 3000

schema = Schema(
    NestedField(1, "block_number", IntegerType(), required=False),
    NestedField(2, "from", StringType(), required=False),
)
spec = PartitionSpec()
io = load_file_io({"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"})

entries = []
for i in range(TOTAL_ROWS):
    block_lo = 1 + i * 100
    block_hi = block_lo + 99

    lower = {1: struct.pack("<i", block_lo)}
    upper = {1: struct.pack("<i", block_hi)}

    if i >= TRANSITION_AT:
        lower[2] = f"0x{i:040x}".encode()
        upper[2] = ("0x" + "f" * 40).encode()

    df = DataFile.from_args(
        _table_format_version=2,
        content=DataFileContent.DATA,
        file_path=f"s3://fake-bucket/d/{i:010d}/t.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=1000,
        file_size_in_bytes=10000,
        column_sizes={1: 4000, 2: 8000},
        value_counts={1: 1000, 2: 1000},
        null_value_counts={1: 0, 2: 0},
        nan_value_counts={},
        lower_bounds=lower,
        upper_bounds=upper,
        sort_order_id=0,
    )
    entries.append(ManifestEntry(ManifestEntryStatus.ADDED, SNAPSHOT_ID, None, None, df))

manifest_path = os.path.join(META_DIR, "manifest-m0.avro")
with ManifestWriterV2(spec, schema, io.new_output(f"{manifest_path}"), SNAPSHOT_ID, "null") as writer:
    for e in entries:
        writer.add_entry(e)
    manifest_file = writer.to_manifest_file()

ml_path = os.path.join(META_DIR, f"snap-{SNAPSHOT_ID}.avro")
with ManifestListWriterV2(io.new_output(f"{ml_path}"), SNAPSHOT_ID, None, 1, "null") as ml_writer:
    ml_writer.add_manifests([manifest_file])

metadata = TableMetadataV2(
    location=f"{OUT_DIR}",
    table_uuid=uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
    last_updated_ms=1700000000000,
    last_column_id=2,
    schemas=[Schema(
        NestedField(1, "block_number", IntegerType(), required=False),
        NestedField(2, "from", StringType(), required=False),
        schema_id=0,
    )],
    partition_specs=[PartitionSpec(spec_id=0)],
    last_partition_id=999,
    properties={},
    current_snapshot_id=SNAPSHOT_ID,
    snapshots=[Snapshot(
        snapshot_id=SNAPSHOT_ID,
        sequence_number=1,
        timestamp_ms=1700000000000,
        manifest_list=f"{ml_path}",
        summary={"operation": "append"},
        schema_id=0,
    )],
    snapshot_log=[SnapshotLogEntry(snapshot_id=SNAPSHOT_ID, timestamp_ms=1700000000000)],
    metadata_log=[],
    sort_orders=[SortOrder(order_id=0)],
    refs={"main": SnapshotRef(snapshot_id=SNAPSHOT_ID, snapshot_ref_type="branch")},
    statistics=[],
    partition_statistics=[],
    last_sequence_number=1,
)

with open(os.path.join(META_DIR, "v1.metadata.json"), "w") as f:
    f.write(metadata.model_dump_json(indent=2))
with open(os.path.join(META_DIR, "version-hint.text"), "w") as f:
    f.write("1")

print(f"Generated {TOTAL_ROWS} manifest entries ({TRANSITION_AT} int-only + {TOTAL_ROWS - TRANSITION_AT} int+string bounds)")
print(f"Output: {OUT_DIR}")
