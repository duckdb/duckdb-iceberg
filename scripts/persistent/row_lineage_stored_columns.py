#!/usr/bin/env python3
"""Create a table upgraded from v2 to v3 whose first data file stores _row_id and _last_updated_sequence_number.

- Snapshot 1 (v2) adds stored-lineage.parquet. Its rows store neither, only _row_id, only
  _last_updated_sequence_number, or both.
- 00002-upgraded.metadata.json is the table right after the upgrade to v3: snapshot 1 has no first-row-id, so the
  data file has no first_row_id.
- Snapshot 2, the first v3 snapshot, appends one row and assigns first_row_id 0 to the data manifest of snapshot 1.

Run from the repository root.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.avro.file import AvroOutputFile
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import (
    MANIFEST_LIST_FILE_SCHEMAS,
    DataFile,
    DataFileContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestListWriter,
    ManifestListWriterV2,
    ManifestWriterV2,
    read_manifest_list,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType

OUTPUT_ROOT = Path("data/persistent/row_lineage_stored_columns")
TABLE_NAME = "default.row_lineage_stored_columns"
V2_SNAPSHOT_ID = 1_111_111_111_111_111_111
V3_SNAPSHOT_ID = 2_222_222_222_222_222_222
V2_TIMESTAMP_MS = 1_760_000_000_000
ROW_ID_FIELD_ID = 2147483540
LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID = 2147483539


class DataManifestWriterV3(ManifestWriterV2):
    @property
    def version(self):
        return 3

    def new_writer(self):
        manifest_schema = self._with_partition(self.version)
        return AvroOutputFile(
            output_file=self._output_file,
            file_schema=manifest_schema,
            record_schema=manifest_schema,
            schema_name="manifest_entry",
            metadata=self._meta,
        )


class ManifestListWriterV3(ManifestListWriterV2):
    """Writes a v3 manifest list, with each manifest's first_row_id."""

    def __init__(self, output_file, snapshot_id, parent_snapshot_id, sequence_number, first_row_id, compression):
        ManifestListWriter.__init__(
            self,
            format_version=3,
            output_file=output_file,
            meta={
                "snapshot-id": str(snapshot_id),
                "parent-snapshot-id": str(parent_snapshot_id),
                "sequence-number": str(sequence_number),
                "first-row-id": str(first_row_id),
                "format-version": "3",
                "avro.codec": compression,
            },
        )
        self._commit_snapshot_id = snapshot_id
        self._sequence_number = sequence_number

    def __enter__(self):
        self._writer = AvroOutputFile[ManifestFile](
            output_file=self._output_file,
            record_schema=MANIFEST_LIST_FILE_SCHEMAS[3],
            file_schema=MANIFEST_LIST_FILE_SCHEMAS[3],
            schema_name="manifest_file",
            metadata=self._meta,
        )
        self._writer.__enter__()
        return self


def with_first_row_id(manifest: ManifestFile, first_row_id: int) -> ManifestFile:
    return ManifestFile.from_args(
        _table_format_version=3,
        manifest_path=manifest.manifest_path,
        manifest_length=manifest.manifest_length,
        partition_spec_id=manifest.partition_spec_id,
        content=manifest.content,
        sequence_number=manifest.sequence_number,
        min_sequence_number=manifest.min_sequence_number,
        added_snapshot_id=manifest.added_snapshot_id,
        added_files_count=manifest.added_files_count,
        existing_files_count=manifest.existing_files_count,
        deleted_files_count=manifest.deleted_files_count,
        added_rows_count=manifest.added_rows_count,
        existing_rows_count=manifest.existing_rows_count,
        deleted_rows_count=manifest.deleted_rows_count,
        partitions=manifest.partitions,
        key_metadata=manifest.key_metadata,
        first_row_id=first_row_id,
    )


def field(name: str, arrow_type: pa.DataType, field_id: int, nullable: bool = True) -> pa.Field:
    return pa.field(name, arrow_type, nullable=nullable, metadata={b"PARQUET:field_id": str(field_id).encode()})


def write_parquet(path: Path, columns: dict[str, tuple[pa.Field, list]]) -> int:
    arrow_schema = pa.schema([arrow_field for arrow_field, _ in columns.values()])
    table = pa.Table.from_arrays([pa.array(values, type=f.type) for f, values in columns.values()], schema=arrow_schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path.stat().st_size


def data_file(format_version: int, path: Path, record_count: int, file_size: int) -> DataFile:
    return DataFile.from_args(
        _table_format_version=format_version,
        content=DataFileContent.DATA,
        file_path=str(path),
        file_format="PARQUET",
        partition=Record(),
        record_count=record_count,
        file_size_in_bytes=file_size,
        sort_order_id=None,
        spec_id=0,
        equality_ids=None,
        key_metadata=None,
    )


def write_metadata(table_root: Path, name: str, metadata: dict, previous: Path) -> Path:
    metadata["metadata-log"] = metadata.get("metadata-log", []) + [
        {"metadata-file": str(previous), "timestamp-ms": metadata["last-updated-ms"] - 1}
    ]
    path = table_root / "metadata" / f"{name}.metadata.json"
    path.write_text(json.dumps(metadata, indent=2))
    return path


def build_table() -> Path:
    shutil.rmtree(OUTPUT_ROOT, ignore_errors=True)
    OUTPUT_ROOT.mkdir(parents=True)

    catalog = SqlCatalog(
        "persistent",
        uri=f"sqlite:///{OUTPUT_ROOT}/catalog.db",
        warehouse=str(OUTPUT_ROOT / "warehouse"),
    )
    catalog.create_namespace("default")
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "val", StringType(), required=False),
    )
    table = catalog.create_table(TABLE_NAME, schema=schema, properties={"format-version": "2"})
    table_root = Path(table.location())
    created_path = Path(table.metadata_location)
    spec = PartitionSpec(spec_id=0)

    # Snapshot 1 (v2): one data file that stores lineage values next to the table columns.
    stored_path = table_root / "data" / "stored-lineage.parquet"
    stored_size = write_parquet(
        stored_path,
        {
            "id": (field("id", pa.int64(), 1, nullable=False), [1, 2, 3, 4]),
            "val": (field("val", pa.string(), 2), ["a", "b", "c", "d"]),
            "_row_id": (field("_row_id", pa.int64(), ROW_ID_FIELD_ID), [None, 100, None, 103]),
            "_last_updated_sequence_number": (
                field("_last_updated_sequence_number", pa.int64(), LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID),
                [None, None, 7, 9],
            ),
        },
    )
    v2_manifest_path = table_root / "metadata" / "stored-lineage-m0.avro"
    with ManifestWriterV2(
        spec=spec,
        schema=schema,
        output_file=table.io.new_output(str(v2_manifest_path)),
        snapshot_id=V2_SNAPSHOT_ID,
        avro_compression="gzip",
    ) as writer:
        writer.add_entry(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=V2_SNAPSHOT_ID,
                sequence_number=None,
                file_sequence_number=None,
                data_file=data_file(2, stored_path, 4, stored_size),
            )
        )
    v2_manifest = writer.to_manifest_file()
    v2_list_path = table_root / "metadata" / f"snap-{V2_SNAPSHOT_ID}-stored-lineage.avro"
    with ManifestListWriterV2(
        output_file=table.io.new_output(str(v2_list_path)),
        snapshot_id=V2_SNAPSHOT_ID,
        parent_snapshot_id=None,
        sequence_number=1,
        compression="gzip",
    ) as list_writer:
        list_writer.add_manifests([v2_manifest])

    metadata = json.loads(created_path.read_text())
    metadata["snapshots"] = [
        {
            "snapshot-id": V2_SNAPSHOT_ID,
            "sequence-number": 1,
            "timestamp-ms": V2_TIMESTAMP_MS,
            "summary": {
                "operation": "append",
                "added-data-files": "1",
                "added-records": "4",
                "added-files-size": str(stored_size),
                "total-data-files": "1",
                "total-records": "4",
                "total-files-size": str(stored_size),
                "total-delete-files": "0",
                "total-position-deletes": "0",
                "total-equality-deletes": "0",
            },
            "manifest-list": str(v2_list_path),
            "schema-id": schema.schema_id,
        }
    ]
    metadata["current-snapshot-id"] = V2_SNAPSHOT_ID
    metadata["last-sequence-number"] = 1
    metadata["last-updated-ms"] = V2_TIMESTAMP_MS
    metadata["refs"] = {"main": {"snapshot-id": V2_SNAPSHOT_ID, "type": "branch"}}
    metadata["snapshot-log"] = [{"snapshot-id": V2_SNAPSHOT_ID, "timestamp-ms": V2_TIMESTAMP_MS}]
    v2_path = write_metadata(table_root, "00001-v2-append", metadata, created_path)

    # The upgrade to v3 changes only the table metadata: snapshot 1 keeps no first-row-id.
    metadata["format-version"] = 3
    metadata["next-row-id"] = 0
    metadata["last-updated-ms"] = V2_TIMESTAMP_MS + 1000
    upgraded_path = write_metadata(table_root, "00002-upgraded", metadata, v2_path)

    # Snapshot 2, the first v3 snapshot: appends one row. The data manifest of snapshot 1 gets first_row_id 0,
    # so its data file inherits first_row_id 0, and the new manifest starts after its 4 rows.
    appended_path = table_root / "data" / "appended.parquet"
    appended_size = write_parquet(
        appended_path,
        {
            "id": (field("id", pa.int64(), 1, nullable=False), [5]),
            "val": (field("val", pa.string(), 2), ["e"]),
        },
    )
    v3_manifest_path = table_root / "metadata" / "appended-m0.avro"
    with DataManifestWriterV3(
        spec=spec,
        schema=schema,
        output_file=table.io.new_output(str(v3_manifest_path)),
        snapshot_id=V3_SNAPSHOT_ID,
        avro_compression="gzip",
    ) as writer:
        writer.add_entry(
            ManifestEntry.from_args(
                _table_format_version=3,
                status=ManifestEntryStatus.ADDED,
                snapshot_id=V3_SNAPSHOT_ID,
                sequence_number=None,
                file_sequence_number=None,
                data_file=data_file(3, appended_path, 1, appended_size),
            )
        )
    v3_manifest = writer.to_manifest_file()

    carried_manifest = read_manifest_list(table.io.new_input(str(v2_list_path)))
    carried_manifest = list(carried_manifest)
    assert len(carried_manifest) == 1
    v3_list_path = table_root / "metadata" / f"snap-{V3_SNAPSHOT_ID}-stored-lineage.avro"
    with ManifestListWriterV3(
        output_file=table.io.new_output(str(v3_list_path)),
        snapshot_id=V3_SNAPSHOT_ID,
        parent_snapshot_id=V2_SNAPSHOT_ID,
        sequence_number=2,
        first_row_id=0,
        compression="gzip",
    ) as list_writer:
        list_writer.add_manifests([with_first_row_id(carried_manifest[0], 0), with_first_row_id(v3_manifest, 4)])

    v3_timestamp_ms = V2_TIMESTAMP_MS + 2000
    metadata["snapshots"].append(
        {
            "snapshot-id": V3_SNAPSHOT_ID,
            "parent-snapshot-id": V2_SNAPSHOT_ID,
            "sequence-number": 2,
            "timestamp-ms": v3_timestamp_ms,
            "first-row-id": 0,
            "added-rows": 5,
            "summary": {
                "operation": "append",
                "added-data-files": "1",
                "added-records": "1",
                "added-files-size": str(appended_size),
                "total-data-files": "2",
                "total-records": "5",
                "total-files-size": str(stored_size + appended_size),
                "total-delete-files": "0",
                "total-position-deletes": "0",
                "total-equality-deletes": "0",
            },
            "manifest-list": str(v3_list_path),
            "schema-id": schema.schema_id,
        }
    )
    metadata["current-snapshot-id"] = V3_SNAPSHOT_ID
    metadata["last-sequence-number"] = 2
    metadata["next-row-id"] = 5
    metadata["last-updated-ms"] = v3_timestamp_ms
    metadata["refs"] = {"main": {"snapshot-id": V3_SNAPSHOT_ID, "type": "branch"}}
    metadata["snapshot-log"].append({"snapshot-id": V3_SNAPSHOT_ID, "timestamp-ms": v3_timestamp_ms})
    final_path = write_metadata(table_root, "00003-v3-append", metadata, upgraded_path)
    (table_root / "metadata" / "version-hint.text").write_text("00003-v3-append")
    return final_path


if __name__ == "__main__":
    print(build_table())
