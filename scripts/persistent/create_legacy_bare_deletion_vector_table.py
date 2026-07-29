#!/usr/bin/env python3

from __future__ import annotations

import json
import shutil
import struct
import zlib
from pathlib import Path

import pyarrow as pa
from pyroaring import BitMap
from pyiceberg.avro.file import AvroOutputFile
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestListWriter,
    ManifestListWriterV2,
    ManifestWriterV2,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType


OUTPUT_ROOT = Path("data/persistent/legacy_bare_deletion_vector")
TABLE_NAME = "default.legacy_bare_deletion_vector"
SNAPSHOT_ID = 7_777_777_777_777_777_776
DELETION_VECTOR_MAGIC = b"\xD1\xD3\x39\x64"


class DeleteManifestWriterV3(ManifestWriterV2):
    @property
    def version(self):
        return 3

    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}

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
    def __init__(
        self, output_file, snapshot_id, parent_snapshot_id, sequence_number, compression
    ):
        ManifestListWriter.__init__(
            self,
            format_version=3,
            output_file=output_file,
            meta={
                "snapshot-id": str(snapshot_id),
                "parent-snapshot-id": str(parent_snapshot_id),
                "sequence-number": str(sequence_number),
                "format-version": "3",
                "avro.codec": compression,
            },
        )
        self._commit_snapshot_id = snapshot_id
        self._sequence_number = sequence_number


def deletion_vector_blob(*positions: int) -> bytes:
    """Serialize one 32-bit Roaring bitmap as an Iceberg deletion-vector-v1 blob."""
    roaring_bitmap = BitMap(positions).serialize()
    checksummed_data = (
        DELETION_VECTOR_MAGIC
        + struct.pack("<q", 1)
        + struct.pack("<i", 0)
        + roaring_bitmap
    )
    checksum = struct.pack(">I", zlib.crc32(checksummed_data))
    vector_size = struct.pack(">I", len(checksummed_data))
    return vector_size + checksummed_data + checksum


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
        NestedField(2, "source", StringType(), required=True),
    )
    # PyIceberg can write v3 manifest schemas but does not yet serialize v3 table metadata.
    # Create the data as v2, then explicitly upgrade the final metadata below.
    table = catalog.create_table(
        TABLE_NAME, schema=schema, properties={"format-version": "2"}
    )
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 1, "source": "legacy"},
                {"id": 2, "source": "legacy"},
                {"id": 3, "source": "legacy"},
            ],
            schema=schema.as_arrow(),
        )
    )
    table.refresh()

    base_snapshot = table.current_snapshot()
    assert base_snapshot is not None
    data_manifests = base_snapshot.manifests(io=table.io)
    data_files = [
        entry.data_file
        for manifest in data_manifests
        if manifest.content == ManifestContent.DATA
        for entry in manifest.fetch_manifest_entry(table.io)
        if entry.status != ManifestEntryStatus.DELETED
    ]
    assert len(data_files) == 1
    data_file = data_files[0]

    table_root = Path(table.location())
    deletion_vector_path = table_root / "data" / "legacy-bare-deletion-vector.puffin"
    blob = deletion_vector_blob(1)

    # DuckDB Iceberg 1.5.3 labeled this as Puffin, but wrote only the DV blob. The
    # content offset and size still locate the blob correctly, so legacy readers worked.
    deletion_vector_path.write_bytes(blob)

    sequence_number = base_snapshot.sequence_number + 1
    delete_manifest_path = (
        table_root / "metadata" / "legacy-bare-deletion-vector-m0.avro"
    )
    with DeleteManifestWriterV3(
        spec=PartitionSpec(spec_id=0),
        schema=table.schema(),
        output_file=table.io.new_output(str(delete_manifest_path)),
        snapshot_id=SNAPSHOT_ID,
        avro_compression="gzip",
    ) as writer:
        writer.add_entry(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=SNAPSHOT_ID,
                sequence_number=sequence_number,
                file_sequence_number=sequence_number,
                data_file=DataFile.from_args(
                    _table_format_version=3,
                    content=DataFileContent.POSITION_DELETES,
                    file_path=str(deletion_vector_path),
                    file_format="PUFFIN",
                    partition=Record(),
                    record_count=1,
                    file_size_in_bytes=len(blob),
                    equality_ids=None,
                    sort_order_id=None,
                    referenced_data_file=data_file.file_path,
                    content_offset=0,
                    content_size_in_bytes=len(blob),
                    spec_id=0,
                ),
            )
        )
    delete_manifest = writer.to_manifest_file()

    manifest_list_path = (
        table_root / "metadata" / "snap-legacy-bare-deletion-vector.avro"
    )
    with ManifestListWriterV3(
        output_file=table.io.new_output(str(manifest_list_path)),
        snapshot_id=SNAPSHOT_ID,
        parent_snapshot_id=base_snapshot.snapshot_id,
        sequence_number=sequence_number,
        compression="gzip",
    ) as manifest_list_writer:
        manifest_list_writer.add_manifests([*data_manifests, delete_manifest])

    old_metadata = json.loads(Path(table.metadata_location).read_text())
    timestamp_ms = base_snapshot.timestamp_ms + 1
    old_metadata["format-version"] = 3
    old_metadata["next-row-id"] = 3
    old_metadata["snapshots"].append(
        {
            "sequence-number": sequence_number,
            "snapshot-id": SNAPSHOT_ID,
            "parent-snapshot-id": base_snapshot.snapshot_id,
            "timestamp-ms": timestamp_ms,
            "summary": {
                "operation": "delete",
                "added-delete-files": "1",
                "added-position-deletes": "1",
                "total-delete-files": "1",
                "total-position-deletes": "1",
                "total-data-files": "1",
                "total-records": "3",
            },
            "manifest-list": str(manifest_list_path),
            "schema-id": table.schema().schema_id,
        }
    )
    old_metadata["current-snapshot-id"] = SNAPSHOT_ID
    old_metadata["last-sequence-number"] = sequence_number
    old_metadata["last-updated-ms"] = timestamp_ms
    old_metadata["refs"] = {"main": {"snapshot-id": SNAPSHOT_ID, "type": "branch"}}
    old_metadata["snapshot-log"].append(
        {"snapshot-id": SNAPSHOT_ID, "timestamp-ms": timestamp_ms}
    )

    final_metadata_path = table_root / "metadata" / "00002-legacy-bare-dv.metadata.json"
    final_metadata_path.write_text(json.dumps(old_metadata, indent=2))
    (table_root / "metadata" / "version-hint.text").write_text("00002-legacy-bare-dv")
    return final_metadata_path


if __name__ == "__main__":
    print(build_table())
