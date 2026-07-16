#!/usr/bin/env python3

from __future__ import annotations

import json
import shutil
import struct
import zlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
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


OUTPUT_ROOT = Path("data/persistent/multi_blob_deletion_vectors")
TABLE_NAME = "default.multi_blob_deletion_vectors"
SNAPSHOT_ID = 7_777_777_777_777_777_777
PUFFIN_MAGIC = b"PFA1"
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
    def __init__(self, output_file, snapshot_id, parent_snapshot_id, sequence_number, compression):
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
    checksummed_data = DELETION_VECTOR_MAGIC + struct.pack("<q", 1) + struct.pack("<i", 0) + roaring_bitmap
    checksum = struct.pack(">I", zlib.crc32(checksummed_data))
    vector_size = struct.pack(">I", len(checksummed_data))
    return vector_size + checksummed_data + checksum


def write_puffin(path: Path, vectors: list[tuple[str, bytes, int]]) -> list[tuple[int, int]]:
    body = bytearray(PUFFIN_MAGIC)
    blob_metadata = []
    locations = []

    for referenced_data_file, blob, cardinality in vectors:
        offset = len(body)
        body.extend(blob)
        locations.append((offset, len(blob)))
        blob_metadata.append(
            {
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": -1,
                "sequence-number": -1,
                "offset": offset,
                "length": len(blob),
                "properties": {
                    "referenced-data-file": referenced_data_file,
                    "cardinality": str(cardinality),
                },
            }
        )

    footer_payload = json.dumps({"blobs": blob_metadata}, separators=(",", ":")).encode()
    body.extend(PUFFIN_MAGIC)
    body.extend(footer_payload)
    body.extend(struct.pack("<i", len(footer_payload)))
    body.extend(struct.pack("<I", 0))
    body.extend(PUFFIN_MAGIC)
    path.write_bytes(body)
    return locations


def build_table() -> Path:
    shutil.rmtree(OUTPUT_ROOT, ignore_errors=True)
    OUTPUT_ROOT.mkdir(parents=True)

    catalog = SqlCatalog(
        "persistent",
        uri=f"sqlite:///{OUTPUT_ROOT.resolve()}/catalog.db",
        warehouse=f"file://{OUTPUT_ROOT.resolve() / 'warehouse'}",
    )
    catalog.create_namespace("default")
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "source", StringType(), required=True),
    )
    # PyIceberg can write v3 manifest schemas but does not yet serialize v3 table metadata.
    # Create the base data as v2, then explicitly upgrade the final metadata below.
    table = catalog.create_table(TABLE_NAME, schema=schema, properties={"format-version": "2"})

    arrow_schema = schema.as_arrow()
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 1, "source": "first"},
                {"id": 2, "source": "first"},
                {"id": 3, "source": "first"}
            ],
            schema=arrow_schema,
        )
    )
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 4, "source": "second"},
                {"id": 5, "source": "second"},
                {"id": 6, "source": "second"}
            ],
            schema=arrow_schema,
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
    assert len(data_files) == 2

    data_files_by_source = {}
    for data_file in data_files:
        parquet_path = data_file.file_path.removeprefix("file://")
        sources = pq.read_table(parquet_path, columns=["source"])["source"].unique().to_pylist()
        assert len(sources) == 1
        data_files_by_source[sources[0]] = data_file
    assert set(data_files_by_source) == {"first", "second"}

    table_root = Path(table.location().removeprefix("file://"))
    puffin_path = table_root / "data" / "multiple-deletion-vectors.puffin"
    puffin_location = f"file://{puffin_path}"
    blobs = [deletion_vector_blob(1), deletion_vector_blob(0, 2)]
    blob_locations = write_puffin(
        puffin_path,
        [
            (data_files_by_source["first"].file_path, blobs[0], 1),
            (data_files_by_source["second"].file_path, blobs[1], 2),
        ],
    )

    sequence_number = base_snapshot.sequence_number + 1
    delete_manifest_path = table_root / "metadata" / "multiple-deletion-vectors-m0.avro"
    with DeleteManifestWriterV3(
        spec=PartitionSpec(spec_id=0),
        schema=table.schema(),
        output_file=table.io.new_output(f"file://{delete_manifest_path}"),
        snapshot_id=SNAPSHOT_ID,
        avro_compression="gzip",
    ) as writer:
        vector_specs = [
            (data_files_by_source["first"], blob_locations[0], 1),
            (data_files_by_source["second"], blob_locations[1], 2),
        ]
        for data_file, (offset, length), cardinality in vector_specs:
            writer.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=SNAPSHOT_ID,
                    sequence_number=sequence_number,
                    file_sequence_number=sequence_number,
                    data_file=DataFile.from_args(
                        _table_format_version=3,
                        content=DataFileContent.POSITION_DELETES,
                        file_path=puffin_location,
                        file_format="PUFFIN",
                        partition=Record(),
                        record_count=cardinality,
                        file_size_in_bytes=puffin_path.stat().st_size,
                        equality_ids=None,
                        sort_order_id=None,
                        referenced_data_file=data_file.file_path,
                        content_offset=offset,
                        content_size_in_bytes=length,
                        spec_id=0,
                    ),
                )
            )
    delete_manifest = writer.to_manifest_file()

    manifest_list_path = table_root / "metadata" / "snap-multiple-deletion-vectors.avro"
    with ManifestListWriterV3(
        output_file=table.io.new_output(f"file://{manifest_list_path}"),
        snapshot_id=SNAPSHOT_ID,
        parent_snapshot_id=base_snapshot.snapshot_id,
        sequence_number=sequence_number,
        compression="gzip",
    ) as manifest_list_writer:
        manifest_list_writer.add_manifests([*data_manifests, delete_manifest])

    old_metadata = json.loads(Path(table.metadata_location.removeprefix("file://")).read_text())
    timestamp_ms = base_snapshot.timestamp_ms + 1
    old_metadata["format-version"] = 3
    old_metadata["next-row-id"] = 6
    old_metadata["snapshots"].append(
        {
            "sequence-number": sequence_number,
            "snapshot-id": SNAPSHOT_ID,
            "parent-snapshot-id": base_snapshot.snapshot_id,
            "timestamp-ms": timestamp_ms,
            "summary": {
                "operation": "delete",
                "added-delete-files": "2",
                "added-position-deletes": "3",
                "total-delete-files": "2",
                "total-position-deletes": "3",
                "total-data-files": "2",
                "total-records": "6",
            },
            "manifest-list": f"file://{manifest_list_path}",
            "schema-id": table.schema().schema_id,
        }
    )
    old_metadata["current-snapshot-id"] = SNAPSHOT_ID
    old_metadata["last-sequence-number"] = sequence_number
    old_metadata["last-updated-ms"] = timestamp_ms
    old_metadata["refs"] = {"main": {"snapshot-id": SNAPSHOT_ID, "type": "branch"}}
    old_metadata["snapshot-log"].append({"snapshot-id": SNAPSHOT_ID, "timestamp-ms": timestamp_ms})

    final_metadata_path = table_root / "metadata" / "00003-multi-blob.metadata.json"
    final_metadata_path.write_text(json.dumps(old_metadata, indent=2))
    (table_root / "metadata" / "version-hint.text").write_text("00003-multi-blob")
    return final_metadata_path


if __name__ == "__main__":
    print(build_table())
