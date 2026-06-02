from __future__ import annotations

import json
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType

DIRECTORY_PATH = Path("/tmp/")

class DeleteManifestWriterV2(ManifestWriterV2):
    """pyiceberg 0.10 has no public delete-manifest writer; flip content=DELETES."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


def build_iceberg_table_with_eq_delete() -> Path:
    warehouse = DIRECTORY_PATH / "warehouse"
    warehouse.mkdir(parents=True)
    catalog = SqlCatalog("r", uri=f"sqlite:///{DIRECTORY_PATH}/c.db", warehouse=f"file://{warehouse}")
    catalog.create_namespace("ns")
    schema = Schema(
        NestedField(1, "col1", LongType(), required=False),
        NestedField(2, "col2", StringType(), required=False),
        NestedField(3, "col3", LongType(), required=False),
        NestedField(4, "col4", LongType(), required=False),
        NestedField(5, "col5", LongType(), required=False),
        NestedField(6, "col6", LongType(), required=False),
        NestedField(7, "col7", LongType(), required=False),
        NestedField(8, "col8", LongType(), required=False),
        NestedField(9, "col9", LongType(), required=False),
        NestedField(10, "col10", LongType(), required=False),
        NestedField(11, "col11", StringType(), required=False),
        NestedField(12, "id", LongType(), required=True),
        identifier_field_ids=[12],
    )
    tbl = catalog.create_table("ns.t", schema=schema, properties={"format-version": "2"})
    tbl.append(
        pa.Table.from_pylist(
            [{"col1": i + 100,
              "col2": chr(ord("a") + i - 1),
              "col3": i + 200,
              "col4": i + 300,
              "col5": i + 400,
              "col6": i + 500,
              "col7": i + 600,
              "col8": i + 700,
              "col9": i + 800,
              "col10": i + 900,
              "col11": chr(ord("a") + i - 1),
              "id": i} for i in range(1, 5)],
            schema=pa.schema(
                [
                    pa.field("col1", pa.int64(), nullable=True),
                    pa.field("col2", pa.string(), nullable=True),
                    pa.field("col3", pa.int64(), nullable=True),
                    pa.field("col4", pa.int64(), nullable=True),
                    pa.field("col5", pa.int64(), nullable=True),
                    pa.field("col6", pa.int64(), nullable=True),
                    pa.field("col7", pa.int64(), nullable=True),
                    pa.field("col8", pa.int64(), nullable=True),
                    pa.field("col9", pa.int64(), nullable=True),
                    pa.field("col10", pa.int64(), nullable=True),
                    pa.field("col11", pa.string(), nullable=True),
                    pa.field("id", pa.int64(), nullable=False),
                ]
            ),
        )
    )
    tbl.refresh()
    base_snap = tbl.current_snapshot()
    assert base_snap is not None
    (data_manifest,) = base_snap.manifests(io=tbl.io)
    table_root = Path(tbl.location().removeprefix("file://"))

    # Equality-delete parquet. PARQUET:field_id MUST match schema field id.
    delete_path = table_root / "data" / f"delete-{uuid.uuid4()}.parquet"
    delete_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [{"id": 2}],
            schema=pa.schema(
                [pa.field("id", pa.int64(), nullable=False, metadata={b"PARQUET:field_id": b"12"})]
            ),
        ),
        delete_path,
        compression="zstd",
    )
    delete_size = delete_path.stat().st_size

    delete_file = DataFile.from_args(
        content=DataFileContent.EQUALITY_DELETES,
        file_path=f"file://{delete_path}",
        file_format="PARQUET",
        partition=Record(),
        record_count=1,
        file_size_in_bytes=delete_size,
        equality_ids=[12],
        spec_id=0,
        sort_order_id=None,
    )

    new_snap_id = 9876543210123456789
    seq = base_snap.sequence_number + 1
    delete_manifest_path = table_root / "metadata" / f"{uuid.uuid4()}-m0.avro"
    with DeleteManifestWriterV2(
            spec=PartitionSpec(spec_id=0),
            schema=tbl.schema(),
            output_file=tbl.io.new_output(f"file://{delete_manifest_path}"),
            snapshot_id=new_snap_id,
            avro_compression="gzip",
    ) as w:
        w.add(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=new_snap_id,
                sequence_number=seq,
                file_sequence_number=seq,
                data_file=delete_file,
            )
        )
    delete_manifest = w.to_manifest_file()

    mlist_path = table_root / "metadata" / f"snap-{new_snap_id}-1-{uuid.uuid4()}.avro"
    with write_manifest_list(
            format_version=2,
            output_file=tbl.io.new_output(f"file://{mlist_path}"),
            snapshot_id=new_snap_id,
            parent_snapshot_id=base_snap.snapshot_id,
            sequence_number=seq,
            avro_compression="gzip",
    ) as mlw:
        mlw.add_manifests([data_manifest, delete_manifest])

    old_meta = json.loads(Path(tbl.metadata_location.removeprefix("file://")).read_text())
    ts = base_snap.timestamp_ms + 1000
    new_meta = {
        **old_meta,
        "snapshots": list(old_meta.get("snapshots", []))
                     + [
                         {
                             "sequence-number": seq,
                             "snapshot-id": new_snap_id,
                             "parent-snapshot-id": base_snap.snapshot_id,
                             "timestamp-ms": ts,
                             "summary": {"operation": "overwrite", "added-delete-files": "1"},
                             "manifest-list": f"file://{mlist_path}",
                             "schema-id": 0,
                         }
                     ],
        "current-snapshot-id": new_snap_id,
        "last-sequence-number": seq,
        "last-updated-ms": ts,
        "refs": {**old_meta.get("refs", {}), "main": {"snapshot-id": new_snap_id, "type": "branch"}},
    }
    new_meta_path = table_root / "metadata" / "v2.metadata.json"
    new_meta_path.write_text(json.dumps(new_meta, indent=2))
    return new_meta_path

if __name__ == "__main__":
    build_iceberg_table_with_eq_delete()

