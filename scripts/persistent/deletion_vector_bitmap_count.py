#!/usr/bin/env python3
"""Builds copies of the legacy_bare_deletion_vector table whose deletion vector declares a wrong bitmap count."""

import json
import struct
import zlib
from pathlib import Path

import fastavro
from pyroaring import BitMap

SOURCE_TABLE = Path("data/persistent/legacy_bare_deletion_vector/warehouse/default/legacy_bare_deletion_vector")
SOURCE_METADATA = SOURCE_TABLE / "metadata" / "00002-legacy-bare-dv.metadata.json"
SOURCE_DELETE_MANIFEST = SOURCE_TABLE / "metadata" / "legacy-bare-deletion-vector-m0.avro"
OUTPUT_ROOT = Path("data/persistent/deletion_vector_bitmap_count")
PUFFIN_MAGIC = b"PFA1"
DELETION_VECTOR_MAGIC = b"\xD1\xD3\x39\x64"

# name -> (declared bitmap count, number of bitmaps written)
CASES = {
    "valid": (1, 1),
    "count_huge": (1 << 40, 1),
    "count_negative": (-1, 1),
    "count_truncated": (2, 1),
}


def deletion_vector_blob(declared_count: int, bitmap_count: int) -> bytes:
    checksummed_data = DELETION_VECTOR_MAGIC + struct.pack("<q", declared_count)
    for key in range(bitmap_count):
        checksummed_data += struct.pack("<i", key) + BitMap([1]).serialize()
    checksum = struct.pack(">I", zlib.crc32(checksummed_data))
    return struct.pack(">I", len(checksummed_data)) + checksummed_data + checksum


def puffin_file(blob: bytes, snapshot_id: int, sequence_number: int, referenced_data_file: str) -> bytes:
    footer = {
        "blobs": [
            {
                "type": "deletion-vector-v1",
                "fields": [2147483645],
                "snapshot-id": snapshot_id,
                "sequence-number": sequence_number,
                "offset": len(PUFFIN_MAGIC),
                "length": len(blob),
                "properties": {"referenced-data-file": referenced_data_file, "cardinality": "1"},
            }
        ],
        "properties": {},
    }
    payload = json.dumps(footer).encode()
    return PUFFIN_MAGIC + blob + PUFFIN_MAGIC + payload + struct.pack("<i", len(payload)) + bytes(4) + PUFFIN_MAGIC


def rewrite_avro(source: Path, target: Path, transform) -> None:
    with source.open("rb") as f:
        reader = fastavro.reader(f)
        schema = json.loads(reader.metadata["avro.schema"])
        metadata = {k: v for k, v in reader.metadata.items() if not k.startswith("avro.")}
        records = [transform(record) for record in reader]
    with target.open("wb") as f:
        fastavro.writer(f, fastavro.parse_schema(schema), records, metadata=metadata, codec="deflate")


def main() -> None:
    table_metadata = json.loads(SOURCE_METADATA.read_text())
    snapshot = table_metadata["snapshots"][-1]
    source_manifest_list = Path(snapshot["manifest-list"])
    (OUTPUT_ROOT / "data").mkdir(parents=True, exist_ok=True)
    (OUTPUT_ROOT / "metadata").mkdir(parents=True, exist_ok=True)
    for name, (declared_count, bitmap_count) in CASES.items():
        with SOURCE_DELETE_MANIFEST.open("rb") as f:
            referenced_data_file = next(fastavro.reader(f))["data_file"]["referenced_data_file"]
        blob = deletion_vector_blob(declared_count, bitmap_count)
        puffin_path = OUTPUT_ROOT / "data" / f"{name}.puffin"
        puffin_bytes = puffin_file(blob, snapshot["snapshot-id"], snapshot["sequence-number"], referenced_data_file)
        puffin_path.write_bytes(puffin_bytes)

        def update_delete_entry(entry):
            entry["data_file"].update(
                file_path=str(puffin_path),
                file_size_in_bytes=len(puffin_bytes),
                content_offset=len(PUFFIN_MAGIC),
                content_size_in_bytes=len(blob),
            )
            return entry

        delete_manifest = OUTPUT_ROOT / "metadata" / f"{name}-m0.avro"
        rewrite_avro(SOURCE_DELETE_MANIFEST, delete_manifest, update_delete_entry)

        def update_manifest_file(manifest_file):
            if manifest_file["manifest_path"] == str(SOURCE_DELETE_MANIFEST):
                manifest_file["manifest_path"] = str(delete_manifest)
                manifest_file["manifest_length"] = delete_manifest.stat().st_size
            return manifest_file

        manifest_list = OUTPUT_ROOT / "metadata" / f"snap-{name}.avro"
        rewrite_avro(source_manifest_list, manifest_list, update_manifest_file)

        case_metadata = json.loads(json.dumps(table_metadata))
        case_metadata["snapshots"][-1]["manifest-list"] = str(manifest_list)
        case_metadata["metadata-log"] = []
        (OUTPUT_ROOT / "metadata" / f"{name}.metadata.json").write_text(json.dumps(case_metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
