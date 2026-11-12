#!/usr/bin/env python3
"""Builds copies of the partition_bool table whose manifest list names a partition spec that does not exist."""

import json
from pathlib import Path

import fastavro

SOURCE_ROOT = Path("data/persistent/partition_bool")
SOURCE_METADATA = SOURCE_ROOT / "metadata" / "v2.metadata.json"
OUTPUT_ROOT = Path("data/persistent/manifest_list_invalid_spec_id")

CASES = {
    "spec_id_999": {"partition_spec_id": 999},
    "spec_id_negative": {"partition_spec_id": -1},
}


def write_manifest_list(source: Path, target: Path, overrides: dict) -> None:
    with source.open("rb") as f:
        reader = fastavro.reader(f)
        schema = json.loads(reader.metadata["avro.schema"])
        metadata = {k: v for k, v in reader.metadata.items() if not k.startswith("avro.")}
        records = [{**record, **overrides} for record in reader]
    with target.open("wb") as f:
        fastavro.writer(f, fastavro.parse_schema(schema), records, metadata=metadata, codec="deflate")


def main() -> None:
    table_metadata = json.loads(SOURCE_METADATA.read_text())
    source_manifest_list = Path(table_metadata["snapshots"][0]["manifest-list"])
    (OUTPUT_ROOT / "metadata").mkdir(parents=True, exist_ok=True)
    for name, overrides in CASES.items():
        manifest_list = OUTPUT_ROOT / "metadata" / f"snap-{name}.avro"
        write_manifest_list(source_manifest_list, manifest_list, overrides)
        case_metadata = json.loads(json.dumps(table_metadata))
        case_metadata["location"] = str(OUTPUT_ROOT)
        case_metadata["snapshots"][0]["manifest-list"] = str(manifest_list)
        case_metadata["metadata-log"] = []
        (OUTPUT_ROOT / "metadata" / f"{name}.metadata.json").write_text(json.dumps(case_metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
