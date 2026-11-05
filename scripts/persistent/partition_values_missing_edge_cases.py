"""Generate identity-partition projection cases. Run from the repository root.

Expected reads follow https://iceberg.apache.org/spec/#column-projection. PyIceberg
0.11.1 also loses NULL constants and constants below absent structs, so it is used
as a fixture writer rather than as the oracle for these cases.
"""

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType, StructType

from partition_values_missing_from_files import field
from row_lineage_stored_columns import ManifestListWriterV3, with_first_row_id


BASE_DIR = Path("data/persistent/partition_values_missing_edge_cases")


def generate():
    shutil.rmtree(BASE_DIR, ignore_errors=True)
    catalog = load_catalog("default", uri="sqlite:///:memory:", warehouse=str(BASE_DIR))
    catalog.create_namespace("default")
    table = catalog.create_table(
        "default.mixed",
        schema=Schema(
            NestedField(1, "id", IntegerType()),
            NestedField(2, "region", StringType()),
            NestedField(
                3,
                "address",
                StructType(
                    NestedField(5, "country", StringType()),
                    NestedField(6, "city", StringType()),
                    NestedField(
                        7,
                        "geo",
                        StructType(
                            NestedField(8, "code", IntegerType()),
                            NestedField(9, "label", StringType()),
                        ),
                    ),
                ),
            ),
            NestedField(4, "extra", StringType()),
        ),
        partition_spec=PartitionSpec(
            PartitionField(2, 1000, IdentityTransform(), "region"),
            PartitionField(2, 1001, BucketTransform(16), "region_bucket"),
            PartitionField(5, 1002, BucketTransform(16), "country_bucket"),
            PartitionField(5, 1003, IdentityTransform(), "country"),
            PartitionField(8, 1004, IdentityTransform(), "code"),
        ),
        properties={"format-version": "2"},
    )

    geo = pa.struct([field("label", pa.string(), 9)])
    address = pa.struct([field("city", pa.string(), 6), field("geo", geo, 7)])
    full_geo = pa.struct([field("code", pa.int32(), 8), field("label", pa.string(), 9)])
    full_address = pa.struct(
        [field("country", pa.string(), 5), field("city", pa.string(), 6), field("geo", full_geo, 7)]
    )
    ids = field("id", pa.int32(), 1)
    missing = pa.schema([ids, field("address", address, 3)])
    stored = pa.schema([ids, field("region", pa.string(), 2), field("address", full_address, 3)])
    bucket = BucketTransform(16).transform(StringType())

    def data_file(name, rows, arrow_schema, partition):
        path = Path(table.location()) / "data" / f"{name}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        data = pa.Table.from_pylist(rows, schema=arrow_schema)
        pq.write_table(data, path)
        return DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=str(path),
            file_format=FileFormat.PARQUET,
            partition=Record(*partition),
            record_count=data.num_rows,
            file_size_in_bytes=path.stat().st_size,
            spec_id=table.spec().spec_id,
        )

    def partition(region, country, code):
        return [region, bucket(region), bucket(country), country, code]

    files = [
        data_file(
            "missing-fr",
            [
                {"id": 1, "address": {"city": "paris", "geo": {"label": "a"}}},
                {"id": 2, "address": {"city": "lyon", "geo": {"label": "b"}}},
            ],
            missing,
            partition("eu", "fr", 42),
        ),
        data_file(
            "missing-us",
            [{"id": 3, "address": {"city": "ny", "geo": {"label": "c"}}}],
            missing,
            partition("us", "us", 7),
        ),
        # NULL partition values must override non-NULL initial defaults.
        data_file(
            "missing-null",
            [{"id": 4, "address": {"city": "null-city", "geo": {"label": "null-code"}}}],
            missing,
            partition(None, None, None),
        ),
        data_file(
            "stored",
            [
                {
                    "id": 5,
                    "region": "ap",
                    "address": {"country": "jp", "city": "tokyo", "geo": {"code": 9, "label": "d"}},
                }
            ],
            stored,
            partition("ap", "jp", 9),
        ),
        # An explicitly NULL struct in the file must stay NULL.
        data_file("stored-null", [{"id": 6, "region": None, "address": None}], stored, partition(None, None, None)),
        data_file("missing-struct", [{"id": 7}], pa.schema([ids]), partition("eu", "fr", 84)),
        data_file(
            "missing-inner-struct",
            [{"id": 8, "address": {"city": "rome"}}],
            pa.schema([ids, field("address", pa.struct([field("city", pa.string(), 6)]), 3)]),
            partition("eu", "it", 12),
        ),
        # Migrated files can use name mapping rather than Parquet field IDs.
        data_file(
            "name-mapped",
            [{"legacy_id": 10, "legacy_region": "de", "legacy_address": {"city": "berlin", "geo": {"label": "e"}}}],
            pa.schema(
                [
                    pa.field("legacy_id", pa.int32()),
                    pa.field("legacy_region", pa.string()),
                    pa.field(
                        "legacy_address",
                        pa.struct(
                            [
                                pa.field("city", pa.string()),
                                pa.field("geo", pa.struct([pa.field("label", pa.string())])),
                            ]
                        ),
                    ),
                ]
            ),
            partition("de", "de", 16),
        ),
        # NULL partition values alone do not establish that an absent optional struct exists.
        data_file("missing-null-struct", [{"id": 11}], pa.schema([ids]), partition(None, None, None)),
    ]
    with table.transaction() as transaction:
        with transaction.update_snapshot().fast_append() as append:
            for file in files:
                append.append_data_file(file)

    # The same source IDs remain in the plan's partition_constants type, but these files have no identity fields.
    with table.update_spec() as update:
        for name in ("region", "country", "code"):
            update.remove_field(name)
    evolved = data_file(
        "without-identity",
        [{"id": 9, "address": {"city": "default-city", "geo": {"label": "default-code"}}}],
        missing,
        [bucket("unknown-region"), bucket("unknown-country")],
    )
    with table.transaction() as transaction:
        with transaction.update_snapshot().fast_append() as append:
            append.append_data_file(evolved)

    # Assemble the final v3 fixture using PyIceberg's v2 writer primitives.
    metadata = json.loads(Path(table.metadata_location).read_text())
    # PyIceberg cannot write v3 yet. Supply v3 manifest lists and row IDs so the final table supports defaults.
    metadata["format-version"] = 3
    first_row_ids = {}
    next_row_id = 0
    v2_manifest_lists = []
    for snapshot, py_snapshot in zip(metadata["snapshots"], table.metadata.snapshots):
        v2_manifest_lists.append(Path(snapshot["manifest-list"]))
        snapshot["first-row-id"] = next_row_id
        manifests = []
        for manifest in py_snapshot.manifests(table.io):
            if manifest.manifest_path not in first_row_ids:
                first_row_ids[manifest.manifest_path] = next_row_id
                next_row_id += manifest.added_rows_count
            manifests.append(with_first_row_id(manifest, first_row_ids[manifest.manifest_path]))
        snapshot["added-rows"] = next_row_id - snapshot["first-row-id"]
        path = Path(table.location()) / "metadata" / f"v3-snap-{snapshot['sequence-number']}.avro"
        with ManifestListWriterV3(
            table.io.new_output(str(path)),
            snapshot["snapshot-id"],
            snapshot.get("parent-snapshot-id"),
            snapshot["sequence-number"],
            snapshot["first-row-id"],
            "gzip",
        ) as writer:
            writer.add_manifests(manifests)
        snapshot["manifest-list"] = str(path)
    metadata["next-row-id"] = next_row_id
    defaults = {
        2: "unknown-region",
        4: "unpartitioned-default",
        5: "unknown-country",
        6: "unknown-city",
        8: -1,
        9: "unknown-label",
    }

    def add_defaults(fields):
        for field in fields:
            if field["id"] in defaults:
                field["initial-default"] = defaults[field["id"]]
            if isinstance(field["type"], dict):
                add_defaults(field["type"]["fields"])

    add_defaults(metadata["schemas"][0]["fields"])
    aliases = {1: "legacy_id", 2: "legacy_region", 3: "legacy_address"}

    def name_mapping(fields):
        result = []
        for field in fields:
            mapping = {"field-id": field["id"], "names": [field["name"]]}
            if field["id"] in aliases:
                mapping["names"].append(aliases[field["id"]])
            if isinstance(field["type"], dict):
                mapping["fields"] = name_mapping(field["type"]["fields"])
            result.append(mapping)
        return result

    metadata["properties"]["schema.name-mapping.default"] = json.dumps(name_mapping(metadata["schemas"][0]["fields"]))
    # Intermediate v2 metadata is only a generation aid. Keep the final fixture and its referenced files.
    metadata.pop("metadata-log", None)
    for path in (Path(table.location()) / "metadata").glob("*.metadata.json"):
        path.unlink()
    for path in v2_manifest_lists:
        path.unlink()
    # Stable entry point for SQL tests; all referenced files retain their original relative paths.
    (Path(table.location()) / "metadata" / "final.metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    generate()
