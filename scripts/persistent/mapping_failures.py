#!/usr/bin/env python3

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

WAREHOUSE = Path("data/persistent/mapping_failures")
shutil.rmtree(WAREHOUSE, ignore_errors=True)
WAREHOUSE.mkdir(parents=True)

catalog = SqlCatalog("mapping_failures", uri="sqlite:///:memory:", warehouse=WAREHOUSE.as_posix())
catalog.create_namespace("default")

TYPES = {
    "struct": (pa.struct([("value", pa.string())]), {"value": "profile"}),
    "list": (pa.list_(pa.string()), ["profile"]),
    "map": (pa.map_(pa.string(), pa.string()), [("key", "profile")]),
}


def generate_table(name, expected_type, expected_value, physical_type, physical_value):
    table_path = WAREHOUSE / name
    (table_path / "data").mkdir(parents=True)
    schema = pa.schema([("id", pa.int64()), ("payload", expected_type)])
    table = catalog.create_table(f"default.{name}", schema=schema, location=table_path.as_posix())
    valid_path = table_path / "data" / "valid.parquet"
    invalid_path = table_path / "data" / "invalid.parquet"
    for path, row_id in [(valid_path, 1), (invalid_path, 2)]:
        pq.write_table(pa.table({"id": [row_id], "payload": [expected_value]}, schema=schema), path)
    table.add_files([valid_path.as_posix(), invalid_path.as_posix()])
    shutil.copyfile(
        Path(table.metadata_location.removeprefix("file://")),
        table_path / "metadata" / "v1.metadata.json",
    )
    physical_schema = pa.schema([("id", pa.int64()), ("payload", physical_type)])
    pq.write_table(pa.table({"id": [2], "payload": [physical_value]}, schema=physical_schema), invalid_path)


for source_name, (source_type, source_value) in TYPES.items():
    for target_name, (target_type, target_value) in TYPES.items():
        if source_name != target_name:
            generate_table(
                f"{source_name}_to_{target_name}", target_type, target_value, source_type, source_value
            )

source_type, source_value = TYPES["struct"]
target_type, target_value = TYPES["list"]
generate_table(
    "struct_child", pa.struct([("child", target_type)]), {"child": target_value},
    pa.struct([("child", source_type)]), {"child": source_value},
)
generate_table(
    "list_element", pa.list_(target_type), [target_value], pa.list_(source_type), [source_value],
)
generate_table(
    "map_value", pa.map_(pa.string(), target_type), [("key", target_value)],
    pa.map_(pa.string(), source_type), [("key", source_value)],
)
