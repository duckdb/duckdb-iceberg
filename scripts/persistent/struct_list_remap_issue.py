#!/usr/bin/env python3

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

WAREHOUSE = Path("data/persistent/issue1270/warehouse")
TABLE_PATH = WAREHOUSE / "default.db" / "my_table"
shutil.rmtree(WAREHOUSE, ignore_errors=True)
(TABLE_PATH / "data").mkdir(parents=True)

SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("metadata", pa.struct([
        ("uid", pa.string()),
        ("profiles", pa.list_(pa.string())),
    ])),
])
SCHEMA_UNANNOTATED = pa.schema([
    ("id", pa.int64()),
    ("metadata", pa.struct([
        ("uid", pa.string()),
        ("profiles", pa.struct([("list", pa.struct([("element", pa.string())]))])),
    ])),
])

p_list = TABLE_PATH / "data" / "a_annotated_list.parquet"
p_group = TABLE_PATH / "data" / "b_unannotated_group.parquet"
pq.write_table(pa.table({
    "id": [1, 2],
    "metadata": [
        {"uid": "row-1", "profiles": ["cloud_profile", "datetime_profile"]},
        {"uid": "row-2", "profiles": ["cloud_profile"]},
    ],
}, schema=SCHEMA), p_list)
pq.write_table(pa.table({
    "id": [3, 4],
    "metadata": [
        {"uid": "row-3", "profiles": ["cloud_profile"]},
        {"uid": "row-4", "profiles": ["datetime_profile"]},
    ],
}, schema=SCHEMA), p_group)

catalog = SqlCatalog("repro", uri="sqlite:///:memory:", warehouse=WAREHOUSE.as_posix())
catalog.create_namespace("default")
table = catalog.create_table("default.my_table", schema=SCHEMA, location=TABLE_PATH.as_posix())
table.add_files([p_list.as_posix(), p_group.as_posix()])
shutil.copyfile(
    Path(table.metadata_location.removeprefix("file://")),
    TABLE_PATH / "metadata" / "v1.metadata.json",
)

pq.write_table(pa.table({
    "id": [3, 4],
    "metadata": [
        {"uid": "row-3", "profiles": {"list": {"element": "cloud_profile"}}},
        {"uid": "row-4", "profiles": {"list": {"element": "datetime_profile"}}},
    ],
}, schema=SCHEMA_UNANNOTATED), p_group)
