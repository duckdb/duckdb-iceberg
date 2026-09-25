#!/usr/bin/env python3

import shutil
from pathlib import Path

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

WAREHOUSE = Path("data/persistent/missing_map_keys")
shutil.rmtree(WAREHOUSE, ignore_errors=True)
WAREHOUSE.mkdir(parents=True)
catalog = SqlCatalog("missing_map_keys", uri="sqlite:///:memory:", warehouse=WAREHOUSE.as_posix())
catalog.create_namespace("default")


def generate_table(name, payload_type, payload):
    path = WAREHOUSE / name
    (path / "data").mkdir(parents=True)
    schema = pa.schema([("id", pa.int64()), ("payload", payload_type)])
    table = catalog.create_table(f"default.{name}", schema=schema, location=path.as_posix())
    schema = schema_to_pyarrow(table.schema())
    data = pa.table({"id": [1], "payload": [payload]}, schema=schema)
    pq.write_table(data, path / "data" / "data.parquet")
    table.add_files([(path / "data" / "data.parquet").as_posix()])
    shutil.copyfile(Path(table.metadata_location.removeprefix("file://")), path / "metadata" / "v1.metadata.json")
    return path, data


def change_map_id(field, component):
    typ = field.type
    if pa.types.is_map(typ):
        key, value = typ.key_field, typ.item_field
        if component == "key":
            key = key.with_metadata({b"PARQUET:field_id": b"999"})
        else:
            value = value.with_metadata({b"PARQUET:field_id": b"999"})
        return field.with_type(pa.map_(key, value))
    if pa.types.is_struct(typ):
        return field.with_type(pa.struct([change_map_id(child, component) for child in typ]))
    if pa.types.is_list(typ):
        return field.with_type(pa.list_(change_map_id(typ.value_field, component)))
    if pa.types.is_large_list(typ):
        return field.with_type(pa.large_list(change_map_id(typ.value_field, component)))
    return field


map_type = pa.map_(pa.string(), pa.string())
map_value = [("key", "value")]
for name, typ, value in [
    ("map", map_type, map_value),
    ("struct", pa.struct([("child", map_type)]), {"child": map_value}),
    ("list", pa.list_(map_type), [map_value]),
]:
    path, data = generate_table(name, typ, value)
    schema = pa.schema([change_map_id(field, "key") for field in data.schema])
    pq.write_table(pa.Table.from_arrays(data.columns, schema=schema), path / "data" / "data.parquet")

path, data = generate_table("missing_value", map_type, map_value)
schema = pa.schema([change_map_id(field, "value") for field in data.schema])
pq.write_table(pa.Table.from_arrays(data.columns, schema=schema), path / "data" / "data.parquet")

path, data = generate_table("missing_map", map_type, map_value)
pq.write_table(data.select(["id"]), path / "data" / "data.parquet")

for stat in ["column_sizes", "value_counts", "null_value_counts", "nan_value_counts", "lower_bounds", "upper_bounds"]:
    path, _ = generate_table(f"manifest_{stat}", map_type, map_value)
    manifest = next((path / "metadata").glob("*-m0.avro"))
    with manifest.open("rb") as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
        metadata = dict(reader.metadata)
        records = list(reader)
    data_file = next(field for field in schema["fields"] if field["name"] == "data_file")["type"]
    stat_field = next(field for field in data_file["fields"] if field["name"] == stat)
    stat_field["type"][1]["items"]["fields"][0]["field-id"] = 999
    with manifest.open("wb") as f:
        fastavro.writer(f, schema, records, metadata=metadata)
