#!/usr/bin/env python3
"""
Reproduces https://github.com/duckdb/duckdb-iceberg/issues/782
IS NULL / IS NOT NULL predicates return 0 rows when manifest stats are NULL.
"""

import os
import shutil
import fastavro

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, BooleanType, TimestamptzType
import pyarrow as pa

# Persistent warehouse location
warehouse_path = "data/persistent/null_stats"
os.makedirs(warehouse_path, exist_ok=True)

catalog = SqlCatalog("local", **{
    "type": "sql",
    "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
    "warehouse": warehouse_path,
})
catalog.create_namespace_if_not_exists("default")

schema = Schema(
    NestedField(1, "id", IntegerType()),
    NestedField(2, "name", StringType()),
    NestedField(3, "ts", TimestamptzType()),
    NestedField(4, "flag", BooleanType()),
)

table = catalog.create_table_if_not_exists("default.test_nulls", schema=schema)
ts = pa.timestamp("us", tz="UTC")

# Batch 1: no NULLs in flag
table.append(pa.table({
    "id": pa.array([1, 2, 3], type=pa.int32()),
    "name": ["a", "b", "c"],
    "ts": pa.array([1709300000000000, 1709400000000000, 1709500000000000], type=ts),
    "flag": pa.array([True, False, True]),
}))

# Batch 2: mix of NULLs and non-NULLs in flag
table.append(pa.table({
    "id": pa.array([4, 5, 6], type=pa.int32()),
    "name": ["d", "e", "f"],
    "ts": pa.array([1709600000000000, 1709700000000000, 1709800000000000], type=ts),
    "flag": pa.array([None, None, True]),
}))

# Batch 3: all NULLs in flag
table.append(pa.table({
    "id": pa.array([7, 8, 9], type=pa.int32()),
    "name": ["g", "h", "i"],
    "ts": pa.array([1709900000000000, 1710000000000000, 1710100000000000], type=ts),
    "flag": pa.array([None, None, None], type=pa.bool_()),
}))

metadata_path = table.metadata_location

# ── Step 2: Patch manifests to NULL out stats (mimicking Fivetran) ───────────
# Fivetran's Iceberg writer leaves these optional fields as NULL.
# Per the Iceberg spec, NULL means "not available" and is valid.

metadata_dir = os.path.join(os.path.dirname(metadata_path), "")

patched_count = 0
for f in os.listdir(metadata_dir):
    if f.endswith(".avro") and not f.startswith("snap-"):
        path = os.path.join(metadata_dir, f)
        with open(path, "rb") as fh:
            reader = fastavro.reader(fh)
            avro_schema = reader.writer_schema
            metadata = dict(reader.metadata)
            records = list(reader)

        for rec in records:
            df = rec.get("data_file", rec)
            for field in ["null_value_counts", "nan_value_counts", "column_sizes", "value_counts", "split_offsets"]:
                if field in df:
                    df[field] = None
            if "sort_order_id" in df:
                df["sort_order_id"] = 0

        with open(path, "wb") as fh:
            fastavro.writer(fh, avro_schema, records, metadata=metadata, codec="deflate")
        patched_count += 1
