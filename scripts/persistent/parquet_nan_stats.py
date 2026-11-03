#!/usr/bin/env python3
"""
A FLOAT and a DOUBLE column holding NaN, written by pyiceberg (pyarrow) with two rows per row group.
Row group 0 holds 1.0 and NaN with min = max = 1.0 and no nan_count: the bounds do not cover the NaN.
"""

import os

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, FloatType, DoubleType
import pyarrow as pa

warehouse_path = "data/persistent/parquet_nan_stats"
os.makedirs(warehouse_path, exist_ok=True)

catalog = SqlCatalog("local", **{
    "type": "sql",
    "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
    "warehouse": warehouse_path,
})
catalog.create_namespace_if_not_exists("default")

schema = Schema(
    NestedField(1, "k", IntegerType()),
    NestedField(2, "d", DoubleType()),
    NestedField(3, "f", FloatType()),
)

table = catalog.create_table_if_not_exists(
    "default.nan_stats", schema=schema, properties={"write.parquet.row-group-limit": "2"}
)

nan = float("nan")
table.append(pa.table({
    "k": pa.array([1, 2, 3], type=pa.int32()),
    "d": pa.array([1.0, nan, 2.0], type=pa.float64()),
    "f": pa.array([1.0, nan, 2.0], type=pa.float32()),
}))
