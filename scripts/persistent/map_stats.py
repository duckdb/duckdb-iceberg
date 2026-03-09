from datetime import UTC, datetime
import os

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.transforms import DayTransform
from pyiceberg.types import MapType, NestedField, StringType, TimestamptzType

# Persistent warehouse location
warehouse_path = "data/persistent/map_stats"
os.makedirs(warehouse_path, exist_ok=True)

catalog = load_catalog(
    "default",
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

# Create namespace if needed
try:
    catalog.create_namespace("default")
except Exception:
    pass

schema = Schema(
    NestedField(field_id=1, name="timestamp", field_type=TimestamptzType()),
    NestedField(field_id=2, name="input", field_type=StringType()),
    NestedField(field_id=3, name="output", field_type=StringType()),
    NestedField(
        field_id=4,
        name="metadata",
        field_type=MapType(
            key_id=5,
            key_type=StringType(),
            value_id=6,
            value_type=StringType(),
            value_required=False,
        ),
        required=False,
    ),
)

schema = assign_fresh_schema_ids(schema)

table = catalog.create_table(
    "default.map_stats",
    schema,
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=schema.find_field("timestamp").field_id,
            transform=DayTransform(),
            field_id=1000,
            name="_day",
        )
    ),
)

# First append
table.append(
    pa.Table.from_pylist(
        [
            {
                "timestamp": datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
                "input": "hello",
                "output": "world",
                "metadata": {"key": "value1"},
            },
        ],
        schema=schema.as_arrow(),
    )
)

# Second append
table.append(
    pa.Table.from_pylist(
        [
            {
                "timestamp": datetime(2026, 1, 2, 0, 0, 0, tzinfo=UTC),
                "input": "hello",
                "output": "world",
                "metadata": {"key": "value1"},
            },
        ],
        schema=schema.as_arrow(),
    )
)
