from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimeType, LongType, StringType
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.metadata import TableMetadata
from datetime import time
import pyarrow as pa


# Utility to convert 'HH:MM:SS' into microseconds since midnight
def to_microseconds(t: time) -> int:
    return (t.hour * 3600 + t.minute * 60 + t.second) * 1_000_000


# Convert string times
times = ["12:34:56", "08:21:09"]
micros = [to_microseconds(time.fromisoformat(t)) for t in times]
user_ids = [12345, 67890]
event_types = ["click", "purchase"]

# Define schema with non-nullable fields
schema = pa.schema(
    [("partition_col", pa.time64("us"), False), ("user_id", pa.int64(), False), ("event_type", pa.string(), False)]
)

arrays = [
    pa.array(micros, type=pa.time64("us")),
    pa.array(user_ids, type=pa.int64()),
    pa.array(event_types, type=pa.string()),
]

# Build the RecordBatch using the schema
record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

# Convert to Table
table_data = pa.Table.from_batches([record_batch])

# Load SQLite-backed local catalog
catalog = RestCatalog(
    "my_datalake",
    **{
        "uri": "http://localhost:8181/catalog",
        "warehouse": "demo",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    },
)

# Create the namespace if it doesn't exist
try:
    catalog.create_namespace("my_namespace")
except Exception as e:
    print(f"Note: {e}")  # Print but continue if namespace already exists

schema = Schema(
    NestedField(1, "partition_col", TimeType(), required=True),
    NestedField(2, "user_id", LongType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
)

# Create table
table = catalog.create_table(
    identifier="my_namespace.my_table",
    schema=schema,
)

# Write records
table.append(table_data)
