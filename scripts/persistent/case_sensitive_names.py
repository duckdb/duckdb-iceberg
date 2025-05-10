from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, NestedField
import pyarrow as pa
import os

# Create a local warehouse directory
warehouse_path = "data/persistent/case_sensitive_names"
os.makedirs(warehouse_path, exist_ok=True)

# Load a local catalog using SQLite
catalog = load_catalog(
    "default",
    **{
        'type': 'sql',
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": warehouse_path,
    },
)

# Create namespace if it doesn't exist
try:
    catalog.create_namespace("default")
    print("Created namespace: default")
except Exception as e:
    print(f"Note about namespace: {e}")

# Verify the namespace exists
namespaces = catalog.list_namespaces()
print(f"Available namespaces: {namespaces}")

# Define the schema with case-sensitive field names
schema = Schema(
    NestedField(1, "user_id", LongType(), required=True), NestedField(2, "uSeR_Id", StringType(), required=False)
)

# Create the table with case sensitivity enabled
table = catalog.create_table(
    identifier="default.case_sensitive_names",
    schema=schema,
    properties={"format-version": "2", "write.update.mode": "merge-on-read"},
)

# Create PyArrow schema with correct nullability
pa_schema = pa.schema(
    [
        pa.field("user_id", pa.int64(), nullable=False),  # Set nullable=False to match required=True
        pa.field("uSeR_Id", pa.string(), nullable=True),
    ]
)

# Create some sample data with the explicit schema
data = pa.Table.from_pylist(
    [{"user_id": 1, "uSeR_Id": "user_1"}, {"user_id": 2, "uSeR_Id": "user_2"}, {"user_id": 3, "uSeR_Id": "user_3"}],
    schema=pa_schema,
)

# Append the data to the table
table.append(data)

# Verify the data
print(table.scan().to_arrow())
