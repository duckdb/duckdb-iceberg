import os
import shutil
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType, StructType

# Tables whose data files omit an identity partition's source column, so readers must take its value from the
# manifest's partition data (spec: "Column Projection")
BASE_DIR = "data/persistent/partition_values_missing_from_files"


def field(name, arrow_type, field_id):
    return pa.field(name, arrow_type, metadata={b"PARQUET:field_id": str(field_id).encode()})


def add_data_file(table, data, partition):
    path = f"{table.location()}/data/{uuid.uuid4()}.parquet"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(data, path)
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record(*partition),
        record_count=data.num_rows,
        file_size_in_bytes=os.path.getsize(path),
    )
    data_file.spec_id = table.spec().spec_id
    with table.transaction() as transaction:
        with transaction.update_snapshot().fast_append() as append:
            append.append_data_file(data_file)


if __name__ == "__main__":
    shutil.rmtree(BASE_DIR, ignore_errors=True)
    os.makedirs(f"{BASE_DIR}/warehouse")
    catalog = load_catalog("default", uri=f"sqlite:///{BASE_DIR}/catalog.sqlite", warehouse=f"{BASE_DIR}/warehouse")
    catalog.create_namespace("default")

    ids_only = pa.table(
        {"id": pa.array([1, 2], pa.int32())},
        schema=pa.schema([field("id", pa.int32(), 1)]),
    )

    # Two partition fields share the source column: identity(region) and bucket(16, region)
    shared_source = catalog.create_table(
        "default.shared_source",
        schema=Schema(NestedField(1, "id", IntegerType()), NestedField(2, "region", StringType())),
        partition_spec=PartitionSpec(
            PartitionField(2, 1000, IdentityTransform(), "region"),
            PartitionField(2, 1001, BucketTransform(16), "region_bucket"),
        ),
    )
    add_data_file(shared_source, ids_only, ["eu", BucketTransform(16).transform(StringType())("eu")])

    # The partition source is a struct field, and the file's struct omits it
    nested_source = catalog.create_table(
        "default.nested_source",
        schema=Schema(
            NestedField(1, "id", IntegerType()),
            NestedField(
                2,
                "address",
                StructType(NestedField(3, "country", StringType()), NestedField(4, "city", StringType())),
            ),
        ),
        partition_spec=PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "country")),
    )
    city_only = pa.struct([field("city", pa.string(), 4)])
    add_data_file(
        nested_source,
        pa.table(
            {
                "id": pa.array([1, 2], pa.int32()),
                "address": pa.array([{"city": "paris"}, {"city": "lyon"}], city_only),
            },
            schema=pa.schema([field("id", pa.int32(), 1), field("address", city_only, 2)]),
        ),
        ["fr"],
    )
