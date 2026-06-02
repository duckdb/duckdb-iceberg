"""
Writes test_table_partitioned_by_int_format_version_2 via PyIceberg,
reproducing the data defined in test/sql/local/stupid.test:

    CREATE TABLE ... (id INT, val INT) PARTITIONED BY (val) WITH ('format-version' = 2)
    INSERT INTO ... VALUES (1, 2), (3, 4);
"""

import requests
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.table.sorting import SortOrder

CATALOG_HOST = "http://127.0.0.1:8181"
CLIENT_ID = "admin"
CLIENT_SECRET = "password"


def get_token() -> str:
    response = requests.post(
        f"{CATALOG_HOST}/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]


def main():
    token = get_token()

    catalog = RestCatalog(
        "rest",
        **{
            "uri": CATALOG_HOST,
            "token": token,
            "warehouse": "",
            "s3.endpoint": "http://127.0.0.1:9000",
            "s3.access-key-id": CLIENT_ID,
            "s3.secret-access-key": CLIENT_SECRET,
            "s3.path-style-access": "true",
            "s3.ssl.enabled": "false",
        },
    )

    namespace = "default"
    table_name = "py_iceberg_test_table_partitioned_by_int_format_version_2"
    full_name = f"{namespace}.{table_name}"

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="val", field_type=IntegerType(), required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            transform=IdentityTransform(),
            name="val_identity",
        ),
    )

    if catalog.table_exists(full_name):
        catalog.drop_table(full_name)

    table = catalog.create_table(
        full_name,
        schema=schema,
        partition_spec=partition_spec,
        sort_order=SortOrder(),
        properties={"format-version": "2"},
    )

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("val", pa.int32()),
        ]
    )

    data = pa.table(
        {
            "id": pa.array([1, 3], type=pa.int32()),
            "val": pa.array([2, 4], type=pa.int32()),
        },
        schema=arrow_schema,
    )

    table.append(data)
    print(f"Written {full_name}: {table.scan().to_arrow().to_pylist()}")


if __name__ == "__main__":
    main()
