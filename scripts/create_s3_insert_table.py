from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, LongType, NestedField, StringType
import os
import pyarrow as pa
import sys
import duckdb
from datetime import date


def catalog_region(catalog_env: str, default: str) -> str:
    # DuckDB Labs CI sets AWS_DEFAULT_REGION for credentials but upstream hardcoded
    # per-catalog regions. Only honor explicit ICEBERG_* overrides (or ICEBERG_AWS_REGION
    # for local dev where glue and s3tables share one region).
    return (
        os.getenv(catalog_env)
        or os.getenv("ICEBERG_AWS_REGION")
        or default
    )


# DuckDB Labs defaults (must match upstream and test/sql/cloud/* fixtures).
GLUE_REGION = catalog_region("ICEBERG_GLUE_REGION", "us-east-1")
S3TABLES_REGION = catalog_region("ICEBERG_S3TABLES_REGION", "us-east-2")
ACCOUNT_ID = os.getenv("ICEBERG_ACCOUNT_ID", os.getenv("ICEBERG_LF_ACCOUNT_ID", "840140254803"))
S3TABLES_BUCKET = os.getenv("ICEBERG_S3TABLES_BUCKET", "iceberg-testing")
GLUE_WAREHOUSE = os.getenv(
    "ICEBERG_GLUE_WAREHOUSE",
    os.getenv(
        "ICEBERG_LF_CATALOG_ID",
        f"{ACCOUNT_ID}:s3tablescatalog/duckdblabs-iceberg-testing",
    ),
)

DATABASE_NAME = os.getenv("ICEBERG_LF_DATABASE", "test_inserts")
TABLE_NAME = "basic_insert_test"
PARTITIONED_TABLE_NAME = os.getenv("ICEBERG_LF_PARTITIONED_TABLE", "basic_insert_test_partitioned")
NO_LF_GRANT_TABLE_NAME = os.getenv("ICEBERG_LF_NO_GRANT_TABLE", "no_lf_grant_test")


def get_glue_catalog():
    rest_catalog = load_catalog(
        "glue_catalog",
        **{
            "type": "rest",
            "warehouse": GLUE_WAREHOUSE,
            "uri": f"https://glue.{GLUE_REGION}.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "glue",
            "rest.signing-region": GLUE_REGION,
        },
    )
    return rest_catalog


def get_s3tables_catalog():
    rest_catalog = load_catalog(
        "s3tables_catalog",
        **{
            "type": "rest",
            "warehouse": f"arn:aws:s3tables:{S3TABLES_REGION}:{ACCOUNT_ID}:bucket/{S3TABLES_BUCKET}",
            "uri": f"https://s3tables.{S3TABLES_REGION}.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": S3TABLES_REGION,
        },
    )
    return rest_catalog


def get_r2_catalog():
    catalog = RestCatalog(
        name="r2_catalog",
        warehouse='6b17833f308abc1e1cc343c552b51f51_r2-catalog',
        uri='https://catalog.cloudflarestorage.com/6b17833f308abc1e1cc343c552b51f51/r2-catalog',
        token=os.getenv('R2_TOKEN'),
    )
    return catalog


def print_help():
    print(
        "default usage: python3 create_s3_insert_table.py --action=[delete-and-create|delete|create] --catalogs=s3tables,glue,r2"
    )


def main():

    action = ""
    catalogs = []

    for arg in sys.argv:
        if arg.startswith("--action="):
            action = arg.replace("--action=", "")
        if arg.startswith("--catalogs="):
            catalogs_str = arg.replace("--catalogs=", "")
            for cat in catalogs_str.split(","):
                if cat == "s3tables":
                    catalogs.append(get_s3tables_catalog())
                if cat == "glue":
                    catalogs.append(get_glue_catalog())
                if cat == "r2":
                    catalogs.append(get_r2_catalog())

    if len(catalogs) == 0 or action == "":
        print_help()
        exit(1)

    print(f"performing {action} for {str(len(catalogs))} catalogs")

    for catalog in catalogs:
        print(f"---- {catalog.name} ----")
        glue_catalog = catalog.name == "glue_catalog"
        if action == "delete-and-create":
            delete_table(catalog, TABLE_NAME)
            if glue_catalog:
                delete_table(catalog, PARTITIONED_TABLE_NAME)
                delete_table(catalog, NO_LF_GRANT_TABLE_NAME)
            create_table(catalog, TABLE_NAME)
            if glue_catalog:
                create_partitioned_table(catalog)
                create_table(catalog, NO_LF_GRANT_TABLE_NAME)
        elif action == "delete":
            delete_table(catalog, TABLE_NAME)
            if glue_catalog:
                delete_table(catalog, PARTITIONED_TABLE_NAME)
                delete_table(catalog, NO_LF_GRANT_TABLE_NAME)
        elif action == "create":
            create_table(catalog, TABLE_NAME)
            if glue_catalog:
                create_partitioned_table(catalog)
                create_table(catalog, NO_LF_GRANT_TABLE_NAME)
        else:
            print(f"did not recognize option {action}")
            exit(1)


def create_basic_iceberg_schema() -> Schema:
    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "address", StringType(), required=True),
        NestedField(4, "date", DateType(), required=True),
    )


def create_basic_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("address", pa.string(), nullable=False),
            pa.field("date", pa.date32(), nullable=False),
        ]
    )


def get_namespaces(rest_catalog):
    return rest_catalog.list_namespaces()


def get_tables(rest_catalog, namespace):
    return rest_catalog.list_tables(namespace)


def ensure_namespace(rest_catalog):
    namespaces = list(map(lambda t: t[0], get_namespaces(rest_catalog)))
    if DATABASE_NAME not in namespaces:
        print(f"schema {DATABASE_NAME} does not exist")
        print(f"known namespaces: " + str(get_namespaces(rest_catalog)))
        print(f"creating namespace {DATABASE_NAME}")
        rest_catalog.create_namespace(f"{DATABASE_NAME}")


def sample_data(table_schema):
    return pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            pa.array(["Smith", "Jones", "Brown"], type=pa.string()),
            pa.array(
                [
                    date(1990, 1, 1),
                    date(1985, 6, 15),
                    date(2000, 12, 31),
                ],
                type=pa.date32(),
            ),
        ],
        schema=table_schema,
    )


def delete_table(rest_catalog, table_name):
    ensure_namespace(rest_catalog)

    namespace = (DATABASE_NAME,)  # Tuple format
    tables = list(map(lambda t: t[1], get_tables(rest_catalog, namespace)))

    if table_name not in tables:
        print(f"table {table_name} does not exist in database {DATABASE_NAME}, skipping delete")
        return

    identifier = (f"{DATABASE_NAME}", f"{table_name}")
    rest_catalog.drop_table(identifier, purge_requested=True)
    print(f"{rest_catalog.name}: table {table_name} dropped succesfully")


def create_table(rest_catalog, table_name):
    ensure_namespace(rest_catalog)

    namespace = (DATABASE_NAME,)
    tables = list(map(lambda t: t[1], get_tables(rest_catalog, namespace)))
    if table_name in tables:
        print(f"{rest_catalog.name}: table {table_name} already exists in database {DATABASE_NAME}")
        return

    table_schema = create_basic_schema()
    rest_catalog.create_table(identifier=f"{DATABASE_NAME}.{table_name}", schema=table_schema)

    basic_table = rest_catalog.load_table(f"{DATABASE_NAME}.{table_name}")
    basic_table.append(sample_data(table_schema))
    print(f"{rest_catalog.name}: appended data to {table_name}")


def create_partitioned_table(rest_catalog):
    ensure_namespace(rest_catalog)

    namespace = (DATABASE_NAME,)
    tables = list(map(lambda t: t[1], get_tables(rest_catalog, namespace)))
    if PARTITIONED_TABLE_NAME in tables:
        print(
            f"{rest_catalog.name}: table {PARTITIONED_TABLE_NAME} already exists in database {DATABASE_NAME}"
        )
        return

    table_schema = create_basic_iceberg_schema()
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=table_schema.find_field("date").field_id,
            field_id=1000,
            transform=IdentityTransform(),
            name="date",
        )
    )
    rest_catalog.create_table(
        identifier=f"{DATABASE_NAME}.{PARTITIONED_TABLE_NAME}",
        schema=table_schema,
        partition_spec=partition_spec,
    )

    partitioned_table = rest_catalog.load_table(f"{DATABASE_NAME}.{PARTITIONED_TABLE_NAME}")
    partitioned_table.append(sample_data(create_basic_schema()))
    print(f"{rest_catalog.name}: appended data to {PARTITIONED_TABLE_NAME}")


if __name__ == "__main__":
    main()
