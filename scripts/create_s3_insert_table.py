from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
import os
import pyarrow as pa
import sys
import duckdb
from datetime import date

REGION = "us-east-2"
DATABASE = "iceberg-testing"
database_name = "iceberg-testing"
TABLE_BUCKET = "iceberg-testing"


DATABASE_NAME = "test_inserts"
TABLE_NAME = "basic_insert_test"


def get_glue_catalog():
    rest_catalog = load_catalog(
        "glue_catalog",
        **{
            "type": "rest",
            "warehouse": "840140254803:s3tablescatalog/duckdblabs-iceberg-testing",
            "uri": f"https://glue.us-east-1.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "glue",
            "rest.signing-region": "us-east-1",
        },
    )
    return rest_catalog


def get_s3tables_catalog():
    rest_catalog = load_catalog(
        "s3tables_catalog",
        **{
            "type": "rest",
            "warehouse": "arn:aws:s3tables:us-east-2:840140254803:bucket/iceberg-testing",
            "uri": f"https://s3tables.{REGION}.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": REGION,
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
        if action == "delete-and-create":
            delete_table(catalog)
            create_table(catalog)
        elif action == "delete":
            delete_table(catalog)
        elif action == "create":
            create_table(catalog)
        else:
            print(f"did not recognize option {action}")
            exit(1)


def create_basic_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("address", pa.string()),
            pa.field("date", pa.date32()),
        ]
    )


def get_namespaces(rest_catalog):
    return rest_catalog.list_namespaces()


def get_tables(rest_catalog, namespace):
    return rest_catalog.list_tables(namespace)


def delete_table(rest_catalog):
    global DATABASE_NAME, TABLE_NAME
    namespaces = list(map(lambda t: t[0], get_namespaces(rest_catalog)))
    if DATABASE_NAME not in namespaces:
        print(f"schema {DATABASE_NAME} does not exist")
        print(f"known namespaces: " + str(get_namespaces(rest_catalog)))
        print(f"creating namespace {DATABASE_NAME}")
        rest_catalog.create_namespace(f"{DATABASE_NAME}")

    namespace = (DATABASE_NAME,)  # Tuple format
    # List tables in the namespace
    tables = list(map(lambda t: t[1], get_tables(rest_catalog, namespace)))

    if TABLE_NAME not in tables:
        print(f"table {TABLE_NAME} does not exist in database {DATABASE_NAME}, skipping delete")
        return

    identifier = (f"{DATABASE_NAME}", f"{TABLE_NAME}")
    # Drop the table
    rest_catalog.drop_table(identifier, purge_requested=True)
    print(f"{rest_catalog.name}: table {TABLE_NAME} dropped succesfully")


def create_table(rest_catalog):
    global DATABASE_NAME, TABLE_NAME
    namespaces = list(map(lambda t: t[0], get_namespaces(rest_catalog)))
    if DATABASE_NAME not in namespaces:
        print(f"schema {DATABASE_NAME} does not exist")
        print(f"known namespaces: " + str(get_namespaces(rest_catalog)))
        print(f"creating namespace {DATABASE_NAME}")

    namespace = (DATABASE_NAME,)  # Tuple format
    # List tables in the namespace
    tables = list(map(lambda t: t[1], get_tables(rest_catalog, namespace)))
    if TABLE_NAME in tables:
        print(f"{rest_catalog.name}: table {TABLE_NAME} already exists in database {DATABASE_NAME}")
        return

    table_schema = create_basic_schema()
    rest_catalog.create_table(identifier=f"{DATABASE_NAME}.{TABLE_NAME}", schema=table_schema)

    basic_table = rest_catalog.load_table(f"{DATABASE_NAME}.{TABLE_NAME}")
    data = [
        pa.array([1, 2, 3], type=pa.int32()),
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
    ]

    table_data = pa.Table.from_arrays(data, schema=table_schema)
    basic_table.append(table_data)
    print(f"{rest_catalog.name}: appended data to {TABLE_NAME}")


if __name__ == "__main__":
    main()
