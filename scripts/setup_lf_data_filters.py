#!/usr/bin/env python3
"""
Provision Lake Formation data filters and grants for duckdb-iceberg cloud tests.

Requires AWS credentials with permissions to manage Lake Formation data filters and grants.
"""

import argparse
import os
import sys

import boto3

REGION = os.getenv(
    "ICEBERG_LF_REGION",
    os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
)
ACCOUNT_ID = os.getenv("ICEBERG_LF_ACCOUNT_ID", "840140254803")
CATALOG_ID = os.getenv("ICEBERG_LF_CATALOG_ID", f"{ACCOUNT_ID}:s3tablescatalog/duckdblabs-iceberg-testing")
DATABASE = os.getenv("ICEBERG_LF_DATABASE", "test_inserts")
UNPARTITIONED_TABLE = os.getenv("ICEBERG_LF_UNPARTITIONED_TABLE", "basic_insert_test")
PARTITIONED_TABLE = os.getenv("ICEBERG_LF_PARTITIONED_TABLE", "basic_insert_test_partitioned")
SESSION_TAG = os.getenv("ICEBERG_LF_SESSION_TAG", "duckdb")
ROLE_ARN = os.getenv(
    "ICEBERG_LF_TEST_ROLE_ARN",
    f"arn:aws:iam::{ACCOUNT_ID}:role/pyiceberg-etl-role",
)


def get_clients():
    session = boto3.Session(region_name=REGION)
    return session.client("lakeformation"), session.client("glue")


def enable_application_integration(lf_client):
    settings = lf_client.get_data_lake_settings()["DataLakeSettings"]
    settings["AllowExternalDataFiltering"] = True
    settings["AuthorizedSessionTagValueList"] = list(
        set(settings.get("AuthorizedSessionTagValueList", []) + [SESSION_TAG])
    )
    allowlist = settings.get("ExternalDataFilteringAllowList", [])
    account_principal = {"DataLakePrincipalIdentifier": ACCOUNT_ID}
    if account_principal not in allowlist:
        allowlist.append(account_principal)
    settings["ExternalDataFilteringAllowList"] = allowlist
    lf_client.put_data_lake_settings(DataLakeSettings=settings)


def create_row_filter(lf_client, name, database, table, expression, column_names=None):
    row_filter = {"FilterExpression": expression}
    if expression.lower() in ("all", "allrows", "all_rows_wildcard"):
        row_filter = {"AllRowsWildcard": {}}
    table_data = {
        "TableCatalogId": CATALOG_ID,
        "DatabaseName": database,
        "TableName": table,
        "Name": name,
        "RowFilter": row_filter,
    }
    if column_names is not None:
        table_data["ColumnNames"] = column_names
    else:
        table_data["ColumnWildcard"] = {}
    lf_client.create_data_cells_filter(TableData=table_data)


def grant_filtered_select(lf_client, database, table, filter_name):
    lf_client.grant_permissions(
        Principal={"DataLakePrincipalIdentifier": ROLE_ARN},
        Resource={
            "DataCellsFilter": {
                "TableCatalogId": CATALOG_ID,
                "DatabaseName": database,
                "TableName": table,
                "Name": filter_name,
            }
        },
        Permissions=["SELECT"],
    )


def delete_filter_if_exists(lf_client, database, table, filter_name):
    try:
        lf_client.delete_data_cells_filter(
            TableCatalogId=CATALOG_ID,
            DatabaseName=database,
            TableName=table,
            Name=filter_name,
        )
    except lf_client.exceptions.EntityNotFoundException:
        pass


def main():
    parser = argparse.ArgumentParser(description="Setup Lake Formation data filters for cloud tests")
    parser.add_argument("--action", choices=["create", "delete"], default="create")
    args = parser.parse_args()

    lf_client, _glue_client = get_clients()

    filters = {
        "duckdb_lf_unpartitioned": (DATABASE, UNPARTITIONED_TABLE, "id < 3", None),
        "duckdb_lf_partitioned": (DATABASE, PARTITIONED_TABLE, "id < 3", None),
        "duckdb_lf_column_restricted": (DATABASE, UNPARTITIONED_TABLE, "all_rows_wildcard", ["id"]),
    }

    if args.action == "delete":
        for filter_name, (database, table, _, __) in filters.items():
            delete_filter_if_exists(lf_client, database, table, filter_name)
        print("Deleted Lake Formation test filters")
        return

    enable_application_integration(lf_client)
    for filter_name, (database, table, expression, column_names) in filters.items():
        delete_filter_if_exists(lf_client, database, table, filter_name)
        create_row_filter(lf_client, filter_name, database, table, expression, column_names)
        grant_filtered_select(lf_client, database, table, filter_name)
        print(f"Created filter {filter_name} on {database}.{table}: {expression}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"setup_lf_data_filters failed: {exc}", file=sys.stderr)
        sys.exit(1)
