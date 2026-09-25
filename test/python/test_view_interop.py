"""Views written by a real Spark Iceberg catalog and loaded by DuckDB."""

from uuid import uuid4
import time
import pytest

from duckdb_unittest import DuckDBUnittestRunner


def test_spark_view_dialect_rejected(spark_con, unittest_binary, unittest_test_config, print_unittest_stdin):
    name = "spark_view_" + uuid4().hex
    spark_con.sql(f"create view default.{name} (answer) as select 42 as original_name")
    try:
        assert spark_con.sql(f"select answer from default.{name}").first()[0] == 42
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            test.statement_error(
                f"select answer from my_datalake.default.{name}",
                "no SQL representation with dialect 'duckdb'",
            )
            test.query(
                "I",
                f"select count(*) from iceberg_view_metadata('my_datalake.default.{name}')",
                [(1,)],
            )
            test.statement_ok(f"drop view my_datalake.default.{name}")
            test.query(
                "I",
                f"select count(*) from duckdb_views() where database_name = 'my_datalake' and view_name = '{name}'",
                [(0,)],
            )
    finally:
        spark_con.sql(f"drop view if exists default.{name}")


def test_spark_view_unsupported_sql(spark_con, unittest_binary, unittest_test_config, print_unittest_stdin):
    name = "spark_view_'" + uuid4().hex
    escaped_name = name.replace("'", "''")
    spark_con.sql(f"create view default.`{name}` as select 42 as `spark column`")
    try:
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            test.statement_error(
                f'select * from my_datalake.default."{name}"',
                "no SQL representation with dialect 'duckdb'",
            )
            test.query(
                "I",
                f"select count(*) from duckdb_views() where view_name = '{escaped_name}' and not is_bound",
                [(1,)],
            )
            test.query(
                "I",
                f"select count(*) from iceberg_view_metadata('my_datalake.default.\"{escaped_name}\"')",
                [(1,)],
            )
            test.statement_ok(f'drop view my_datalake.default."{name}"')
    finally:
        spark_con.sql(f"drop view if exists default.`{name}`")


@pytest.mark.parametrize(
    "default_catalog,default_namespace,representations,error",
    [
        (None, ["default"], [("duckdb", "select 42 as answer")], None),
        ("my_datalake", ["default"], [("duckdb", "select 42 as answer")], None),
        (
            None,
            ["default"],
            [("spark", "select 99 as answer"), ("duckdb", "select 42 as answer")],
            None,
        ),
        (
            None,
            ["default"],
            [("spark", "select 42 as answer")],
            "no SQL representation with dialect 'duckdb'",
        ),
        (
            "other_catalog",
            ["default"],
            [("duckdb", "select 42 as answer")],
            "a different default catalog",
        ),
        (
            None,
            ["other_namespace"],
            [("duckdb", "select 42 as answer")],
            "a different default namespace",
        ),
        (
            None,
            [],
            [("duckdb", "select 42 as answer")],
            "a different default namespace",
        ),
        (
            None,
            ["default"],
            [("duckdb", "select 42 as `spark column`")],
            "cannot be parsed by DuckDB",
        ),
    ],
)
def test_view_representation_and_defaults(
    rest_catalog,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
    default_catalog,
    default_namespace,
    representations,
    error,
):
    # PyIceberg does not expose create_view; use its configured REST session so
    # the real catalog validates and persists the metadata for every profile.
    rest_catalog.create_namespace_if_not_exists("default")
    name = "view_defaults_" + uuid4().hex
    version = {
        "version-id": 1,
        "timestamp-ms": int(time.time() * 1000),
        "schema-id": 0,
        "summary": {},
        "default-namespace": default_namespace,
        "representations": [{"type": "sql", "dialect": dialect, "sql": sql} for dialect, sql in representations],
    }
    if default_catalog is not None:
        version["default-catalog"] = default_catalog
    response = rest_catalog._session.post(
        rest_catalog.url("namespaces/{namespace}/views", namespace="default"),
        json={
            "name": name,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [{"id": 1, "name": "answer", "required": False, "type": "int"}],
            },
            "view-version": version,
            "properties": {},
        },
    )
    response.raise_for_status()
    try:
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            query = f"select answer from my_datalake.default.{name}"
            if error:
                test.statement_error(query, error)
            else:
                test.query("I", query, [(42,)])
            test.query(
                "I",
                f"select count(*) from iceberg_view_metadata('my_datalake.default.{name}')",
                [(1,)],
            )
            test.query(
                "I",
                f"select count(*) from duckdb_views() where view_name = '{name}'",
                [(1,)],
            )
            test.statement_ok(f"drop view my_datalake.default.{name}")
    finally:
        if ("default", name) in rest_catalog.list_views("default"):
            rest_catalog.drop_view(f"default.{name}")
