"""Views written by a real Spark Iceberg catalog and loaded by DuckDB."""

from uuid import uuid4
import json
import pytest

from duckdb_unittest import DuckDBUnittestRunner


def test_spark_view_interop(spark_con, unittest_binary, unittest_test_config, print_unittest_stdin):
    name = "spark_view_" + uuid4().hex
    spark_con.sql(f"create view default.{name} (answer) as select 42 as original_name")
    try:
        assert spark_con.sql(f"select answer from default.{name}").first()[0] == 42
        with DuckDBUnittestRunner(
            unittest_binary, test_config=unittest_test_config, print_stdin=print_unittest_stdin
        ) as test:
            test.query("I", f"select answer from my_datalake.default.{name}", [(42,)])
            test.query("I", f"select count(*) from iceberg_view_metadata('my_datalake.default.{name}')", [(1,)])
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
            unittest_binary, test_config=unittest_test_config, print_stdin=print_unittest_stdin
        ) as test:
            test.statement_error(
                f'select * from my_datalake.default."{name}"', "its SQL dialect cannot be parsed by DuckDB"
            )
            test.query(
                "I",
                f"select count(*) from duckdb_views() where view_name = '{escaped_name}' and not is_bound",
                [(1,)],
            )
            test.query(
                "I", f"select count(*) from iceberg_view_metadata('my_datalake.default.\"{escaped_name}\"')", [(1,)]
            )
            test.statement_ok(f'drop view my_datalake.default."{name}"')
    finally:
        spark_con.sql(f"drop view if exists default.`{name}`")


@pytest.mark.parametrize(
    "query,expected",
    [
        ("select n from source", [(42,), (43,)]),
        ("select n from {namespace}.source", [(42,), (43,)]),
        ("select n from {catalog}.{namespace}.source", [(42,), (43,)]),
        ("with source as (select 61 as n) select n from source", [(61,)]),
        ("select (select max(n) from source) as n", [(43,)]),
        (
            "select n from source union all select n from (with source as (select 61 as n) select * from source)",
            [(42,), (43,), (61,)],
        ),
        ("with source as (select n+1 as n from source) select n from source", [(43,), (44,)]),
    ],
)
def test_spark_view_cross_namespace(
    spark_con,
    catalog_connection,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
    tmp_path,
    query,
    expected,
):
    namespace = "view_source_" + uuid4().hex
    name = "spark_view_" + uuid4().hex
    catalog = catalog_connection.catalog
    spark_con.sql(f"create namespace {namespace}")
    try:
        spark_con.sql(f"create table {namespace}.source (n int) using iceberg")
        spark_con.sql(f"insert into {namespace}.source values (42), (43)")
        spark_con.sql(f"use {catalog}.{namespace}")
        # Store the view in a different namespace from its unqualified source table.
        sql = query.format(catalog=catalog, namespace=namespace)
        spark_con.sql(f"create view default.{name} (answer) as {sql}")
        assert [
            tuple(row) for row in spark_con.sql(f"select answer from default.{name} order by answer").collect()
        ] == expected

        # Catalog names in a foreign view are engine configuration names. Attach the
        # selected catalog under Spark's name as well, using the existing profile.
        config = json.loads(unittest_test_config.read_text())
        config["on_init"] += f' alter database my_datalake set alias to "{catalog}";'
        config_path = tmp_path / "view_interop.json"
        config_path.write_text(json.dumps(config))
        with DuckDBUnittestRunner(unittest_binary, test_config=config_path, print_stdin=print_unittest_stdin) as test:
            test.query("I", f'select answer from "{catalog}".default.{name} order by answer', expected)
            test.statement_ok(f'drop view "{catalog}".default.{name}')
    finally:
        spark_con.sql(f"use {catalog}.default")
        spark_con.sql(f"drop view if exists default.{name}")
        spark_con.sql(f"drop table if exists {namespace}.source")
        spark_con.sql(f"drop namespace if exists {namespace}")
