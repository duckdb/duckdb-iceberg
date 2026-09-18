"""Views written by a real Spark Iceberg catalog and loaded by DuckDB."""

from uuid import uuid4
import json

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


def test_spark_view_cross_namespace(
    spark_con, catalog_connection, unittest_binary, unittest_test_config, print_unittest_stdin, tmp_path
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
        spark_con.sql(f"create view default.{name} (answer) as select n from source")
        assert [row[0] for row in spark_con.sql(f"select answer from default.{name} order by answer").collect()] == [
            42,
            43,
        ]

        # Catalog names in a foreign view are engine configuration names. Attach the
        # selected catalog under Spark's name as well, using the existing profile.
        config = json.loads(unittest_test_config.read_text())
        config["on_init"] += f' alter database my_datalake set alias to "{catalog}";'
        config_path = tmp_path / "view_interop.json"
        config_path.write_text(json.dumps(config))
        with DuckDBUnittestRunner(unittest_binary, test_config=config_path, print_stdin=print_unittest_stdin) as test:
            test.query("I", f'select answer from "{catalog}".default.{name} order by answer', [(42,), (43,)])
            test.statement_ok(f'drop view "{catalog}".default.{name}')
    finally:
        spark_con.sql(f"use {catalog}.default")
        spark_con.sql(f"drop view if exists default.{name}")
        spark_con.sql(f"drop table if exists {namespace}.source")
        spark_con.sql(f"drop namespace if exists {namespace}")
