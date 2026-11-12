"""Reference readers must see replacement lineage and retained branch field identities."""

from uuid import uuid4

import pytest

from duckdb_unittest import DuckDBUnittestRunner


@pytest.mark.requires_spark(">=3.5")
@pytest.mark.parametrize("replacement", ["bare", "zero_rows", "populated"])
def test_replacement_branch_and_history(
    spark_con, rest_catalog, unittest_binary, unittest_test_config, print_unittest_stdin, replacement
):
    name = "replacement_interop_" + uuid4().hex
    table = f"default.{name}"
    duck_table = f"my_datalake.{table}"
    spark_con.sql(
        f"CREATE TABLE {table} (id INT, detail STRUCT<x: INT, y: STRING>, dropped INT) "
        "USING iceberg TBLPROPERTIES ('format-version' = '2')"
    )
    try:
        spark_con.sql(f"INSERT INTO {table} SELECT 1, named_struct('x', 10, 'y', 'old'), 99")
        original = rest_catalog.load_table(table)
        old_snapshot = original.current_snapshot().snapshot_id
        old_ids = {
            path: original.schema().find_field(path).field_id for path in ["id", "detail", "detail.x", "detail.y"]
        }
        spark_con.sql(f"ALTER TABLE {table} CREATE BRANCH before_replace AS OF VERSION {old_snapshot}")
        spark_con.sql(f"ALTER TABLE {table} CREATE TAG old_schema AS OF VERSION {old_snapshot}")

        with DuckDBUnittestRunner(
            unittest_binary, test_config=unittest_test_config, print_stdin=print_unittest_stdin
        ) as test:
            if replacement == "bare":
                test.statement_ok(
                    f"CREATE OR REPLACE TABLE {duck_table} "
                    "(added BIGINT, detail STRUCT(y VARCHAR, x BIGINT, z INTEGER), id BIGINT)"
                )
            else:
                predicate = "false" if replacement == "zero_rows" else "true"
                test.statement_ok(
                    f"CREATE OR REPLACE TABLE {duck_table} AS "
                    "SELECT 100::BIGINT added, {'y': 'new', 'x': 20::BIGINT, 'z': 30} detail, 2::BIGINT id "
                    f"WHERE {predicate}"
                )

        updated = rest_catalog.load_table(table)
        assert updated.metadata.table_uuid == original.metadata.table_uuid
        assert updated.location() == original.location()
        assert updated.refs()["before_replace"].snapshot_id == old_snapshot
        assert updated.refs()["old_schema"].snapshot_id == old_snapshot
        for path, field_id in old_ids.items():
            assert updated.schema().find_field(path).field_id == field_id
        for path in ["added", "detail.z"]:
            assert updated.schema().find_field(path).field_id > original.metadata.last_column_id
        if replacement == "bare":
            assert updated.current_snapshot() is None
            assert "main" not in updated.refs()
            assert len(updated.snapshots()) == 1
        else:
            snapshot = updated.current_snapshot()
            assert snapshot.parent_snapshot_id is None
            assert snapshot.summary.operation.value == "append"
            assert len(updated.snapshots()) == 2

        spark_con.sql(f"REFRESH TABLE {table}")
        projection = "id, detail.x, detail.y, detail.z, added"
        current = [tuple(row) for row in spark_con.sql(f"SELECT {projection} FROM {table}").collect()]
        assert current == ([(2, 20, "new", 30, 100)] if replacement == "populated" else [])
        # Branch reads use the current schema: matching IDs must recover the old values.
        branch = spark_con.sql(f"SELECT {projection} FROM {table}.branch_before_replace").collect()
        assert [tuple(row) for row in branch] == [(1, 10, "old", None, None)]
        # Tags and snapshot-ID reads retain the snapshot's original schema.
        for source in [f"{table}.tag_old_schema", f"{table} VERSION AS OF {old_snapshot}"]:
            rows = spark_con.sql(f"SELECT id, detail.x, detail.y, dropped FROM {source}").collect()
            assert [tuple(row) for row in rows] == [(1, 10, "old", 99)]

        spark_con.sql(f"INSERT INTO {table} SELECT 200L, named_struct('y', 'spark', 'x', 40L, 'z', 50), 3L")
        appended = rest_catalog.load_table(table).current_snapshot()
        previous = updated.current_snapshot()
        assert appended.parent_snapshot_id == (previous.snapshot_id if previous else None)
        with DuckDBUnittestRunner(
            unittest_binary, test_config=unittest_test_config, print_stdin=print_unittest_stdin
        ) as test:
            test.query("II", f"SELECT id, detail.x FROM {duck_table} WHERE id = 3", [(3, 40)])
            test.query("II", f"SELECT id, dropped FROM {duck_table} AT (VERSION => {old_snapshot})", [(1, 99)])
    finally:
        spark_con.sql(f"DROP TABLE IF EXISTS {table}")
