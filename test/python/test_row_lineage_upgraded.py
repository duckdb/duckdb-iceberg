import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable
from test_spark_read import Row


TABLE_NAME = "row_lineage_test_upgraded"
QUALIFIED_TABLE_NAME = f"default.{TABLE_NAME}"
CATALOG_TABLE_NAME = f"my_datalake.{QUALIFIED_TABLE_NAME}"

ROW_LINEAGE_SEED = SparkSeedTable(
    QUALIFIED_TABLE_NAME,
    f"""
    CREATE OR REPLACE TABLE {QUALIFIED_TABLE_NAME} (
      id INT,
      data STRING
    )
    TBLPROPERTIES (
        'format-version'='2',
        'write.delete.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
    );

    INSERT INTO {QUALIFIED_TABLE_NAME} VALUES
    (1, 'a'),
    (2, 'b'),
    (3, 'c'),
    (4, 'd'),
    (5, 'e');

    UPDATE {QUALIFIED_TABLE_NAME}
    SET data = CONCAT(data, '_u1')
    WHERE id IN (2, 4);

    DELETE FROM {QUALIFIED_TABLE_NAME}
    WHERE id IN (3, 5);

    INSERT INTO {QUALIFIED_TABLE_NAME} VALUES
    (6, 'f'),
    (7, 'g');

    UPDATE {QUALIFIED_TABLE_NAME}
    SET data = 'replaced'
    WHERE id IN (1, 6);

    DELETE FROM {QUALIFIED_TABLE_NAME} WHERE id = 7;

    INSERT INTO {QUALIFIED_TABLE_NAME} VALUES
    (7, 'g_new');

    ALTER TABLE {QUALIFIED_TABLE_NAME}
    SET TBLPROPERTIES (
        'format-version'='3',
        'write.delete.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
    );
    """,
)


class TestRowLineageUnittestStdin:
    @pytest.mark.requires_spark(">=4.0")
    @pytest.mark.requires_capabilities("row_lineage", "format_v3")
    @pytest.mark.spark_seed_tables(ROW_LINEAGE_SEED)
    def test_row_lineage_test_upgraded_end_to_end(
        self,
        catalog_connection,
        unittest_binary,
        unittest_test_config,
        print_unittest_stdin,
    ):
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            with test.with_transaction(commit=False):
                test.statement_ok(
                    f"""
                    INSERT into {CATALOG_TABLE_NAME} VALUES
                        (8, 'not_replaced'),
                        (9, 'also_not_replaced')
                    """
                )
                test.query(
                    "II",
                    f"select id, data from {CATALOG_TABLE_NAME} order by id",
                    [
                        (1, "replaced"),
                        (2, "b_u1"),
                        (4, "d_u1"),
                        (6, "replaced"),
                        (7, "g_new"),
                        (8, "not_replaced"),
                        (9, "also_not_replaced"),
                    ],
                )

            with test.with_transaction(commit=False):
                test.statement_ok(f"DELETE FROM {CATALOG_TABLE_NAME} WHERE id IN (2, 6)")
                test.query(
                    "II",
                    f"select id, data from {CATALOG_TABLE_NAME} order by id",
                    [(1, "replaced"), (4, "d_u1"), (7, "g_new")],
                )

            with test.with_transaction(commit=False):
                test.statement_ok(
                    f"""
                    UPDATE {CATALOG_TABLE_NAME}
                    SET data = 'replaced_again'
                    WHERE id IN (2, 6)
                    """
                )
                test.query(
                    "II",
                    f"select id, data from {CATALOG_TABLE_NAME} order by id",
                    [
                        (1, "replaced"),
                        (2, "replaced_again"),
                        (4, "d_u1"),
                        (6, "replaced_again"),
                        (7, "g_new"),
                    ],
                )

            FINAL_ROWS = [(8, 2, "replaced_again"), (7, 7, "g_new")]
            with test.with_transaction():
                test.statement_ok(
                    f"""
                    UPDATE {CATALOG_TABLE_NAME}
                    SET data = 'replaced_again'
                    WHERE id IN (2, 6)
                    """
                )
                test.query(
                    "III",
                    f"select _row_id IS NOT NULL, id, data from {CATALOG_TABLE_NAME} order by id",
                    [
                        (False, 1, "replaced"),
                        (False, 2, "replaced_again"),
                        (False, 4, "d_u1"),
                        (False, 6, "replaced_again"),
                        (False, 7, "g_new"),
                    ],
                )
                test.query(
                    "I",
                    f"select _row_id IS NULL from {CATALOG_TABLE_NAME} where id = 2",
                    [(True,)],
                )
                test.query(
                    "I",
                    f"select _row_id IS NULL from {CATALOG_TABLE_NAME} where id = 6",
                    [(True,)],
                )

                test.statement_ok(f"DELETE FROM {CATALOG_TABLE_NAME} WHERE id IN (4, 1)")
                test.query(
                    "II",
                    f"select id, data from {CATALOG_TABLE_NAME} order by id",
                    [(2, "replaced_again"), (6, "replaced_again"), (7, "g_new")],
                )
                test.query(
                    "I",
                    f"select _row_id IS NULL from {CATALOG_TABLE_NAME} where id = 2",
                    [(True,)],
                )
                test.query(
                    "I",
                    f"select _row_id IS NULL from {CATALOG_TABLE_NAME} where id = 6",
                    [(True,)],
                )

                test.statement_ok(f"DELETE FROM {CATALOG_TABLE_NAME} WHERE id IN (6, 8)")
                FINAL_ROWS = [(8, 2, "replaced_again"), (7, 7, "g_new")]
                test.query(
                    "III",
                    f"select _last_updated_sequence_number, id, data from {CATALOG_TABLE_NAME} order by id",
                    [(None, 2, "replaced_again"), (None, 7, "g_new")],
                )
                test.query(
                    "I",
                    f"select _row_id IS NULL from {CATALOG_TABLE_NAME} where id = 2",
                    [(True,)],
                )

            test.query(
                "III",
                f"select _last_updated_sequence_number, id, data from {CATALOG_TABLE_NAME} order by id",
                FINAL_ROWS,
            )
            # Only committed IDs can be captured and preserved by subsequent rewrites.
            test.statement_ok(f"set variable id2_row_id = (select _row_id from {CATALOG_TABLE_NAME} where id = 2)")
            with test.with_transaction():
                for value in ("intermediate", "replaced_again"):
                    test.statement_ok(f"UPDATE {CATALOG_TABLE_NAME} SET data = '{value}' WHERE id = 2")
                    test.query(
                        "II",
                        f"""select _row_id = getvariable('id2_row_id'),
                            _last_updated_sequence_number IS NULL
                            from {CATALOG_TABLE_NAME} where id = 2""",
                        [(True, True)],
                    )
            test.query(
                "II",
                f"""select _row_id = getvariable('id2_row_id'), _last_updated_sequence_number
                    from {CATALOG_TABLE_NAME} where id = 2""",
                [(True, 12)],
            )

        catalog_connection.restart()
        df = catalog_connection.con.sql(
            f"""
            select _last_updated_sequence_number, _row_id IS NOT NULL as has_row_id, *
            from {QUALIFIED_TABLE_NAME}
            order by id
            """
        )
        res = df.collect()
        print(res)
        assert res == [
            Row(
                _last_updated_sequence_number=12,
                has_row_id=True,
                id=2,
                data="replaced_again",
            ),
            Row(_last_updated_sequence_number=7, has_row_id=True, id=7, data="g_new"),
        ]

    @pytest.mark.requires_spark(">=4.0")
    @pytest.mark.requires_capabilities("row_lineage", "format_v3")
    def test_pending_lineage_spark_append(
        self,
        catalog_connection,
        unittest_binary,
        unittest_test_config,
        print_unittest_stdin,
    ):
        table = "my_datalake.default.pending_lineage_spark_append"
        spark_table = "default.pending_lineage_spark_append"
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            test.statement_ok(f"DROP TABLE IF EXISTS {table}")
            test.statement_ok(f"CREATE TABLE {table} (id INT, data STRING) WITH ('format-version' = 3)")
            test.statement_ok(f"INSERT INTO {table} VALUES (0, 'base'), (1, 'committed')")
            with test.with_transaction():
                test.statement_ok(f"INSERT INTO {table} VALUES (2, 'pending'), (3, 'deleted')")
                test.statement_ok(f"UPDATE {table} SET data = 'first' WHERE id IN (1, 2)")
                test.statement_ok(f"DELETE FROM {table} WHERE id = 3")
                test.statement_ok(f"UPDATE {table} SET data = 'second' WHERE id IN (1, 2)")
                test.query(
                    "III",
                    f"SELECT id, _row_id, _last_updated_sequence_number FROM {table} ORDER BY id",
                    [(0, 0, 1), (1, 1, None), (2, None, None)],
                )

        catalog_connection.restart()
        rows = catalog_connection.con.sql(
            f"SELECT id, data, _row_id, _last_updated_sequence_number FROM {spark_table} ORDER BY id"
        ).collect()
        assert [(row.id, row.data, row._last_updated_sequence_number) for row in rows] == [
            (0, "base", 1),
            (1, "second", 5),
            (2, "second", 5),
        ]
        assigned_ids = {row.id: row._row_id for row in rows}
        assert assigned_ids[0] == 0
        assert assigned_ids[1] == 1
        assert None not in assigned_ids.values()
        assert len(set(assigned_ids.values())) == 3

        catalog_connection.con.sql(f"INSERT INTO {spark_table} VALUES (4, 'spark')")
        rows = catalog_connection.con.sql(f"SELECT id, _row_id FROM {spark_table} ORDER BY id").collect()
        assert {row.id: row._row_id for row in rows if row.id != 4} == assigned_ids
        assert rows[-1].id == 4
        assert rows[-1]._row_id > max(assigned_ids.values())
        assert len({row._row_id for row in rows}) == 4

        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            test.query("II", f"SELECT count(*), count(DISTINCT _row_id) FROM {table}", [(4, 4)])
            test.query(
                "II",
                f"SELECT id, data FROM {table} ORDER BY id",
                [(0, "base"), (1, "second"), (2, "second"), (4, "spark")],
            )
            test.statement_ok(f"DROP TABLE {table}")
