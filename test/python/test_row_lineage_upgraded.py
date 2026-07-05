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
                    f"select DISTINCT ON(_row_id) id, data from {CATALOG_TABLE_NAME} order by id",
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
                        (True, 1, "replaced"),
                        (True, 2, "replaced_again"),
                        (True, 4, "d_u1"),
                        (True, 6, "replaced_again"),
                        (True, 7, "g_new"),
                    ],
                )
                test.statement_ok(f"set variable id2_row_id = (select _row_id from {CATALOG_TABLE_NAME} where id = 2)")
                test.statement_ok(f"set variable id6_row_id = (select _row_id from {CATALOG_TABLE_NAME} where id = 6)")
                test.query(
                    "I",
                    f"select _row_id = getvariable('id2_row_id') from {CATALOG_TABLE_NAME} where id = 2",
                    [(True,)],
                )
                test.query(
                    "I",
                    f"select _row_id = getvariable('id6_row_id') from {CATALOG_TABLE_NAME} where id = 6",
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
                    f"select _row_id = getvariable('id2_row_id') from {CATALOG_TABLE_NAME} where id = 2",
                    [(True,)],
                )
                test.query(
                    "I",
                    f"select _row_id = getvariable('id6_row_id') from {CATALOG_TABLE_NAME} where id = 6",
                    [(True,)],
                )

                test.statement_ok(f"DELETE FROM {CATALOG_TABLE_NAME} WHERE id IN (6, 8)")
                FINAL_ROWS = [(8, 2, "replaced_again"), (7, 7, "g_new")]
                test.query(
                    "III",
                    f"select _last_updated_sequence_number, id, data from {CATALOG_TABLE_NAME} order by id",
                    FINAL_ROWS,
                )
                test.query(
                    "I",
                    f"select _row_id = getvariable('id2_row_id') from {CATALOG_TABLE_NAME} where id = 2",
                    [(True,)],
                )

            test.query(
                "III",
                f"select _last_updated_sequence_number, id, data from {CATALOG_TABLE_NAME} order by id",
                FINAL_ROWS,
            )
            test.query(
                "I",
                f"select _row_id = getvariable('id2_row_id') from {CATALOG_TABLE_NAME} where id = 2",
                [(True,)],
            )

        catalog_connection.refresh_table(QUALIFIED_TABLE_NAME)
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
                _last_updated_sequence_number=8,
                has_row_id=True,
                id=2,
                data="replaced_again",
            ),
            Row(_last_updated_sequence_number=7, has_row_id=True, id=7, data="g_new"),
        ]
