import datetime
from decimal import Decimal

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


pyspark_sql = pytest.importorskip("pyspark.sql")
pa = pytest.importorskip("pyarrow")
Row = pyspark_sql.Row

INSERT_TEST_SEED = SparkSeedTable(
    "default.insert_test",
    """
    CREATE OR REPLACE TABLE default.insert_test (
        col1 DATE,
        col2 INTEGER,
        col3 STRING
    )
    TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read',
        'commit.retry.num-retries'='0'
    );
    """,
)

INSERT_ALL_TYPES_SEED = SparkSeedTable(
    "default.insert_all_types",
    """
    CREATE OR REPLACE TABLE default.insert_all_types (
        byte_col TINYINT,
        short_col SMALLINT,
        int_col INT,
        long_col BIGINT,
        float_col FLOAT,
        double_col DOUBLE,
        decimal_col DECIMAL(15, 5),
        date_col DATE
    ) USING ICEBERG;
    """,
)

FINAL_INSERT_TEST_ROWS = [
    (datetime.date(2010, 6, 11), 42, "test"),
    (datetime.date(2020, 8, 12), 45345, "inserted by con1"),
    (datetime.date(2020, 8, 13), 1, "insert 1"),
    (datetime.date(2020, 8, 14), 2, "insert 2"),
    (datetime.date(2020, 8, 15), 3, "insert 3"),
    (datetime.date(2020, 8, 16), 4, "insert 4"),
]


def _assert_insert_all_types_row(row):
    assert row["byte_col"] == 127
    assert row["short_col"] == 1340
    assert row["int_col"] == 2147483647
    assert row["long_col"] == 9223372036854775807
    assert row["float_col"] == pytest.approx(3.14, rel=1e-6)
    assert row["double_col"] == -1.7976931348623157
    assert row["decimal_col"] == Decimal("1234567890.12345")
    assert row["date_col"] == datetime.date(2023, 12, 31)


class TestInsertEndToEnd:
    @pytest.mark.spark_seed_tables(INSERT_TEST_SEED, INSERT_ALL_TYPES_SEED)
    def test_insert_end_to_end(
        self,
        catalog_connection,
        rest_catalog,
        unittest_binary,
        unittest_test_config,
        print_unittest_stdin,
    ):
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            with test.with_transaction():
                test.statement_ok(
                    """
                    INSERT INTO my_datalake.default.insert_test SELECT
                        '2010/06/11'::DATE,
                        42,
                        'test'
                    """
                )
                test.query("III", "SELECT * FROM my_datalake.default.insert_test", [FINAL_INSERT_TEST_ROWS[0]])

            test.query("III", "SELECT * FROM my_datalake.default.insert_test", [FINAL_INSERT_TEST_ROWS[0]])

            with test.with_transaction(commit=False):
                test.statement_ok(
                    """
                    INSERT INTO my_datalake.default.insert_test SELECT
                        '1980/11/25'::DATE,
                        -3434,
                        'this is a long string'
                    """
                )
                test.query(
                    "III",
                    "SELECT * FROM my_datalake.default.insert_test ORDER BY ALL",
                    [
                        (datetime.date(1980, 11, 25), -3434, "this is a long string"),
                        FINAL_INSERT_TEST_ROWS[0],
                    ],
                )

            test.query("III", "SELECT * FROM my_datalake.default.insert_test ORDER BY ALL", [FINAL_INSERT_TEST_ROWS[0]])

            test.statement_ok("begin", connection="con1")
            test.statement_ok(
                """
                INSERT INTO my_datalake.default.insert_test SELECT
                    '2020/08/12'::DATE,
                    45345,
                    'inserted by con1'
                """,
                connection="con1",
            )

            test.statement_ok("begin", connection="con2")
            test.statement_ok(
                """
                INSERT INTO my_datalake.default.insert_test SELECT
                    '2020/08/12'::DATE,
                    45345,
                    'inserted by con2'
                """,
                connection="con2",
            )

            test.statement_ok("commit", connection="con1")
            test.query(
                "III",
                "SELECT * FROM my_datalake.default.insert_test ORDER BY ALL",
                [
                    FINAL_INSERT_TEST_ROWS[0],
                    (datetime.date(2020, 8, 12), 45345, "inserted by con2"),
                ],
                connection="con2",
            )
            test.statement_error("commit", "<REGEX>:.*TransactionContext Error.*Conflict_409.*", connection="con2")
            test.query(
                "III",
                "SELECT * FROM my_datalake.default.insert_test ORDER BY ALL",
                FINAL_INSERT_TEST_ROWS[:2],
            )

            with test.with_transaction():
                test.statement_ok(
                    """
                    INSERT INTO my_datalake.default.insert_test SELECT
                        '2020/08/13'::DATE,
                        1,
                        'insert 1'
                    """
                )
                test.statement_ok(
                    """
                    INSERT INTO my_datalake.default.insert_test SELECT
                        '2020/08/14'::DATE,
                        2,
                        'insert 2'
                    """
                )
                test.statement_ok(
                    """
                    INSERT INTO my_datalake.default.insert_test SELECT
                        '2020/08/15'::DATE,
                        3,
                        'insert 3'
                    """
                )

            test.query("III", "SELECT * FROM my_datalake.default.insert_test ORDER BY ALL", FINAL_INSERT_TEST_ROWS[:5])

            with test.with_transaction():
                test.statement_ok(
                    """
                    INSERT INTO my_datalake.default.insert_test SELECT
                        '2020/08/16'::DATE,
                        4,
                        'insert 4'
                    """
                )

            with test.with_transaction():
                test.statement_ok(
                    """
                    INSERT INTO my_datalake.default.insert_all_types
                    SELECT
                        127::TINYINT,
                        1340::SMALLINT,
                        2147483647::INT,
                        9223372036854775807::BIGINT,
                        3.14::FLOAT,
                        -1.7976931348623157::DOUBLE,
                        1234567890.12345::DECIMAL(15, 5),
                        DATE '2023-12-31'
                    """
                )

            test.query("III", "SELECT * FROM my_datalake.default.insert_test ORDER BY ALL", FINAL_INSERT_TEST_ROWS)
            test.query(
                "IIIIIIII",
                "SELECT * FROM my_datalake.default.insert_all_types ORDER BY ALL",
                [
                    (
                        127,
                        1340,
                        2147483647,
                        9223372036854775807,
                        3.14,
                        -1.7976931348623157,
                        1234567890.12345,
                        "2023-12-31",
                    )
                ],
            )
            test.query(
                "I", "SELECT distinct status FROM iceberg_metadata('my_datalake.default.insert_test')", [("ADDED",)]
            )
            test.query(
                "I", "SELECT distinct content FROM iceberg_metadata('my_datalake.default.insert_test')", [("DATA",)]
            )
            test.query(
                "I",
                "SELECT distinct manifest_content FROM iceberg_metadata('my_datalake.default.insert_test')",
                [("DATA",)],
            )

        catalog_connection.restart()

        spark_insert_rows = catalog_connection.con.sql(
            "SELECT * FROM default.insert_test ORDER BY col1, col2, col3"
        ).collect()
        assert spark_insert_rows == [Row(col1=row[0], col2=row[1], col3=row[2]) for row in FINAL_INSERT_TEST_ROWS]

        spark_all_types_rows = catalog_connection.con.sql("SELECT * FROM default.insert_all_types").collect()
        assert len(spark_all_types_rows) == 1
        spark_all_types_row = spark_all_types_rows[0].asDict(recursive=True)
        _assert_insert_all_types_row(spark_all_types_row)

        insert_table = rest_catalog.load_table("default.insert_test")
        insert_arrow_table: pa.Table = insert_table.scan().to_arrow()
        assert insert_arrow_table.to_pylist() == [
            {"col1": row[0], "col2": row[1], "col3": row[2]} for row in FINAL_INSERT_TEST_ROWS
        ]

        all_types_table = rest_catalog.load_table("default.insert_all_types")
        all_types_arrow_table: pa.Table = all_types_table.scan().to_arrow()
        all_types_rows = all_types_arrow_table.to_pylist()
        assert len(all_types_rows) == 1
        _assert_insert_all_types_row(all_types_rows[0])
