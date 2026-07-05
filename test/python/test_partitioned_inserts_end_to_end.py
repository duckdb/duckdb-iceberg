import datetime
import os
import time
from dataclasses import dataclass
from decimal import Decimal

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


# PySpark converts timestamps through Python's local timezone, so pin to UTC
# before the session fixture is initialized.
os.environ["TZ"] = "UTC"
time.tzset()

pyspark_sql = pytest.importorskip("pyspark.sql")
Row = pyspark_sql.Row


def _sql_rows(start_id: int, literals: list[str]) -> str:
    return ",\n    ".join(f"({start_id + offset}, {literal})" for offset, literal in enumerate(literals))


def _build_seed(
    table_name: str,
    column_name: str,
    column_type: str,
    partition_expr: str,
    literals: list[str],
) -> SparkSeedTable:
    qualified_table_name = f"default.{table_name}"
    return SparkSeedTable(
        qualified_table_name,
        f"""
        CREATE OR REPLACE TABLE {qualified_table_name} (
            id INTEGER,
            {column_name} {column_type}
        )
        USING iceberg
        PARTITIONED BY ({partition_expr});

        INSERT INTO {qualified_table_name} VALUES
            {_sql_rows(1, literals)}
        """,
    )


def _expected_rows(column_name: str, values: list[object]) -> list[Row]:
    rows = [Row(id=index, **{column_name: value}) for index, value in enumerate(values[:-1], start=1)]
    rows.append(Row(id=11, **{column_name: None}))
    rows.extend(Row(id=index + 100, **{column_name: value}) for index, value in enumerate(values[:-1], start=1))
    rows.append(Row(id=111, **{column_name: None}))
    return rows


@dataclass(frozen=True)
class PartitionedInsertCase:
    table_name: str
    column_name: str
    spark_seed: SparkSeedTable
    duckdb_insert_literals: list[str]
    duckdb_filter_literals: list[str]
    spark_expected_rows: list[Row]
    spark_filter_sql: str
    spark_filter_expected_rows: list[Row]
    requires_icu: bool = False

    @property
    def qualified_table_name(self) -> str:
        return f"default.{self.table_name}"

    @property
    def catalog_table_name(self) -> str:
        return f"my_datalake.{self.qualified_table_name}"

    @property
    def duckdb_insert_sql(self) -> str:
        return f"""
        INSERT INTO {self.catalog_table_name} VALUES
            {_sql_rows(101, self.duckdb_insert_literals)}
        """


ANIMAL_VALUES = ["aardvark", "bison", "camel", "dingo", "eagle", "falcon", "gecko", "hippo", "ibis", "jaguar", None]
DATE_VALUES = [datetime.date(2020, month, 1) for month in range(1, 11)] + [None]
TIMESTAMP_VALUES = [datetime.datetime(2023, month, 1, 0, 0, 0) for month in range(1, 11)] + [None]
TRUNCATE_NUMBER_VALUES = [1, 11, 21, 31, 41, 51, 61, 71, 81, 91, None]
BUCKET_DECIMAL_VALUES = [Decimal(f"{value}.00") for value in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]] + [None]
TRUNCATE_DECIMAL_VALUES = [Decimal(f"{value}.00") for value in range(1, 11)] + [None]
BUCKET_BLOB_VALUES = [
    bytearray(b"\x01\x02\x03\x04\x05"),
    bytearray(b"\x06\x07\x08\x09\x10"),
    bytearray(b"\x11\x12\x13\x14\x15"),
    bytearray(b"\x16\x17\x18\x19\x20"),
    bytearray(b"\x21\x22\x23\x24\x25"),
    bytearray(b"\x26\x27\x28\x29\x30"),
    bytearray(b"\x31\x32\x33\x34\x35"),
    bytearray(b"\x36\x37\x38\x39\x40"),
    bytearray(b"\x41\x42\x43\x44\x45"),
    bytearray(b"\x46\x47\x48\x49\x50"),
    None,
]
TRUNCATE_BINARY_VALUES = [
    bytearray(b"\x01\x02\x03"),
    bytearray(b"\x02\x03\x04"),
    bytearray(b"\x03\x04\x05"),
    bytearray(b"\x04\x05\x06"),
    bytearray(b"\x05\x06\x07"),
    bytearray(b"\x06\x07\x08"),
    bytearray(b"\x07\x08\x09"),
    bytearray(b"\x08\x09\x0a"),
    bytearray(b"\x09\x00\x0a"),
    bytearray(b"\x0a\x00\x0b"),
    None,
]


PARTITIONED_INSERT_CASES = [
    PartitionedInsertCase(
        table_name="bucket_partitioned_int_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "bucket_partitioned_int_for_insert",
            "value",
            "INTEGER",
            "bucket(4, value)",
            [str(value) for value in range(1, 11)] + ["NULL"],
        ),
        duckdb_insert_literals=[str(value) for value in range(1, 11)] + ["NULL"],
        duckdb_filter_literals=[str(value) for value in range(1, 11)],
        spark_expected_rows=_expected_rows("value", list(range(1, 11)) + [None]),
        spark_filter_sql="SELECT * FROM default.bucket_partitioned_int_for_insert WHERE value = 1 ORDER BY id",
        spark_filter_expected_rows=[Row(id=1, value=1), Row(id=101, value=1)],
    ),
    PartitionedInsertCase(
        table_name="bucket_partitioned_bigint_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "bucket_partitioned_bigint_for_insert",
            "value",
            "BIGINT",
            "bucket(4, value)",
            [f"{value}L" for value in range(1, 11)] + ["NULL"],
        ),
        duckdb_insert_literals=[f"{value}::BIGINT" for value in range(1, 11)] + ["NULL"],
        duckdb_filter_literals=[str(value) for value in range(1, 11)],
        spark_expected_rows=_expected_rows("value", list(range(1, 11)) + [None]),
        spark_filter_sql="SELECT * FROM default.bucket_partitioned_bigint_for_insert WHERE value = 1 ORDER BY id",
        spark_filter_expected_rows=[Row(id=1, value=1), Row(id=101, value=1)],
    ),
    PartitionedInsertCase(
        table_name="bucket_partitioned_varchar_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "bucket_partitioned_varchar_for_insert",
            "value",
            "STRING",
            "bucket(8, value)",
            [f"'{value}'" for value in ANIMAL_VALUES[:-1]] + ["NULL"],
        ),
        duckdb_insert_literals=[f"'{value}'" for value in ANIMAL_VALUES[:-1]] + ["NULL"],
        duckdb_filter_literals=[f"'{value}'" for value in ANIMAL_VALUES[:-1]],
        spark_expected_rows=_expected_rows("value", ANIMAL_VALUES),
        spark_filter_sql=(
            "SELECT * FROM default.bucket_partitioned_varchar_for_insert WHERE value = 'aardvark' ORDER BY id"
        ),
        spark_filter_expected_rows=[Row(id=1, value="aardvark"), Row(id=101, value="aardvark")],
    ),
    PartitionedInsertCase(
        table_name="bucket_partitioned_date_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "bucket_partitioned_date_for_insert",
            "value",
            "DATE",
            "bucket(4, value)",
            [f"CAST('{value.isoformat()}' AS DATE)" for value in DATE_VALUES[:-1]] + ["NULL"],
        ),
        duckdb_insert_literals=[f"'{value.isoformat()}'::DATE" for value in DATE_VALUES[:-1]] + ["NULL"],
        duckdb_filter_literals=[f"'{value.isoformat()}'::DATE" for value in DATE_VALUES[:-1]],
        spark_expected_rows=_expected_rows("value", DATE_VALUES),
        spark_filter_sql=(
            "SELECT * FROM default.bucket_partitioned_date_for_insert WHERE value = DATE '2020-01-01' ORDER BY id"
        ),
        spark_filter_expected_rows=[
            Row(id=1, value=datetime.date(2020, 1, 1)),
            Row(id=101, value=datetime.date(2020, 1, 1)),
        ],
    ),
    PartitionedInsertCase(
        table_name="bucket_partitioned_timestamp_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "bucket_partitioned_timestamp_for_insert",
            "value",
            "TIMESTAMP",
            "bucket(4, value)",
            [f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'" for value in TIMESTAMP_VALUES[:-1]] + ["NULL"],
        ),
        duckdb_insert_literals=[
            f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'::TIMESTAMP" for value in TIMESTAMP_VALUES[:-1]
        ]
        + ["NULL"],
        duckdb_filter_literals=[
            f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'::TIMESTAMP" for value in TIMESTAMP_VALUES[:-1]
        ],
        spark_expected_rows=_expected_rows("value", TIMESTAMP_VALUES),
        spark_filter_sql=(
            "SELECT * FROM default.bucket_partitioned_timestamp_for_insert "
            "WHERE value = TIMESTAMP '2023-01-01 00:00:00' ORDER BY id"
        ),
        spark_filter_expected_rows=[
            Row(id=1, value=datetime.datetime(2023, 1, 1, 0, 0, 0)),
            Row(id=101, value=datetime.datetime(2023, 1, 1, 0, 0, 0)),
        ],
        requires_icu=True,
    ),
    PartitionedInsertCase(
        table_name="bucket_partitioned_decimal_for_insert",
        column_name="amount",
        spark_seed=_build_seed(
            "bucket_partitioned_decimal_for_insert",
            "amount",
            "DECIMAL(10, 2)",
            "bucket(4, amount)",
            [str(value) for value in BUCKET_DECIMAL_VALUES[:-1]] + ["NULL"],
        ),
        duckdb_insert_literals=[f"{value}::DECIMAL(10,2)" for value in BUCKET_DECIMAL_VALUES[:-1]] + ["NULL"],
        duckdb_filter_literals=[f"{value}::DECIMAL(10,2)" for value in BUCKET_DECIMAL_VALUES[:-1]],
        spark_expected_rows=_expected_rows("amount", BUCKET_DECIMAL_VALUES),
        spark_filter_sql="SELECT * FROM default.bucket_partitioned_decimal_for_insert WHERE amount = 10.00 ORDER BY id",
        spark_filter_expected_rows=[Row(id=1, amount=Decimal("10.00")), Row(id=101, amount=Decimal("10.00"))],
    ),
    PartitionedInsertCase(
        table_name="bucket_partitioned_blob_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "bucket_partitioned_blob_for_insert",
            "value",
            "BINARY",
            "bucket(4, value)",
            [
                "X'0102030405'",
                "X'0607080910'",
                "X'1112131415'",
                "X'1617181920'",
                "X'2122232425'",
                "X'2627282930'",
                "X'3132333435'",
                "X'3637383940'",
                "X'4142434445'",
                "X'4647484950'",
                "NULL",
            ],
        ),
        duckdb_insert_literals=[
            "'\\x01\\x02\\x03\\x04\\x05'::BLOB",
            "'\\x06\\x07\\x08\\x09\\x10'::BLOB",
            "'\\x11\\x12\\x13\\x14\\x15'::BLOB",
            "'\\x16\\x17\\x18\\x19\\x20'::BLOB",
            "'\\x21\\x22\\x23\\x24\\x25'::BLOB",
            "'\\x26\\x27\\x28\\x29\\x30'::BLOB",
            "'\\x31\\x32\\x33\\x34\\x35'::BLOB",
            "'\\x36\\x37\\x38\\x39\\x40'::BLOB",
            "'\\x41\\x42\\x43\\x44\\x45'::BLOB",
            "'\\x46\\x47\\x48\\x49\\x50'::BLOB",
            "NULL",
        ],
        duckdb_filter_literals=[
            "'\\x01\\x02\\x03\\x04\\x05'::BLOB",
            "'\\x06\\x07\\x08\\x09\\x10'::BLOB",
            "'\\x11\\x12\\x13\\x14\\x15'::BLOB",
            "'\\x16\\x17\\x18\\x19\\x20'::BLOB",
            "'\\x21\\x22\\x23\\x24\\x25'::BLOB",
            "'\\x26\\x27\\x28\\x29\\x30'::BLOB",
            "'\\x31\\x32\\x33\\x34\\x35'::BLOB",
            "'\\x36\\x37\\x38\\x39\\x40'::BLOB",
            "'\\x41\\x42\\x43\\x44\\x45'::BLOB",
            "'\\x46\\x47\\x48\\x49\\x50'::BLOB",
        ],
        spark_expected_rows=_expected_rows("value", BUCKET_BLOB_VALUES),
        spark_filter_sql="SELECT * FROM default.bucket_partitioned_blob_for_insert WHERE value = X'0102030405' ORDER BY id",
        spark_filter_expected_rows=[
            Row(id=1, value=bytearray(b"\x01\x02\x03\x04\x05")),
            Row(id=101, value=bytearray(b"\x01\x02\x03\x04\x05")),
        ],
    ),
    PartitionedInsertCase(
        table_name="truncate_partitioned_int_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "truncate_partitioned_int_for_insert",
            "value",
            "INTEGER",
            "truncate(value, 10)",
            [str(value) for value in TRUNCATE_NUMBER_VALUES[:-1]] + ["NULL"],
        ),
        duckdb_insert_literals=[str(value) for value in TRUNCATE_NUMBER_VALUES[:-1]] + ["NULL"],
        duckdb_filter_literals=[str(value) for value in TRUNCATE_NUMBER_VALUES[:-1]],
        spark_expected_rows=_expected_rows("value", TRUNCATE_NUMBER_VALUES),
        spark_filter_sql="SELECT * FROM default.truncate_partitioned_int_for_insert WHERE value = 1 ORDER BY id",
        spark_filter_expected_rows=[Row(id=1, value=1), Row(id=101, value=1)],
    ),
    PartitionedInsertCase(
        table_name="truncate_partitioned_bigint_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "truncate_partitioned_bigint_for_insert",
            "value",
            "BIGINT",
            "truncate(value, 10)",
            [str(value) for value in TRUNCATE_NUMBER_VALUES[:-1]] + ["NULL"],
        ),
        duckdb_insert_literals=[str(value) for value in TRUNCATE_NUMBER_VALUES[:-1]] + ["NULL"],
        duckdb_filter_literals=[str(value) for value in TRUNCATE_NUMBER_VALUES[:-1]],
        spark_expected_rows=_expected_rows("value", TRUNCATE_NUMBER_VALUES),
        spark_filter_sql="SELECT * FROM default.truncate_partitioned_bigint_for_insert WHERE value = 1 ORDER BY id",
        spark_filter_expected_rows=[Row(id=1, value=1), Row(id=101, value=1)],
    ),
    PartitionedInsertCase(
        table_name="truncate_partitioned_varchar_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "truncate_partitioned_varchar_for_insert",
            "value",
            "STRING",
            "truncate(value, 3)",
            [f"'{value}'" for value in ANIMAL_VALUES[:-1]] + ["NULL"],
        ),
        duckdb_insert_literals=[f"'{value}'" for value in ANIMAL_VALUES[:-1]] + ["NULL"],
        duckdb_filter_literals=[f"'{value}'" for value in ANIMAL_VALUES[:-1]],
        spark_expected_rows=_expected_rows("value", ANIMAL_VALUES),
        spark_filter_sql=(
            "SELECT * FROM default.truncate_partitioned_varchar_for_insert WHERE value = 'aardvark' ORDER BY id"
        ),
        spark_filter_expected_rows=[Row(id=1, value="aardvark"), Row(id=101, value="aardvark")],
    ),
    PartitionedInsertCase(
        table_name="truncate_partitioned_binary_for_insert",
        column_name="value",
        spark_seed=_build_seed(
            "truncate_partitioned_binary_for_insert",
            "value",
            "BINARY",
            "truncate(value, 2)",
            [
                "X'010203'",
                "X'020304'",
                "X'030405'",
                "X'040506'",
                "X'050607'",
                "X'060708'",
                "X'070809'",
                "X'08090A'",
                "X'09000A'",
                "X'0A000B'",
                "NULL",
            ],
        ),
        duckdb_insert_literals=[
            "'\\x01\\x02\\x03'::BLOB",
            "'\\x02\\x03\\x04'::BLOB",
            "'\\x03\\x04\\x05'::BLOB",
            "'\\x04\\x05\\x06'::BLOB",
            "'\\x05\\x06\\x07'::BLOB",
            "'\\x06\\x07\\x08'::BLOB",
            "'\\x07\\x08\\x09'::BLOB",
            "'\\x08\\x09\\x0A'::BLOB",
            "'\\x09\\x00\\x0A'::BLOB",
            "'\\x0A\\x00\\x0B'::BLOB",
            "NULL",
        ],
        duckdb_filter_literals=[
            "'\\x01\\x02\\x03'::BLOB",
            "'\\x02\\x03\\x04'::BLOB",
            "'\\x03\\x04\\x05'::BLOB",
            "'\\x04\\x05\\x06'::BLOB",
            "'\\x05\\x06\\x07'::BLOB",
            "'\\x06\\x07\\x08'::BLOB",
            "'\\x07\\x08\\x09'::BLOB",
            "'\\x08\\x09\\x0A'::BLOB",
            "'\\x09\\x00\\x0A'::BLOB",
            "'\\x0A\\x00\\x0B'::BLOB",
        ],
        spark_expected_rows=_expected_rows("value", TRUNCATE_BINARY_VALUES),
        spark_filter_sql="SELECT * FROM default.truncate_partitioned_binary_for_insert WHERE value = X'010203' ORDER BY id",
        spark_filter_expected_rows=[
            Row(id=1, value=bytearray(b"\x01\x02\x03")),
            Row(id=101, value=bytearray(b"\x01\x02\x03")),
        ],
    ),
    PartitionedInsertCase(
        table_name="truncate_partitioned_decimal_for_insert",
        column_name="amount",
        spark_seed=_build_seed(
            "truncate_partitioned_decimal_for_insert",
            "amount",
            "DECIMAL(10, 2)",
            "truncate(amount, 100)",
            [str(value) for value in TRUNCATE_DECIMAL_VALUES[:-1]] + ["NULL"],
        ),
        duckdb_insert_literals=[f"{value}::DECIMAL(10,2)" for value in TRUNCATE_DECIMAL_VALUES[:-1]] + ["NULL"],
        duckdb_filter_literals=[f"{value}::DECIMAL(10,2)" for value in TRUNCATE_DECIMAL_VALUES[:-1]],
        spark_expected_rows=_expected_rows("amount", TRUNCATE_DECIMAL_VALUES),
        spark_filter_sql="SELECT * FROM default.truncate_partitioned_decimal_for_insert WHERE amount = 1.00 ORDER BY id",
        spark_filter_expected_rows=[Row(id=1, amount=Decimal("1.00")), Row(id=101, amount=Decimal("1.00"))],
    ),
]


def _partitioned_case_param(case: PartitionedInsertCase):
    return pytest.param(case, marks=pytest.mark.spark_seed_tables(case.spark_seed), id=case.table_name)


class TestPartitionedInsertsEndToEnd:
    @pytest.mark.parametrize("case", [_partitioned_case_param(case) for case in PARTITIONED_INSERT_CASES])
    def test_partitioned_insert_end_to_end(
        self,
        case,
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
            if case.requires_icu:
                test.send("require icu")
                test.statement_ok("SET Calendar='gregorian'")
                test.statement_ok("SET TimeZone='UTC'")

            test.query("I", f"SELECT count(*) FROM {case.catalog_table_name}", [(11,)])
            test.statement_ok(case.duckdb_insert_sql)
            test.query("I", f"SELECT count(*) FROM {case.catalog_table_name}", [(22,)])

            for literal in case.duckdb_filter_literals:
                test.query(
                    "I",
                    f"SELECT count(*) FROM {case.catalog_table_name} WHERE {case.column_name} = {literal}",
                    [(2,)],
                )
            test.query("I", f"SELECT count(*) FROM {case.catalog_table_name} WHERE {case.column_name} IS NULL", [(2,)])

        catalog_connection.refresh_table(case.qualified_table_name)
        total_rows = catalog_connection.con.sql(f"SELECT * FROM {case.qualified_table_name} ORDER BY id").collect()
        assert total_rows == case.spark_expected_rows

        filtered_rows = catalog_connection.con.sql(case.spark_filter_sql).collect()
        assert filtered_rows == case.spark_filter_expected_rows
