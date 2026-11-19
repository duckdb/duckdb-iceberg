import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


def upgraded_table_seed(table: str, granularity: str) -> SparkSeedTable:
    # Two data files, then one Spark DELETE that removes rows from both, then an upgrade to v3
    return SparkSeedTable(
        table,
        f"""
        CREATE OR REPLACE TABLE {table} (id INT, data STRING)
        TBLPROPERTIES (
            'format-version'='2',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.delete.granularity'='{granularity}'
        );

        INSERT INTO {table}
        SELECT /*+ REPARTITION(1) */ * FROM VALUES
        (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e') AS t(id, data);

        INSERT INTO {table}
        SELECT /*+ REPARTITION(1) */ * FROM VALUES
        (11, 'k'), (12, 'l'), (13, 'm'), (14, 'n'), (15, 'o') AS t(id, data);

        DELETE FROM {table} WHERE id IN (1, 2, 15);

        ALTER TABLE {table} SET TBLPROPERTIES ('format-version'='3');
        """,
    )


FILE_SCOPED = "default.v3_upgrade_file_scoped_position_deletes"
PARTITION_SCOPED = "default.v3_upgrade_partition_scoped_position_deletes"


def spark_ids(catalog_connection, table: str) -> list[int]:
    catalog_connection.restart()
    return [row.id for row in catalog_connection.con.sql(f"SELECT id FROM {table} ORDER BY id").collect()]


class TestV3UpgradePositionDeletes:
    @pytest.mark.requires_spark(">=4.0")
    @pytest.mark.requires_capabilities("format_v3")
    @pytest.mark.spark_seed_tables(upgraded_table_seed(FILE_SCOPED, "file"))
    def test_dv_replaces_spark_file_scoped_delete(
        self,
        catalog_connection,
        unittest_binary,
        unittest_test_config,
        print_unittest_stdin,
    ):
        table = f"my_datalake.{FILE_SCOPED}"
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            # Spark sets no referenced_data_file, but each file's file_path bounds name one data file
            test.query(
                "II",
                f"""
                SELECT record_count, referenced_data_file IS NULL
                FROM iceberg_files({table}) WHERE content = 1 ORDER BY record_count
                """,
                [(1, True), (2, True)],
            )
            test.statement_ok(f"DELETE FROM {table} WHERE id = 3")
            test.query(
                "I",
                f"SELECT string_agg(id, ',' ORDER BY id) FROM {table}",
                [("4,5,11,12,13,14",)],
            )
            # The vector absorbs ids 1 and 2, so their file is removed. The file for id 15 stays.
            test.query(
                "III",
                f"""
                SELECT lower(file_format), status = 'DELETED', record_count
                FROM iceberg_metadata({table}) WHERE content = 'POSITION_DELETES'
                ORDER BY 1, record_count
                """,
                [("parquet", False, 1), ("parquet", True, 2), ("puffin", False, 3)],
            )
            test.query(
                "II",
                f"""
                SELECT summary['removed-position-delete-files'], summary['total-position-deletes']
                FROM iceberg_snapshots({table}) ORDER BY sequence_number DESC LIMIT 1
                """,
                [("1", "4")],
            )

        assert spark_ids(catalog_connection, FILE_SCOPED) == [4, 5, 11, 12, 13, 14]

    @pytest.mark.requires_spark(">=4.0")
    @pytest.mark.requires_capabilities("format_v3")
    @pytest.mark.spark_seed_tables(upgraded_table_seed(PARTITION_SCOPED, "partition"))
    def test_dv_keeps_spark_partition_scoped_delete(
        self,
        catalog_connection,
        unittest_binary,
        unittest_test_config,
        print_unittest_stdin,
    ):
        table = f"my_datalake.{PARTITION_SCOPED}"
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            # One Spark delete file covers both data files
            test.query(
                "II",
                f"""
                SELECT record_count, referenced_data_file IS NULL
                FROM iceberg_files({table}) WHERE content = 1
                """,
                [(3, True)],
            )

            # Only the first data file gets a vector, so the second one still applies id 15 from the shared file
            test.statement_ok(f"DELETE FROM {table} WHERE id = 3")
            test.query(
                "I",
                f"SELECT string_agg(id, ',' ORDER BY id) FROM {table}",
                [("4,5,11,12,13,14",)],
            )

            # Both data files have a vector now, and the shared file still stays live
            test.statement_ok(f"DELETE FROM {table} WHERE id = 12")
            test.query(
                "I",
                f"SELECT string_agg(id, ',' ORDER BY id) FROM {table}",
                [("4,5,11,13,14",)],
            )
            test.query(
                "III",
                f"""
                SELECT lower(file_format), status = 'DELETED', record_count
                FROM iceberg_metadata({table}) WHERE content = 'POSITION_DELETES'
                ORDER BY 1, record_count
                """,
                [("parquet", False, 3), ("puffin", False, 2), ("puffin", False, 3)],
            )

        assert spark_ids(catalog_connection, PARTITION_SCOPED) == [4, 5, 11, 13, 14]
