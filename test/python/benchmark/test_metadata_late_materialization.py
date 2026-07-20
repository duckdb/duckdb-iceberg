from duckdb_unittest import DuckDBUnittestRunner


MANIFEST_COUNT = 256
ROWS_PER_MANIFEST = 4096
EXPECTED_ROWS = MANIFEST_COUNT * ROWS_PER_MANIFEST
TABLE_NAME = "my_datalake.default.metadata_late_materialization"


def test_setup_metadata_late_materialization_benchmark(
    catalog_profile,
    unittest_binary,
    unittest_test_config,
    print_unittest_stdin,
):
    assert catalog_profile.name in (
        "fixture",
        "fixture-latest",
    ), "The regression benchmarks read from a fixture catalog"

    with DuckDBUnittestRunner(
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    ) as test:
        test.statement_ok("SET threads = 1")
        test.statement_ok(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        test.statement_ok(
            f"""
            CREATE TABLE {TABLE_NAME} (
                partition_id INTEGER,
                payload BIGINT
            ) PARTITIONED BY (partition_id) WITH (
                'format-version' = '2',
                'commit.manifest-merge.enabled' = 'false'
            )
            """
        )

        # Every statement is a separate DuckDB transaction and therefore creates one
        # controlled append, containing one partition and one small data file.
        for partition_id in range(MANIFEST_COUNT):
            first_value = partition_id * ROWS_PER_MANIFEST
            test.statement_ok(
                f"""
                INSERT INTO {TABLE_NAME}
                SELECT {partition_id}, range + {first_value}
                FROM range({ROWS_PER_MANIFEST})
                """
            )

        # Assert the physical shape on which the benchmark depends. This deliberately
        # checks exact counts so writer or manifest-merge changes cannot silently turn
        # the benchmark into a different workload.
        test.query(
            "IIIIIII",
            f"""
            SELECT
                count(*),
                count(DISTINCT manifest_path),
                count(DISTINCT manifest_sequence_number),
                count(DISTINCT file_path),
                min(record_count),
                max(record_count),
                sum(record_count)
            FROM iceberg_metadata({TABLE_NAME})
            WHERE manifest_content = 'DATA'
              AND content = 'DATA'
              AND status <> 'DELETED'
            """,
            [
                (
                    MANIFEST_COUNT,
                    MANIFEST_COUNT,
                    MANIFEST_COUNT,
                    MANIFEST_COUNT,
                    ROWS_PER_MANIFEST,
                    ROWS_PER_MANIFEST,
                    EXPECTED_ROWS,
                )
            ],
        )
        test.query(
            "I",
            f"""
            SELECT count(*)
            FROM (
                SELECT manifest_path
                FROM iceberg_metadata({TABLE_NAME})
                WHERE manifest_content = 'DATA'
                  AND content = 'DATA'
                  AND status <> 'DELETED'
                GROUP BY manifest_path
                HAVING count(*) <> 1
                    OR min(record_count) <> {ROWS_PER_MANIFEST}
                    OR max(record_count) <> {ROWS_PER_MANIFEST}
            )
            """,
            [(0,)],
        )
        test.query(
            "III",
            f"""
            WITH partition_sizes AS (
                SELECT
                    partition_id,
                    count(*) AS row_count,
                    min(payload) AS min_payload,
                    max(payload) AS max_payload
                FROM {TABLE_NAME}
                GROUP BY partition_id
            )
            SELECT
                count(*),
                sum(row_count),
                count(*) FILTER (
                    WHERE row_count <> {ROWS_PER_MANIFEST}
                       OR min_payload <> partition_id * {ROWS_PER_MANIFEST}
                       OR max_payload <> (partition_id + 1) * {ROWS_PER_MANIFEST} - 1
                )
            FROM partition_sizes
            """,
            [(MANIFEST_COUNT, EXPECTED_ROWS, 0)],
        )
