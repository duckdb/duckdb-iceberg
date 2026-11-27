import struct
from functools import partial

import pytest

from conftest import is_active_catalog
from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


JAVA_FIXTURE_ONLY = pytest.mark.skipif(
    not is_active_catalog("fixture"),
    reason="Registers statistics files through pyiceberg against the Java REST fixture",
)

THETA_SKETCH_BLOB_TYPE = "apache-datasketches-theta-v1"
PUFFIN_MAGIC = b"PFA1"


@pytest.fixture
def duckdb_runner(unittest_binary, unittest_test_config, print_unittest_stdin):
    return partial(
        DuckDBUnittestRunner,
        unittest_binary,
        test_config=unittest_test_config,
        print_stdin=print_unittest_stdin,
    )


def _drop_table_if_exists(catalog, identifier):
    if catalog.table_exists(identifier):
        catalog.drop_table(identifier)


def _set_partition_statistics(catalog, identifier, snapshot_id, path, file_size_in_bytes):
    """Register a partition statistics file with a plain REST commit: pyiceberg gained the update class in 0.12."""
    namespace, table = identifier.split(".")
    response = catalog._session.post(
        catalog.url("namespaces/{namespace}/tables/{table}", namespace=namespace, table=table),
        json={
            "requirements": [],
            "updates": [
                {
                    "action": "set-partition-statistics",
                    "partition-statistics": {
                        "snapshot-id": snapshot_id,
                        "statistics-path": path,
                        "file-size-in-bytes": file_size_in_bytes,
                    },
                }
            ],
        },
    )
    response.raise_for_status()


def _puffin_file(blobs):
    """A Puffin file: magic, the blobs, magic, the JSON footer, its size, flags, magic."""
    import json

    payload = b""
    blob_metadata = []
    for blob, metadata in blobs:
        blob_metadata.append({**metadata, "offset": len(PUFFIN_MAGIC) + len(payload), "length": len(blob)})
        payload += blob
    footer_payload = json.dumps({"blobs": blob_metadata}, separators=(",", ":")).encode()
    footer = PUFFIN_MAGIC + footer_payload + struct.pack("<iI", len(footer_payload), 0) + PUFFIN_MAGIC
    return PUFFIN_MAGIC + payload + footer, len(footer)


@JAVA_FIXTURE_ONLY
def test_statistics_functions_read_the_registered_files(rest_catalog, duckdb_runner, tmp_path):
    statistics = pytest.importorskip("pyiceberg.table.statistics")
    pq = pytest.importorskip("pyarrow.parquet")
    pa = pytest.importorskip("pyarrow")

    identifier = "default.statistics_functions"
    table_name = f"my_datalake.{identifier}"
    _drop_table_if_exists(rest_catalog, identifier)
    try:
        with duckdb_runner() as test:
            test.statement_ok(f"CREATE TABLE {table_name} (id INTEGER, region VARCHAR) PARTITIONED BY (region)")
            test.statement_ok(
                f"INSERT INTO {table_name} SELECT range, CASE WHEN range < 7 THEN 'eu' ELSE 'us' END FROM range(10)"
            )
            test.query("I", f"SELECT count(*) FROM iceberg_table_stats({table_name})", [[0]])
            test.query("I", f"SELECT count(*) FROM iceberg_partition_statistics({table_name})", [[0]])

        table = rest_catalog.load_table(identifier)
        snapshot_id = table.current_snapshot().snapshot_id
        region_field = table.schema().find_field("region").field_id

        # A table statistics file with one theta sketch blob for 'region'
        blob_metadata = {
            "type": THETA_SKETCH_BLOB_TYPE,
            "fields": [region_field],
            "snapshot-id": snapshot_id,
            "sequence-number": 1,
            "properties": {"ndv": "2"},
        }
        puffin, footer_size = _puffin_file([(b"\x01\x02\x03\x04", blob_metadata)])
        statistics_path = f"{table.location()}/metadata/{snapshot_id}.stats"
        with table.io.new_output(statistics_path).create() as output:
            output.write(puffin)
        table.update_statistics().set_statistics(
            statistics.StatisticsFile(
                snapshot_id=snapshot_id,
                statistics_path=statistics_path,
                file_size_in_bytes=len(puffin),
                file_footer_size_in_bytes=footer_size,
                blob_metadata=[
                    statistics.BlobMetadata(
                        type=THETA_SKETCH_BLOB_TYPE,
                        snapshot_id=snapshot_id,
                        sequence_number=1,
                        fields=[region_field],
                        properties={"ndv": "2"},
                    )
                ],
            )
        ).commit()

        # A partition statistics file with the spec's columns, one row per partition
        rows = pa.table(
            {
                "partition": [{"region": "eu"}, {"region": "us"}],
                "spec_id": pa.array([0, 0], pa.int32()),
                "data_record_count": pa.array([7, 3], pa.int64()),
                "data_file_count": pa.array([1, 1], pa.int32()),
                "total_data_file_size_in_bytes": pa.array([700, 300], pa.int64()),
                "position_delete_record_count": pa.array([0, 0], pa.int64()),
                "position_delete_file_count": pa.array([0, 0], pa.int32()),
                "equality_delete_record_count": pa.array([0, 0], pa.int64()),
                "equality_delete_file_count": pa.array([0, 0], pa.int32()),
                "total_record_count": pa.array([7, 3], pa.int64()),
                "last_updated_at": pa.array([1700000000000, 1700000000000], pa.int64()),
                "last_updated_snapshot_id": pa.array([snapshot_id, snapshot_id], pa.int64()),
                "dv_count": pa.array([0, 0], pa.int32()),
            }
        )
        local_file = tmp_path / "partition-stats.parquet"
        pq.write_table(rows, local_file)
        partition_statistics_path = f"{table.location()}/metadata/partition-stats-{snapshot_id}.parquet"
        with table.io.new_output(partition_statistics_path).create() as output:
            output.write(local_file.read_bytes())
        _set_partition_statistics(
            rest_catalog, identifier, snapshot_id, partition_statistics_path, local_file.stat().st_size
        )

        with duckdb_runner() as test:
            test.query(
                "IIIII",
                f"SELECT statistics_snapshot_id, blob_type, field_names, ndv, file_size_in_bytes FROM iceberg_table_stats({table_name})",
                [[snapshot_id, THETA_SKETCH_BLOB_TYPE, "[region]", 2, len(puffin)]],
            )
            test.query(
                "IIIII",
                f"""
                SELECT statistics_snapshot_id, partition, data_record_count, total_record_count, last_updated_at
                FROM iceberg_partition_statistics({table_name}) ORDER BY partition
                """,
                [
                    [snapshot_id, "{'region': eu}", 7, 7, "2023-11-14 22:13:20"],
                    [snapshot_id, "{'region': us}", 3, 3, "2023-11-14 22:13:20"],
                ],
            )
    finally:
        _drop_table_if_exists(rest_catalog, identifier)


NDV_TABLE = "default.spark_table_statistics"
NDV_SEED = SparkSeedTable(
    NDV_TABLE,
    f"""
    CREATE TABLE {NDV_TABLE} (id INT, region STRING, never_set BIGINT)
    USING iceberg TBLPROPERTIES ('format-version'='2');
    INSERT INTO {NDV_TABLE} VALUES (1, 'eu', NULL), (1, NULL, NULL), (2, 'eu', NULL);
    INSERT INTO {NDV_TABLE} VALUES (2, 'us', NULL), (3, 'us', NULL);
    """,
)


def _write_spark_statistics(spark, table, procedure):
    snapshots = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY parent_id NULLS FIRST").collect()
    assert len(snapshots) == 2
    files = []
    for (snapshot_id,) in snapshots:
        [(path,)] = spark.sql(
            f"CALL {spark.catalog.currentCatalog()}.system.{procedure}("
            f"table => '{table}', snapshot_id => {snapshot_id}L)"
        ).collect()
        files.append((snapshot_id, path))
    return files


@pytest.mark.spark_seed_tables(NDV_SEED)
def test_spark_table_statistics(spark_con, duckdb_runner):
    expected = []
    for snapshot_id, path in _write_spark_statistics(spark_con, NDV_TABLE, "compute_table_stats"):
        # These small sketches are exact. Count the source values at the same snapshot.
        counts = spark_con.sql(
            f"""SELECT count(DISTINCT id) AS id, count(DISTINCT region) AS region,
                       count(DISTINCT never_set) AS never_set
                FROM {NDV_TABLE} VERSION AS OF {snapshot_id}"""
        ).first()
        for name, count in counts.asDict().items():
            expected.append((snapshot_id, snapshot_id, path, THETA_SKETCH_BLOB_TYPE, name, count))

    with duckdb_runner() as test:
        test.query(
            "IITTTI",
            f"""
            SELECT statistics_snapshot_id, blob_snapshot_id, statistics_path, blob_type, field_names[1], ndv
            FROM iceberg_table_stats(my_datalake.{NDV_TABLE})
            ORDER BY statistics_snapshot_id, field_names[1]
            """,
            sorted(expected),
        )


def _partition_statistics_case(version, *requirements):
    table = f"default.spark_partition_statistics_v{version}"
    seed = SparkSeedTable(
        table,
        f"""
        CREATE TABLE {table} (id INT, region STRING, event_time TIMESTAMP)
        USING iceberg PARTITIONED BY (region, days(event_time))
        TBLPROPERTIES ('format-version'='{version}', 'write.delete.mode'='merge-on-read');
        INSERT INTO {table}
        SELECT /*+ COALESCE(1) */ * FROM VALUES
            (1, 'eu', TIMESTAMP '2024-01-01 01:00:00'),
            (2, 'eu', TIMESTAMP '2024-01-01 02:00:00'),
            (3, 'eu', TIMESTAMP '2024-01-02 01:00:00'),
            (4, NULL, CAST(NULL AS TIMESTAMP)) AS t(id, region, event_time);
        DELETE FROM {table} WHERE id = 2;
        """,
    )
    return pytest.param(table, version, marks=[pytest.mark.spark_seed_tables(seed), *requirements], id=f"v{version}")


def _spark_partition_counts(spark, table, snapshot_id, version):
    # Count live manifest entries independently of the partition statistics file.
    # V2 omits the deletion vector count. V3 counts vectors separately from position delete files.
    dv_count = "count_if(content = 1 AND file_format = 'PUFFIN')" if version == 3 else "NULL"
    return spark.sql(
        f"""
        SELECT partition.region, partition.event_time_day, spec_id,
               sum(IF(content = 0, record_count, 0)), count_if(content = 0),
               sum(IF(content = 0, file_size_in_bytes, 0)),
               sum(IF(content = 1, record_count, 0)),
               count_if(content = 1 AND file_format <> 'PUFFIN') AS position_delete_files,
               {dv_count} AS deletion_vectors
        FROM {table}.files VERSION AS OF {snapshot_id}
        GROUP BY partition, spec_id
        ORDER BY partition.region NULLS FIRST, partition.event_time_day NULLS FIRST
        """
    ).collect()


@pytest.mark.requires_spark(">=4.0")
@pytest.mark.parametrize(
    "table,version",
    [
        _partition_statistics_case(2),
        _partition_statistics_case(3, pytest.mark.requires_capabilities("format_v3")),
    ],
)
def test_spark_partition_statistics(table, version, spark_con, duckdb_runner):
    files = _write_spark_statistics(spark_con, table, "compute_partition_stats")
    with duckdb_runner() as test:
        for expected_deletes, (snapshot_id, path) in enumerate(files):
            partitions = _spark_partition_counts(spark_con, table, snapshot_id, version)
            assert len(partitions) == 3
            delete_count = "position_delete_files" if version == 2 else "deletion_vectors"
            assert sum(row[delete_count] for row in partitions) == expected_deletes
            test.query(
                "ITTTIIIIIII",
                f"""
                SELECT statistics_snapshot_id, statistics_path, partition.region, partition.event_time_day,
                       spec_id, data_record_count, data_file_count, total_data_file_size_in_bytes,
                       position_delete_record_count, position_delete_file_count, dv_count
                FROM iceberg_partition_statistics(my_datalake.{table}, snapshot_from_id => {snapshot_id})
                ORDER BY partition.region NULLS FIRST, partition.event_time_day NULLS FIRST
                """,
                [(snapshot_id, path, *partition) for partition in partitions],
            )
