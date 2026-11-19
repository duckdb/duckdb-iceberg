import struct
from functools import partial

import pytest

from conftest import is_active_catalog
from duckdb_unittest import DuckDBUnittestRunner


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
