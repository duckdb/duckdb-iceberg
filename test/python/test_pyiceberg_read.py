import pytest
import os
import datetime
from decimal import Decimal
from math import inf

pyice = pytest.importorskip("pyiceberg")
pa = pytest.importorskip("pyarrow")


@pytest.mark.skipif(
    os.getenv('EQUALITY_DELETE_WRITES_ENABLED', None) == None, reason="Equality deletes must be turned on for DuckDB"
)
class TestPyIcebergReadEqualityDeletes:
    @pytest.mark.skip(reason="PyIceberg does not support equality deletes")
    def test_pyiceberg_read_duckdb_equality_delete_with_deleted_column(self, rest_catalog):
        tbl = rest_catalog.load_table("default.equality_delete_table_1")
        arrow_table: pa.Table = tbl.scan().to_arrow()
        res = sorted(arrow_table.to_pylist(), key=lambda r: (r["a"], r["c"]))
        assert len(res) == 23
        assert res == [
            {"a": 0, "c": 0},
            {"a": 2, "c": 2},
            {"a": 3, "c": 3},
            {"a": 4, "c": 4},
            {"a": 5, "c": 5},
            {"a": 7, "c": 7},
            {"a": 8, "c": 8},
            {"a": 9, "c": 9},
            {"a": 10, "c": 0},
            {"a": 12, "c": 2},
            {"a": 13, "c": 3},
            {"a": 14, "c": 4},
            {"a": 15, "c": 5},
            {"a": 17, "c": 7},
            {"a": 18, "c": 8},
            {"a": 19, "c": 9},
            {"a": 20, "c": 0},
            {"a": 100, "c": 100},
            {"a": 101, "c": 101},
            {"a": 102, "c": 102},
            {"a": 103, "c": 103},
            {"a": 104, "c": 104},
            {"a": 105, "c": 105},
        ]

    @pytest.mark.skip(reason="PyIceberg does not support equality deletes")
    def test_pyiceberg_read_duckdb_equality_delete(self, rest_catalog):
        tbl = rest_catalog.load_table("default.equality_delete_table_test_multiple_equality_deletes")
        arrow_table: pa.Table = tbl.scan().to_arrow()
        res = sorted(arrow_table.to_pylist(), key=lambda r: (r["a"], r["c"]))
        assert len(res) == 23
        assert res == [
            {"a": 0, "b": 0, "c": 0},
            {"a": 2, "b": 1, "c": 1},
            {"a": 3, "b": 2, "c": 2},
            {"a": 4, "b": 3, "c": 3},
            {"a": 5, "b": 4, "c": 4},
            {"a": 7, "b": 0, "c": 5},
            {"a": 8, "b": 2, "c": 7},
            {"a": 9, "b": 3, "c": 8},
            {"a": 10, "b": 4, "c": 9},
            {"a": 12, "b": 0, "c": 0},
            {"a": 13, "b": 1, "c": 1},
            {"a": 14, "b": 2, "c": 2},
            {"a": 15, "b": 3, "c": 3},
            {"a": 17, "b": 4, "c": 4},
            {"a": 18, "b": 0, "c": 5},
            {"a": 19, "b": 2, "c": 7},
            {"a": 20, "b": 3, "c": 8},
        ]
