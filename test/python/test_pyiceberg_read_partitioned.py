import pytest
import os
import datetime
import uuid
from decimal import Decimal

from conftest import capability_param

pyice = pytest.importorskip("pyiceberg")
pa = pytest.importorskip("pyarrow")

UTC = datetime.timezone.utc

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_row(row):
    """Normalize a row dict for comparison.

    PyIceberg may return UUID values as bytes (fixed_size_binary(16)) or as
    uuid.UUID objects depending on the PyArrow version.  Normalise both to the
    canonical lowercase hyphenated string so assertions are version-agnostic.
    """
    result = {}
    for k, v in row.items():
        if isinstance(v, bytes) and len(v) == 16:
            result[k] = str(uuid.UUID(bytes=v))
        elif isinstance(v, uuid.UUID):
            result[k] = str(v)
        else:
            result[k] = v
    return result


def _sorted(rows):
    """Sort rows by (id, str(val)) for stable cross-type comparisons."""
    return sorted(rows, key=lambda r: (r["id"], str(r["val"])))


def _collected(arrow_table):
    """Normalise and sort rows from an Arrow table."""
    return _sorted([_normalize_row(r) for r in arrow_table.to_pylist()])


def _table_param(table_name, *requirements):
    return capability_param(table_name, *requirements, id=table_name)


def _load_table(rest_catalog, table_name):
    return rest_catalog.load_table(f"default.{table_name}")


# ---------------------------------------------------------------------------
# Expected rows
# ---------------------------------------------------------------------------

INT_ROWS = [
    {"id": 1, "val": 10},
    {"id": 2, "val": 20},
    {"id": 3, "val": 10},
    {"id": 4, "val": 30},
]

BIGINT_ROWS = [
    {"id": 1, "val": 1000000000},
    {"id": 2, "val": 2000000000},
    {"id": 3, "val": 1000000000},
    {"id": 4, "val": 3000000000},
]

VARCHAR_ROWS = [
    {"id": 1, "val": "apple"},
    {"id": 2, "val": "banana"},
    {"id": 3, "val": "apple"},
    {"id": 4, "val": "cherry"},
]

DECIMAL_ROWS = [
    {"id": 1, "val": Decimal("1.50")},
    {"id": 2, "val": Decimal("2.75")},
    {"id": 3, "val": Decimal("1.50")},
    {"id": 4, "val": Decimal("3.00")},
]

FLOAT_ROWS = [
    {"id": 1, "val": 1.0},
    {"id": 2, "val": 2.0},
    {"id": 3, "val": 1.0},
    {"id": 4, "val": 3.0},
]

DOUBLE_ROWS = [
    {"id": 1, "val": 1.0},
    {"id": 2, "val": 2.0},
    {"id": 3, "val": 1.0},
    {"id": 4, "val": 3.0},
]

# UUID values normalised to lowercase hyphenated strings (see _normalize_row).
UUID_ROWS = [
    {"id": 1, "val": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
    {"id": 2, "val": "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
    {"id": 3, "val": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
    {"id": 4, "val": "c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
]

# Iceberg TIME → Arrow time64(us) → Python datetime.time
TIME_ROWS = [
    {"id": 1, "val": datetime.time(8, 0, 0)},
    {"id": 2, "val": datetime.time(12, 30, 0)},
    {"id": 3, "val": datetime.time(8, 0, 0)},
    {"id": 4, "val": datetime.time(18, 45, 0)},
]

DATE_ROWS = [
    {"id": 1, "val": datetime.date(2020, 1, 15)},
    {"id": 2, "val": datetime.date(2021, 6, 20)},
    {"id": 3, "val": datetime.date(2022, 3, 10)},
    {"id": 4, "val": datetime.date(2020, 7, 4)},
]

TIMESTAMP_ROWS = [
    {"id": 1, "val": datetime.datetime(2020, 1, 15, 8, 30, 0)},
    {"id": 2, "val": datetime.datetime(2021, 6, 20, 14, 45, 0)},
    {"id": 3, "val": datetime.datetime(2022, 3, 10, 22, 15, 0)},
    {"id": 4, "val": datetime.datetime(2020, 1, 15, 10, 0, 0)},
]

# The v2 identity timestamp table was inserted with (1,…),(1,…),(3,…),(4,…)
TIMESTAMP_V2_IDENTITY_ROWS = [
    {"id": 1, "val": datetime.datetime(2020, 1, 15, 8, 30, 0)},
    {"id": 1, "val": datetime.datetime(2021, 6, 20, 14, 45, 0)},
    {"id": 3, "val": datetime.datetime(2022, 3, 10, 22, 15, 0)},
    {"id": 4, "val": datetime.datetime(2020, 1, 15, 10, 0, 0)},
]

# TIMESTAMPTZ: Arrow timestamp(us, tz='UTC') → timezone-aware datetime
TIMESTAMPTZ_ROWS = [
    {"id": 1, "val": datetime.datetime(2020, 1, 15, 8, 30, 0, tzinfo=UTC)},
    {"id": 2, "val": datetime.datetime(2021, 6, 20, 14, 45, 0, tzinfo=UTC)},
    {"id": 3, "val": datetime.datetime(2022, 3, 10, 22, 15, 0, tzinfo=UTC)},
    {"id": 4, "val": datetime.datetime(2020, 1, 15, 10, 0, 0, tzinfo=UTC)},
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


# All of these tables are generated together, so all of them need V3 as a result
@pytest.mark.requires_capabilities("format_v3")
class TestPyIcebergReadPartitioned:
    # ------------------------------------------------------------------ INT
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_int_format_version_2"),
            _table_param("test_table_partitioned_by_int_format_version_3", "format_v3"),
        ],
    )
    def test_int_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(INT_ROWS)

    # --------------------------------------------------------------- BIGINT
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_bigint_format_version_2"),
            _table_param("test_table_partitioned_by_bigint_format_version_3", "format_v3"),
        ],
    )
    def test_bigint_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(BIGINT_ROWS)

    # -------------------------------------------------------------- VARCHAR
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_varchar_format_version_2"),
            _table_param("test_table_partitioned_by_varchar_format_version_3", "format_v3"),
        ],
    )
    def test_varchar_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(VARCHAR_ROWS)

    # -------------------------------------------------------------- DECIMAL
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_decimal_format_version_2"),
            _table_param("test_table_partitioned_by_decimal_format_version_3", "format_v3"),
        ],
    )
    def test_decimal_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(DECIMAL_ROWS)

    # ---------------------------------------------------------------- FLOAT
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_float_format_version_2"),
            _table_param("test_table_partitioned_by_float_format_version_3", "format_v3"),
        ],
    )
    def test_float_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(FLOAT_ROWS)

    # --------------------------------------------------------------- DOUBLE
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_double_format_version_2"),
        ],
    )
    def test_double_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(DOUBLE_ROWS)

    # ----------------------------------------------------------------- UUID
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_uuid_format_version_2"),
            _table_param("test_table_partitioned_by_uuid_format_version_3", "format_v3"),
        ],
    )
    def test_uuid_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(UUID_ROWS)

    # ----------------------------------------------------------------- TIME
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_time_format_version_2"),
            _table_param("test_table_partitioned_by_time_format_version_3", "format_v3"),
        ],
    )
    def test_time_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(TIME_ROWS)

    # ----------------------------------------------------------------- DATE
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_date_format_version_2"),
            _table_param("test_table_partitioned_by_date_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_date_year_format_version_2"),
            _table_param("test_table_partitioned_by_date_year_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_date_month_format_version_2"),
            _table_param("test_table_partitioned_by_date_month_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_date_day_format_version_2"),
            _table_param("test_table_partitioned_by_date_day_format_version_3", "format_v3"),
        ],
    )
    def test_date_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(DATE_ROWS)

    # ------------------------------------------------------------ TIMESTAMP
    def test_timestamp_identity_v2(self, rest_catalog):
        table = rest_catalog.load_table("default.test_table_partitioned_by_timestamp_format_version_2")
        assert _collected(table.scan().to_arrow()) == _sorted(TIMESTAMP_V2_IDENTITY_ROWS)

    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_timestamp_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_timestamp_year_format_version_2"),
            _table_param("test_table_partitioned_by_timestamp_year_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_timestamp_month_format_version_2"),
            _table_param("test_table_partitioned_by_timestamp_month_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_timestamp_day_format_version_2"),
            _table_param("test_table_partitioned_by_timestamp_day_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_timestamp_hour_format_version_2"),
            _table_param("test_table_partitioned_by_timestamp_hour_format_version_3", "format_v3"),
        ],
    )
    def test_timestamp_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(TIMESTAMP_ROWS)

    # --------------------------------------------------------- TIMESTAMPTZ
    @pytest.mark.parametrize(
        "table_name",
        [
            _table_param("test_table_partitioned_by_timestamptz_format_version_2"),
            _table_param("test_table_partitioned_by_timestamptz_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_timestamptz_year_format_version_2"),
            _table_param("test_table_partitioned_by_timestamptz_year_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_timestamptz_month_format_version_2"),
            _table_param("test_table_partitioned_by_timestamptz_month_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_timestamptz_day_format_version_2"),
            _table_param("test_table_partitioned_by_timestamptz_day_format_version_3", "format_v3"),
            _table_param("test_table_partitioned_by_timestamptz_hour_format_version_2"),
            _table_param("test_table_partitioned_by_timestamptz_hour_format_version_3", "format_v3"),
        ],
    )
    def test_timestamptz_partitioned(self, rest_catalog, table_name):
        table = _load_table(rest_catalog, table_name)
        assert _collected(table.scan().to_arrow()) == _sorted(TIMESTAMPTZ_ROWS)
