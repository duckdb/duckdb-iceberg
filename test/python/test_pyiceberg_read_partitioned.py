import pytest
import os
import datetime
import uuid
from decimal import Decimal

pyice = pytest.importorskip("pyiceberg")
pa = pytest.importorskip("pyarrow")
pyice_rest = pytest.importorskip("pyiceberg.catalog.rest")

UTC = datetime.timezone.utc


@pytest.fixture()
def bearer_token():
    if hasattr(bearer_token, "cached_token"):
        return bearer_token.cached_token

    import requests

    CATALOG_HOST = "http://127.0.0.1:8181"

    CLIENT_ID = "admin"
    CLIENT_SECRET = "password"

    token_url = f"{CATALOG_HOST}/v1/oauth/tokens"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "PRINCIPAL_ROLE:ALL",
    }

    response = requests.post(token_url, data=payload)

    assert response.status_code == 200
    access_token = response.json().get("access_token")
    bearer_token.cached_token = access_token
    return access_token


@pytest.fixture()
def rest_catalog(bearer_token):
    catalog = pyice_rest.RestCatalog(
        "rest",
        **{
            "uri": "http://127.0.0.1:8181",
            "token": bearer_token,
            "warehouse": "",
            "s3.endpoint": "http://127.0.0.1:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.path-style-access": "true",
            "s3.ssl.enabled": "false",
        },
    )
    return catalog


requires_iceberg_server = pytest.mark.skipif(
    os.getenv("ICEBERG_SERVER_AVAILABLE", None) is None,
    reason="Test data wasn't generated, run tests in test/sql/local/irc first (and set 'export ICEBERG_SERVER_AVAILABLE=1')",
)

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


@requires_iceberg_server
class TestPyIcebergReadPartitioned:
    # ------------------------------------------------------------------ INT
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_int_format_version_2",
            "test_table_partitioned_by_int_format_version_3",
        ],
    )
    def test_int_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(INT_ROWS)

    # --------------------------------------------------------------- BIGINT
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_bigint_format_version_2",
            "test_table_partitioned_by_bigint_format_version_3",
        ],
    )
    def test_bigint_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(BIGINT_ROWS)

    # -------------------------------------------------------------- VARCHAR
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_varchar_format_version_2",
            "test_table_partitioned_by_varchar_format_version_3",
        ],
    )
    def test_varchar_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(VARCHAR_ROWS)

    # -------------------------------------------------------------- DECIMAL
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_decimal_format_version_2",
            "test_table_partitioned_by_decimal_format_version_3",
        ],
    )
    def test_decimal_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(DECIMAL_ROWS)

    # ---------------------------------------------------------------- FLOAT
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_float_format_version_2",
            "test_table_partitioned_by_float_format_version_3",
        ],
    )
    def test_float_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(FLOAT_ROWS)

    # --------------------------------------------------------------- DOUBLE
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_double_format_version_2",
        ],
    )
    def test_double_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(DOUBLE_ROWS)

    # ----------------------------------------------------------------- UUID
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_uuid_format_version_2",
            "test_table_partitioned_by_uuid_format_version_3",
        ],
    )
    def test_uuid_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(UUID_ROWS)

    # ----------------------------------------------------------------- TIME
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_time_format_version_2",
            "test_table_partitioned_by_time_format_version_3",
        ],
    )
    def test_time_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(TIME_ROWS)

    # ----------------------------------------------------------------- DATE
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_date_format_version_2",
            "test_table_partitioned_by_date_format_version_3",
            "test_table_partitioned_by_date_year_format_version_2",
            "test_table_partitioned_by_date_year_format_version_3",
            "test_table_partitioned_by_date_month_format_version_2",
            "test_table_partitioned_by_date_month_format_version_3",
            "test_table_partitioned_by_date_day_format_version_2",
            "test_table_partitioned_by_date_day_format_version_3",
        ],
    )
    def test_date_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(DATE_ROWS)

    # ------------------------------------------------------------ TIMESTAMP
    def test_timestamp_identity_v2(self, rest_catalog):
        table = rest_catalog.load_table("default.test_table_partitioned_by_timestamp_format_version_2")
        assert _collected(table.scan().to_arrow()) == _sorted(TIMESTAMP_V2_IDENTITY_ROWS)

    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_timestamp_format_version_3",
            "test_table_partitioned_by_timestamp_year_format_version_2",
            "test_table_partitioned_by_timestamp_year_format_version_3",
            "test_table_partitioned_by_timestamp_month_format_version_2",
            "test_table_partitioned_by_timestamp_month_format_version_3",
            "test_table_partitioned_by_timestamp_day_format_version_2",
            "test_table_partitioned_by_timestamp_day_format_version_3",
            "test_table_partitioned_by_timestamp_hour_format_version_2",
            "test_table_partitioned_by_timestamp_hour_format_version_3",
        ],
    )
    def test_timestamp_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(TIMESTAMP_ROWS)

    # --------------------------------------------------------- TIMESTAMPTZ
    @pytest.mark.parametrize(
        "table_name",
        [
            "test_table_partitioned_by_timestamptz_format_version_2",
            "test_table_partitioned_by_timestamptz_format_version_3",
            "test_table_partitioned_by_timestamptz_year_format_version_2",
            "test_table_partitioned_by_timestamptz_year_format_version_3",
            "test_table_partitioned_by_timestamptz_month_format_version_2",
            "test_table_partitioned_by_timestamptz_month_format_version_3",
            "test_table_partitioned_by_timestamptz_day_format_version_2",
            "test_table_partitioned_by_timestamptz_day_format_version_3",
            "test_table_partitioned_by_timestamptz_hour_format_version_2",
            "test_table_partitioned_by_timestamptz_hour_format_version_3",
        ],
    )
    def test_timestamptz_partitioned(self, rest_catalog, table_name):
        table = rest_catalog.load_table(f"default.{table_name}")
        assert _collected(table.scan().to_arrow()) == _sorted(TIMESTAMPTZ_ROWS)
