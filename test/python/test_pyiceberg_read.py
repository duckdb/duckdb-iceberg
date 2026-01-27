import pytest
import os
import datetime
from decimal import Decimal
from math import inf

pyice = pytest.importorskip("pyiceberg")
pa = pytest.importorskip("pyarrow")
pyice_rest = pytest.importorskip("pyiceberg.catalog.rest")


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
    print("Token:", access_token)
    bearer_token.cached_token = access_token
    return access_token


@pytest.fixture()
def rest_catalog(bearer_token):
    catalog = pyice_rest.RestCatalog(
        "rest",
        **{
            "uri": "http://127.0.0.1:8181",
            "token": bearer_token,
            "warehouse": '',
            "s3.endpoint": "http://127.0.0.1:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.path-style-access": "true",
            "s3.ssl.enabled": "false",
        },
    )
    return catalog


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None, reason="Test data wasn't generated, run 'make data' first"
)
class TestPyIcebergRead:
    def test_pyiceberg_read(self, rest_catalog):
        table = rest_catalog.load_table("default.insert_test")
        arrow_table: pa.Table = table.scan().to_arrow()
        res = arrow_table.to_pylist()
        assert len(res) == 6
        assert res == [
            {'col1': datetime.date(2010, 6, 11), 'col2': 42, 'col3': 'test'},
            {'col1': datetime.date(2020, 8, 12), 'col2': 45345, 'col3': 'inserted by con1'},
            {'col1': datetime.date(2020, 8, 13), 'col2': 1, 'col3': 'insert 1'},
            {'col1': datetime.date(2020, 8, 14), 'col2': 2, 'col3': 'insert 2'},
            {'col1': datetime.date(2020, 8, 15), 'col2': 3, 'col3': 'insert 3'},
            {'col1': datetime.date(2020, 8, 16), 'col2': 4, 'col3': 'insert 4'},
        ]


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None, reason="Test data wasn't generated, run 'make data' first"
)
class TestPyIcebergRead:
    def test_pyiceberg_read(self, rest_catalog):
        table = rest_catalog.load_table("default.duckdb_deletes_for_other_engines")
        arrow_table: pa.Table = table.scan().to_arrow()
        res = arrow_table.to_pylist()
        assert len(res) == 10
        assert res == [
            {'a': 1},
            {'a': 3},
            {'a': 5},
            {'a': 7},
            {'a': 9},
            {'a': 51},
            {'a': 53},
            {'a': 55},
            {'a': 57},
            {'a': 59},
        ]


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None, reason="Test data wasn't generated, run 'make data' first"
)
class TestPyIcebergRead:
    def test_pyiceberg_read(self, rest_catalog):
        table = rest_catalog.load_table("default.duckdb_updates_for_other_engines")
        arrow_table: pa.Table = table.scan().to_arrow()
        res = arrow_table.to_pylist()
        assert len(res) == 20
        assert res == [
            {'a': 1},
            {'a': 3},
            {'a': 5},
            {'a': 7},
            {'a': 9},
            {'a': 51},
            {'a': 53},
            {'a': 55},
            {'a': 57},
            {'a': 59},
            {'a': 100},
            {'a': 100},
            {'a': 100},
            {'a': 100},
            {'a': 100},
            {'a': 100},
            {'a': 100},
            {'a': 100},
            {'a': 100},
            {'a': 100},
        ]


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None, reason="Test data wasn't generated, run 'make data' first"
)
class TestPyIcebergRead:
    def test_pyiceberg_read(self, rest_catalog):
        tbl = rest_catalog.load_table("default.test_metadata_for_pyiceberg")
        scan = tbl.scan(row_filter=pyice.expressions.EqualTo("a", 350))
        # Collect the file paths Iceberg selects
        matched_files = [task.file.file_path for task in scan.plan_files()]
        # only 1 data file should match the filter
        assert len(matched_files) == 1


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None, reason="Test data wasn't generated, run 'make data' first"
)
class TestPyIcebergRead:
    def test_pyiceberg_read_duckdb_upper_lower_bounds(self, rest_catalog):
        tbl = rest_catalog.load_table("default.lower_upper_bounds_test")
        arrow_table: pa.Table = tbl.scan().to_arrow()
        res = arrow_table.to_pylist()
        assert len(res) == 3
        assert res == [
            {
                'int_type': -2147483648,
                'long_type': -9223372036854775808,
                'varchar_type': '',
                'bool_type': False,
                'float_type': -3.4028234663852886e38,
                'double_type': -1.7976931348623157e308,
                'decimal_type_18_3': Decimal('-9999999999999.999'),
                'date_type': datetime.date(1, 1, 1),
                'timestamp_type': datetime.datetime(1, 1, 1, 0, 0),
                'binary_type': b'',
            },
            {
                'int_type': 2147483647,
                'long_type': 9223372036854775807,
                'varchar_type': 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',
                'bool_type': True,
                'float_type': 3.4028234663852886e38,
                'double_type': 1.7976931348623157e308,
                'decimal_type_18_3': Decimal('9999999999999.999'),
                'date_type': datetime.date(9999, 12, 31),
                'timestamp_type': datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
                'binary_type': b'\xff\xff\xff\xff\xff\xff\xff\xff',
            },
            {
                'int_type': None,
                'long_type': None,
                'varchar_type': None,
                'bool_type': None,
                'float_type': None,
                'double_type': None,
                'decimal_type_18_3': None,
                'date_type': None,
                'timestamp_type': None,
                'binary_type': None,
            },
        ]


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None, reason="Test data wasn't generated, run 'make data' first"
)
class TestPyIcebergRead:
    def test_pyiceberg_read_duckdb_infinities(self, rest_catalog):
        tbl = rest_catalog.load_table("default.test_infinities")
        arrow_table: pa.Table = tbl.scan().to_arrow()
        res = arrow_table.to_pylist()
        assert len(res) == 2
        assert res == [{'float_type': inf, 'double_type': inf}, {'float_type': -inf, 'double_type': -inf}]


@pytest.mark.skipif(
    os.getenv('ICEBERG_SERVER_AVAILABLE', None) == None, reason="Test data wasn't generated, run 'make data' first"
)
class TestPyIcebergReadDuckDBNestedTypes:
    def test_pyiceberg_read_duckdb_nested_types(self, rest_catalog):
        tbl = rest_catalog.load_table("default.duckdb_nested_types")
        arrow_table: pa.Table = tbl.scan().to_arrow()
        res = arrow_table.to_pylist()
        assert len(res) == 1
        assert res == [
            {
                'id': 1,
                'name': 'Alice',
                'address': {'street': '123 Main St', 'city': 'Metropolis', 'zip': '12345'},
                'phone_numbers': ['123-456-7890', '987-654-3210'],
                'metadata': [('age', '30'), ('membership', 'gold')],
            }
        ]
