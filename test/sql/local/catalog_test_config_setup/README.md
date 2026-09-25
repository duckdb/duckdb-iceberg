# Catalog SQL tests

`catalog_agnostic/` runs against a started REST catalog without Spark or a data
generation step. Use the config for the active catalog, from the repository root:

```sh
TEST_CONFIG="$(bash -c 'source scripts/catalog_test_config.sh && active_catalog_test_config')"
./build/reldebug/test/unittest --test-config "$TEST_CONFIG" \
  'test/sql/local/catalog_test_config_setup/catalog_agnostic/*'
```

A single test can be selected by its full repository-relative path. Run catalog
tests serially: the tests share tables and storage, including setup includes.

## SQL fixtures

Small fixtures drop and recreate their own tables on each test run. Setup used by
one test is inline; shared setup lives in `catalog_agnostic/setup/*.test_setup`.
Their obsolete Spark generators have been removed.
Generators still needed by tests outside this suite remain. Include setup after
requirements and before enabling request logging or opening transactions, so setup
does not affect the assertions being measured.
Keep timestamps explicit about their UTC offset.

`catalog_agnostic/test_tpch.test` lazily builds SF1 data using
`dbgen(sf=1, suffix='_tpch_sf1')`. The cached tables live in `my_datalake.default`
with the same `_tpch_sf1` suffix and are reserved for immutable test data. The setup uses a
SQL variable containing a one-element boolean list, `foreach <variable:...>`, and
`onlyif` / `continue` to bypass generation when every table exists. If some tables
are missing, it generates local source data and creates only those missing tables.
Consumers must not modify this fixture; change the table suffix when changing
the data definition. The setup files are includes, not independently registered
test cases.

## Other-engine interoperability

`catalog_interop/` retains tests that compare Spark and DuckDB encodings, read
Spark-specific metadata, or prepare data for Spark/PyIceberg readers. These tests
remain part of the full `catalog_test_config_setup/*` selection used in CI. The
Spark-input tests still require their existing data generators:

```sh
make fixture-data
./build/reldebug/test/unittest --test-config test/configs/fixture.json \
  'test/sql/local/catalog_test_config_setup/catalog_interop/*'
```

Use the corresponding `*-data` target and config for another catalog. Existing
catalog-specific skip policies follow the relocated tests.

Python reader tests declare `@pytest.mark.duckdb_setup_tests(...)` with paths
relative to this directory. The fixture runs those SQL tests before the reader,
using `--unittest-binary` and the active catalog's config. Failures fail setup;
unsupported SQL setup skips the dependent reader. `once=True` is for shared
immutable inputs, such as the partition-type matrix, and reuses successful setup
within one pytest session. A method marker overrides a class marker.

A Spark -> DuckDB -> Spark test also declares `spark_seed_tables(...)`: the Spark
input is generated first, followed by DuckDB setup and the Python assertions.
These Python tests can run directly without an earlier SQL-suite run:

```sh
python3 -m pytest -vv test/python/test_spark_read.py test/python/test_pyiceberg_read.py \
  --unittest-binary ./build/reldebug/test/unittest
```
