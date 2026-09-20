# Running upstream DuckDB tests against the fixture

`fixture_duckdb_tests.json` requires a **dedicated, disposable REST catalog**.
It drops every visible table and view and every non-`main` schema before each
test. It also empties `main`. Do not use this config against the fixture holding
generated data or another test run. Run test processes serially.

The upstream-test CI starts a fresh fixture with `make fixture`, without Spark
data generation. Normal extension tests continue to use `fixture.json` and are
unaffected by this cleanup policy.

## Lifecycle

- `fixture_duckdb_attach.sql` runs through `init_script` when the runner creates
  a database, and on main-connection reconnect. It attaches the catalog,
  ensures `main` exists, and sets the existing timezone and format-version
  defaults. Nested namespace discovery is enabled so cleanup also finds dotted
  schemas and their tables. It never deletes remote objects.
- `on_new_connection` selects `my_datalake.main`, including for named connections
  sharing the database.
- `fixture_duckdb_reset.inc` runs through `init_sqllogic` once before each test
  body. It enumerates objects into SQL lists, quotes their identifiers, drops
  views and tables before schemas (children before parents), and selects the empty `main` schema.
  DuckDB protects `main` from being dropped, so cleanup retains that schema.

Cleanup runs at the start rather than relying on successful teardown. Thus a
failed test's leftovers are removed before the next test. Named connections,
`load`, and `restart` within the test do not trigger cleanup and can still read
its remote data. Cleanup deletes catalog objects; it is not an object-storage
vacuum and does not promise to purge every historical data file.

Using `main` instead of `{BASE_TEST_NAME}` matches the normal starting schema of
upstream tests. Catalog-wide cleanup also removes hardcoded schemas such as `s1`
and `tpch` that a per-test namespace cannot isolate.

This does not emulate independent remote databases for multiple local `load`
paths or named databases in the same test. It also does not fix unsupported
constraints, schema transactions, or differences in catalog metadata. Keep those
skips until their individual assertions pass.

## Configuration and validation

The config's `test_env` supplies `ICEBERG_ENDPOINT` and `S3_ENDPOINT` to the
attachment SQL. To validate on another disposable fixture, copy the config and
change those two entries. `S3_ENDPOINT` is `host:port`, without a URL scheme.
These are tester substitutions; process environment values do not override
explicit `test_env` entries. Keep the other entries, including the
`FIXTURE_DUCKDB_TESTS` marker.

Run from the repository root, with an already-built unittest binary:

```sh
./build/debug/test/unittest \
  --test-config test/configs/fixture_duckdb_tests.json \
  test/sql/local/catalog_custom_setup/fixture/upstream_test_isolation.test

./build/debug/test/unittest --order lex --test-dir duckdb \
  'test/sql/*' 'exclude:*.test_slow' \
  --test-config test/configs/fixture_duckdb_tests.json \
  --init-sqllogic "$PWD/test/configs/fixture_duckdb_reset.inc"
```

`--test-dir duckdb` changes the runtime working directory. The absolute
`--init-sqllogic` override keeps the reset script reachable; `init_script` SQL is
already read while loading the config, before that directory change.

The isolation regression checks quoted names, nested namespaces, removal of tables and empty
schemas, repeated cleanup with empty object lists, named connections, and data
surviving `load` and `restart`. It deliberately leaves a table behind, so a
subsequent test exercises cleanup of a previous test's state.

For skip audits, disable `skip_error_messages` in a temporary config as well as
removing the candidate's path from `skip_tests`. Otherwise an ignored error can
look like a successful test. Repeat passing candidates in one serial batch
before removing their skips.
