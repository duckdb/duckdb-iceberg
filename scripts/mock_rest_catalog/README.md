# Mock REST catalog

For tests without any external service, use
[`make test_mock_native_reldebug`](../../test/native_catalog/README.md).
The native backend runs inside DuckDB and shares this server's capability and
skip policy. This Python backend remains available for testing over real HTTP.

This test-only Python standard-library server exercises the real Iceberg HTTP client
and local Parquet/Avro writes without Docker, Spark, or an object store.

Run from the repository root with an existing extension-enabled build:

```sh
make mock
./build/reldebug/test/unittest \
  --test-config "$(scripts/catalog_test_config.sh)" \
  "test/sql/local/catalog_test_config_setup/catalog_agnostic/*" \
  "exclude:*.test_slow"
make mock-stop
```

`make mock` stops the active catalog, starts a fresh mock, and records `mock` in
`.catalogs/.active_catalog`. The server runs in the background and shares state
across normal unittest invocations until stopped. You can also pass
`--test-config test/configs/mock.json` directly or select an individual SQL test.
Run test files serially against the shared catalog.

## Makefile and CI entry points

The test target manages startup, serial SQL execution, and shutdown on success or
failure:

```sh
make test_mock_reldebug
make test_mock_reldebug \
  MOCK_TEST_FILTER='test/sql/local/catalog_test_config_setup/catalog_agnostic/alter/*'
```

`test_mock_release`, `test_mock_debug`, and `test_mock_relassert` select the other
build directories. `test_mock` accepts `MOCK_TEST_BINARY` for an already-built
unittest executable.
These targets follow `make mock`'s active-catalog switching behavior.

The distribution workflow uses the standard Make test targets to run the native
catalog suite and the delegated extension tests. It does not start the Python
mock. The service-backed mock suite runs in the dedicated Linux
`test-mock-catalog.yml` job, which invokes
`make test_mock_relassert` against the existing build artifact and retains failure
diagnostics. Catalog files run serially against one server.

This follows DuckLake's split between its
[distribution workflow](https://github.com/duckdb/ducklake/blob/main/.github/workflows/MainDistributionPipeline.yml)
and dedicated Linux
[catalog tests](https://github.com/duckdb/ducklake/blob/main/.github/workflows/Catalogs.yml).
The explicit `test_mock_*` targets remain available for local use, and
`SKIP_TESTS=1` skips them.

## Catalog behavior

[mock.json](../../test/configs/mock.json) owns the fixed loopback endpoint,
attachment SQL, capability flags, and skip policy. The launcher reads the endpoint
from that file and fails if the port is occupied. Python integration tests and
Spark data generation are outside this backend's scope.

Each start creates a fresh run directory under `.catalogs/mock/` containing the
warehouse, request journal and server log. `make mock-stop` shuts down the owned
server through a token-authenticated control endpoint, clears the active marker
if it names `mock`, and retains the run files. Restarting does not reload old
warehouse metadata. Large request/response journal payloads are truncated to
16 KiB; published metadata files remain complete.

The mock supports namespace operations and property updates, staged v2/v3 table
creation, create-time metadata updates, append/delete/overwrite/replace snapshots,
optimistic requirements, table loading/listing, renaming (including across
namespaces), and unregistering tables. Renames preserve the UUID, storage location,
and metadata file. Namespace property changes preserve unrelated keys and report
missing removals. Committed snapshots are retained and exposed through the
snapshot-history capability. Metadata files are immutable and completed before
publication. Unregistering leaves storage files intact.

Every published metadata version links its predecessor in `metadata-log`, including
metadata-only changes. Old files retain their original update timestamps and
contents, allowing transaction-start reconstruction of schema, layout, properties
and snapshots. The main snapshot log records new snapshots at their creation
timestamps and rollbacks at the time of the reference change. Time travel by
snapshot ID or timestamp and rollback to an ancestor snapshot are supported for
v2 and v3 tables. Historical files remain in the run directory; restarting still creates
a fresh catalog.

V3 creation initializes `next-row-id` to zero. Upgrading a v2 table does the same
without changing historical snapshots or their files. New v3 snapshots must carry
nonnegative 64-bit `first-row-id` and `added-rows` values, and their allocations
must not overlap previously reserved IDs. The cursor advances past the complete
reserved range, including gaps, and is retained across snapshot rollback.
Downgrades are rejected. V3 schemas retain defaults and extended types; v2 schemas
reject v3-only types and non-null defaults. Schema field IDs are collected only
from type definitions, never from default values. Initial field IDs are preserved
from the client; tests requiring the fixture catalog's different assignment order
are explicitly excluded in the config. Client cleanup of uncommitted local files
is allowed; dropping a table still unregisters it without purging storage.

Single- and multi-table commits use the same candidate validation and publication
path. The catalog lock covers requirement checks, metadata preparation, and the
publication of all changed table pointers. Every candidate is validated and every
metadata file is completed before any pointer changes; a conflict or write error
publishes none. A successful batch gives all its metadata versions the same update
timestamp. A failed write may leave unreferenced files in the run directory for
diagnosis. Staged creates are removed from staging only after successful publication.

The config enables atomic commit conflicts and multi-table commits. DuckDB still
rejects transactions that mix staged creation or rename/drop operations with
other table updates when it cannot represent them as one atomic REST request.
The SQL suite covers multi-table success/failure and concurrent append retries.

Views, purge, relocation, credentials, and server-side scan planning remain unsupported.
The SQL config controls exclusions and expected
unsupported errors; a passing suite does not imply support for skipped behavior.
The server reports unsupported/error counts on stop.

The `test-mock-catalog.yml` workflow runs the catalog-agnostic SQL suite through
`make test_mock_relassert` with the
Linux relassert build artifact, excluding `.test_slow`, and uploads `.catalogs/mock`
on failure. Real-catalog CI remains necessary for interoperability testing.

Nested ALTER tests exercise struct widening and collection-field add/drop/rename.
The expanded `alter_field_type.test` covers widening, rollback, and fresh
connections. A broader regression found an unresolved projection failure after
nested field-name reuse; its [handoff and reproducer](../../docs/handoffs/nested-alter-projection/README.md)
are kept outside the active suite pending investigation.
