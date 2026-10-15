# Native REST test catalog

The native catalog implements the Python mock's REST contract in the separate
`iceberg_mock` test extension. It uses an `HTTPUtil` provider and `HTTPClient`, so
Iceberg still constructs requests, serializes commits, parses responses, logs
HTTP requests, and runs normal retry logic.
Requests to `http://iceberg-mock.invalid` never open a socket. Once enabled,
other HTTP origins are rejected.
HTTP filesystem sessions are rejected before HTTPFS consumes transport parameters;
warehouse reads and writes use local files.

The mock is built independently of Iceberg when `BUILD_UNITTESTS` is enabled
(except for WebAssembly). Its sources and SQL helpers are not included in either
Iceberg library. SQL tests load it from the build's local extension repository;
the API executable links the mock, HTTPFS, and DuckDB's static library without
linking Iceberg.

Build first, then run the standard test target:

```sh
make reldebug
make test_reldebug
```

After configuring a test build, the mock can also be rebuilt on its own:

```sh
cmake --build build/reldebug --target iceberg_mock_loadable_extension
```

The standard `test_release`, `test_debug`, `test_reldebug`, and `test_relassert`
targets run the native catalog suite before the delegated extension tests.
`make test` selects `test_release`. `SKIP_TESTS=1` skips both phases, and `T`
continues to select the delegated tests.

To run or focus only the native catalog suite:

```sh
make test_mock_native_reldebug \
  MOCK_NATIVE_TEST_FILTER='test/sql/local/catalog_test_config_setup/catalog_agnostic/transactions/*'
```

The target also runs the native protocol and lifecycle SQL tests. It uses
`test/configs/mock_native.json` directly and does not change
`.catalogs/.active_catalog` or start/stop services. The config inherits the Python
mock's capabilities and explicit exclusions through a relative `extends` entry.
Unlike the Python config, it does not skip unexpected unsupported errors.
Filters use repository-relative paths so each test runs once and the config's
relative skip paths also apply on Windows.

The standalone API tests exercise independent database lifetimes and publication
failure after completing the first file of a multi-table commit:

```sh
cmake --build build/reldebug --target unittest_iceberg_native
./build/reldebug/test/unittest_iceberg_native
```

The prefetch API tests verify background ATTACH discovery, overlapping requests,
cache publication, connection lifetime, and cancellation on DETACH:

```sh
cmake --build build/reldebug --target unittest_iceberg_prefetch
./build/reldebug/test/unittest_iceberg_prefetch
```

## SQL test interfaces

Load the test extension before using its SQL helpers. The native test config
does this automatically. These are test helpers, not a supported persistent
catalog:

```sql
LOAD iceberg_mock;

CALL iceberg_test_catalog_init('/tmp/my-native-catalog-test');

ATTACH '' AS my_datalake (
    TYPE ICEBERG,
    URI 'http://iceberg-mock.invalid',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none',
    STAGE_CREATE_TABLES true
);

SELECT status, body
FROM iceberg_test_catalog_request('GET', '/v1/config', NULL);
```

Initialization loads HTTPFS before installing the native provider. Repeating it
with the same root is a no-op; supplying another root is an error. The root must
be a local directory. Each database creates a unique child directory containing
its warehouse and a bounded-payload `requests.jsonl` journal. Metadata files
remain complete and immutable. The test runner owns cleanup.

All connections in one `DatabaseInstance` share catalog state, even after the
initializing connection closes. Different databases are isolated, including
when given the same root. Reopening a database starts an empty catalog; old files
are not reloaded. Do not change HTTP providers while using this test catalog.

The request helper accepts a method, an absolute REST path beginning with `/`,
and a JSON body string or SQL NULL. It returns `status INTEGER, body VARCHAR`.
GET, POST, HEAD, DELETE, PUT, and OPTIONS are accepted; unsupported routes return
the same REST error envelope as the server. Mutations occur during execution,
not during binding or EXPLAIN.

Client protocol tests can install an exact response with
`POST /__test/response`, supplying a JSON object with `method`, `path`, `status`,
and `body`. The path includes the query string. The response replaces that route
until overwritten or cleared with `DELETE /__test/response`, which clears all
scripted responses. This permits config and auth tests without external servers;
the normal catalog model handles all other routes.

Concurrency tests can add `"blocked": true` to a scripted response. The response
is captured and held without locking the catalog; `POST /__test/wait` with
`{"count": N}` waits for exactly N held responses. Replacing a route affects new
requests only. `DELETE /__test/response` releases all held responses, including
those from replaced routes. Both waits time out after ten seconds.

Supported behavior and exclusions follow the
[Python mock documentation](../../scripts/mock_rest_catalog/README.md).
The C++ model validates requirements against published metadata and applies
updates in request order. It completes every file in a batch before atomically
publishing table pointers. Failed batches may leave unreferenced files for
diagnosis but do not publish partial changes.

Publication crosses the request's starting millisecond when necessary. This keeps
metadata timestamps distinguishable from transactions that began before the
request, despite the in-process transport's speed. The clock remains real wall
time; commits are never assigned future timestamps.
