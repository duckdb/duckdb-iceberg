# Native REST test catalog

The native catalog implements the Python mock's REST contract in the separate
`iceberg_mock` test extension. It uses an `HTTPUtil` provider and `HTTPClient`, so
Iceberg still constructs requests, serializes commits, parses responses, logs
HTTP requests, and runs normal retry logic.
Requests to `http://iceberg-mock.invalid` never open a socket. Once enabled,
other HTTP origins are rejected unless explicitly registered with the optional
`origins` list in `iceberg_test_catalog_init`. Registered origins use the same
in-process catalog, allowing tests to exercise hostname-based catalog detection.
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

DuckDB's core CI registers `test/sql/local/native_catalog` with
`test/configs/mock_native.json` as its scoped `TEST_CONFIG`. It includes
`test/native_catalog/extension_config.cmake` to build the mock and builds Avro
alongside Iceberg in the cloud extension group. Core CI sets
`ICEBERG_MOCK_BUILD_LOADABLE=OFF` and links the mock statically into its test build,
so it is not published with the cloud extensions. Iceberg and its dependencies
are also linked to support the credential callback tests. These tests run through
the ordinary core test runner without invoking this repository's Makefile.
The config applies only to the registered native tests; other extensions keep
their own test setup. WebAssembly and the deployment autoloading checks do not
register the native suite.

The standalone API tests exercise independent database lifetimes and publication
failure after completing the first file of a multi-table commit:

```sh
cmake --build build/reldebug --target unittest_iceberg_native
./build/reldebug/test/unittest_iceberg_native
```

The separate scan serialization API executable links Iceberg and exercises its callbacks
directly, so optimizer fallback cannot hide a failed round trip:

```sh
./build/reldebug/test/unittest_iceberg_serialization
```

The CI build step also runs these serialization tests before packaging the SQL
test runner.

The LoadTable cache API tests deterministically interleave request construction,
publication, and eviction, and check the credential-refresh fallback. They also
cover expired Azure SAS credentials and S3 credential refresh after an HTTP 304,
without contacting cloud storage. Parsed metadata cases check response-generation
identity, concurrent initialization, owner release after replacement, and
copy-on-write isolation from readers. Authoritative-file fallback is retried, and
historical metadata-log reconstruction keeps a separate owner from the cached
head:

```sh
./build/reldebug/test/unittest_iceberg_cache
```

The CI build step runs these tests alongside the other API executables.

The planning API tests check borrowed and extracted filter lifetimes and filtered
views sharing a provider. They also check invalidation of cached missing paths.
Delete-index cases compare indexed and exhaustive applicability, cover null/NaN
and composite partitions, original paths and Puffin offsets, and drive concurrent
views. A gated filesystem holds a lazy manifest read until an overlapping caller
has registered, checking atomic publication, retained index ownership, and shared
I/O/cancellation errors. The equality-delete view cases require equality-delete writes:

```sh
./build/reldebug/test/unittest_iceberg_planning
```

Parsed-manifest cases check shared entry addresses, list-specific inheritance,
copy-on-write rewrites, key compatibility, eviction, interleaved misses and
failed publication without invalidating streamed references. Frozen partition
indexes are also probed after the creating connection closes.
`test/sql/local/native_catalog/parsed_manifest_cache.test` checks cold/warm
counters, selective then broader scans, dynamic filters, new snapshots,
rollback, ETag/304 and early LIMIT cleanup. The standalone
`test/sql/local/iceberg_scans/parsed_manifest_cache.test` covers warm v1 header
fallback and day partition values.

CI runs these tests in both the assertion build and the release build. The release
build enables equality-delete writes, so the shared-provider case executes there.

Run the ETag SQL tests with the native mock config:

```sh
./build/reldebug/test/unittest --test-config test/configs/mock_native.json \
  'test/sql/local/native_catalog/load_table_etag*'
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
with the same root can register additional origins; supplying another root is an
error. The root must be a local directory. Each database creates a unique child
directory containing its warehouse and a bounded-payload `requests.jsonl` journal. Metadata files
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
and `body`, plus an optional `headers` object. The path includes the query string.
The response replaces that route until overwritten or cleared with
`DELETE /__test/response`, which clears all
scripted responses. This permits config and auth tests without external servers;
the normal catalog model handles all other routes.

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
