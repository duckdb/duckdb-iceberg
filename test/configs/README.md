# Catalog test capabilities

Catalog configurations declare sqllogictest capabilities in `test_env`. Tests
declare their requirements near the top of the file, before SQL or includes:

```text
require-env CATALOG_TEST_CONFIG_SETUP

require-env FORMAT_V3_SUPPORT

require-env UNKNOWN_SUPPORT
```

Each requirement must match. Capability flags use presence: declaring
`<FEATURE>_SUPPORT` means the configured catalog supports that feature; omitting
it means it does not. Tests require support with `require-env <FEATURE>_SUPPORT`.
Tests specifically exercising the absence of a capability can use:

```text
require-env-not FORMAT_V3_SUPPORT
```

Existing negative coverage uses this form: `alter_add_column_default_without_v3.test`
requires absence of `FORMAT_V3_SUPPORT`, and
`test_delete_consolidation_cleanup_failure.test` requires absence of
`FILE_CLEANUP_SUPPORT` together with presence of `FORMAT_V3_SUPPORT`.

The no-value exclusion runs only when the variable is absent. Keep the
`CATALOG_TEST_CONFIG_SETUP` requirement so missing catalog configuration does not
look like an unsupported feature.

Supported flags retain `"true"` as their config `env_value`, but the checks only
look for presence. Do not declare unsupported flags with `"false"`, `"unknown"`,
or an empty value: any defined value counts as support. Omit unverified
capabilities until their tests have been validated. Config `test_env` values take
precedence over ordinary process environment variables unless explicitly passed
through by the runner. Avoid setting capability flags in the process environment:
an omitted config entry does not mask a process environment variable.

Value-based requirements remain useful for modes: for example,
`require-env SCAN_PLANNING_MODE client server` accepts either value.
`require-env-not NAME value [value ...]` excludes the listed values and requires
the variable to exist, unlike the no-value form above.

These declarations describe the configured catalog version, credentials, and
execution mode, not every deployment of that catalog product. They preserve the
existing test policy; a declared flag is not a claim that a fresh cross-catalog
certification was performed.

## Flags

| Flag | Requirement |
| --- | --- |
| `FORMAT_V3_SUPPORT` | Create and commit format-version 3 tables. Individual V3 features may need additional flags below. |
| `UNKNOWN_SUPPORT` | Store Iceberg `unknown` columns and evolve them to concrete types. Does not specify whether a catalog rejects UNKNOWN in V2. |
| `VARIANT_SUPPORT` | Store VARIANT columns. Does not promise support for every nested writer representation. |
| `GEOMETRY_SUPPORT` | Store geometry columns and preserve their Iceberg type metadata. |
| `TIMESTAMP_NS_SUPPORT` | Store nanosecond timestamp and timestamptz columns, including schema evolution and partition transforms exercised by the tests. |
| `COLUMN_DEFAULT_SUPPORT` | Preserve and apply column defaults on creation and schema evolution. Specific default types may have additional requirements. |
| `TIMESTAMP_NS_DEFAULT_SUPPORT` | Preserve defaults on nanosecond timestamps without a time zone. |
| `TIMESTAMPTZ_NS_DEFAULT_SUPPORT` | Preserve defaults on nanosecond timestamps with a time zone. Kept separate because Polaris currently has only the former default-type exception. |
| `TIMETRAVEL_SUPPORT` | Resolve historical snapshots for version/timestamp reads, historical scan plans, and rollback. |
| `SNAPSHOT_HISTORY_SUPPORT` | Expose the committed snapshot history and its operation/summary metadata. Used by snapshot inspection tests that do not perform time-travel reads. |
| `METADATA_LOG_SUPPORT` | Return metadata-log entries needed to reconstruct table state at transaction start. Distinct from the snapshot list. |
| `METADATA_FILE_READ_SUPPORT` | Allow the client to read metadata JSON files using the configured storage credentials, including historical files. |
| `STAGED_CREATE_SUPPORT` | Support staging a table creation before its commit. |
| `MULTI_TABLE_COMMIT_SUPPORT` | Support the REST multi-table transaction commit endpoint. |
| `FILE_CLEANUP_SUPPORT` | Allow deletion of uncommitted files using the configured credentials. |
| `NAMESPACE_PROPERTIES_SUPPORT` | Support setting and removing namespace properties. Does not require an automatically populated `namespace_id`. |
| `CASE_SENSITIVE_TABLE_NAMES_SUPPORT` | Support distinct table names differing only in case. |
| `ATOMIC_COMMIT_CONFLICT_SUPPORT` | Reject concurrent commits with stale parent state so retries preserve every successful write. Declared for Lakekeeper and S3 Tables; omitted for the standard fixture, which lacks support, and for the other configurations pending validation. |
| `SCAN_PLANNING_MODE` | `client` or `server`. Tests asserting client pruning logs or request counts require `client`; fixture-latest uses `server`. |

Transaction-start reconstruction can require both `METADATA_LOG_SUPPORT` and
`METADATA_FILE_READ_SUPPORT`. Do not label these tests as time travel merely
because they read an older transaction state. Tests using V3 types/defaults
declare the particular feature as well as format support where applicable.

## Absence coverage

`catalog_agnostic/capabilities/no_*.test` exercises configurations that omit a
capability. Each test keeps `require-env CATALOG_TEST_CONFIG_SETUP` and uses
`require-env-not <FEATURE>_SUPPORT` without a value. Ordinary control operations
must succeed before the unsupported operation is checked. Unexpected success
fails the test: verify the behavior and add the support declaration instead of
adding a catalog-name restriction or a skip.

The probes cover type creation, integer and nanosecond timestamp defaults,
metadata-log and snapshot history, historical snapshot reads, direct metadata
file access, namespace properties, case-sensitive table names, multi-table
commits, and non-staged table visibility/rollback. Existing tests cover V3/default
rejection and cleanup failure. Type-specific default probes require the type,
V3, and general default support; when these prerequisites are absent, the
corresponding broader absence probes apply instead.

Absence need not mean an error: metadata/history probes check missing history,
and the staging probe checks that a table is already remotely visible before
commit and survives rollback. Multi-table rejection also checks that rollback
preserves both tables.

`ATOMIC_COMMIT_CONFLICT_SUPPORT` remains an explicit coverage gap. A catalog
without atomic conflict detection can still serialize a particular run's writes;
asserting that a race must lose data would make a flaky negative test. The
existing concurrent commit test validates the positive guarantee. Catalogs
currently marked unverified need a controlled REST-level race before this
capability can be declared or its absence reproducibly tested.

## Maintaining the declarations

When adding a test, declare the capabilities it actually exercises. Keep
`CATALOG_TEST_CONFIG_SETUP` for catalog initialization and genuinely
catalog-specific tests; prefer capabilities over catalog-name allowlists in
`catalog_agnostic`. When a catalog gains support, update its flag and run the
affected tests. Review all configs when adding a flag, including
`fixture_duckdb_tests.json`, which can also run extension tests, and declare it
only where support is verified. Remove the declaration when support is absent.

Keep `skip_tests` for known bugs, unexplained failures, hardcoded fixture paths,
and unavailable generated data. These are not evidence of absent capabilities.
For example, the S3 Tables UNKNOWN skips describe acceptance behavior that
differs from test expectations, and Gravitino's namespace-property skips assume
an existing `namespace_id`. Neither justifies declaring the feature unsupported.
The existing server-planning bug exceptions also remain explicit skips.

Capability gates apply to whole files. Split tests when unrelated assertions
would otherwise lose useful coverage; in particular, client-planning assertions
can eventually be separated from result checks to run the latter in both modes.

Python capability declarations are independent and unchanged by this migration.
This document and the JSON flags govern sqllogictests only.

Validate JSON with `jq empty test/configs/*.json`, and run affected tests serially
with the active catalog config. Do not start or switch catalogs just to validate
documentation. Review changes to skip coverage as well as JSON syntax: a removed
path must have an equivalent capability gate (or be an obsolete path).
