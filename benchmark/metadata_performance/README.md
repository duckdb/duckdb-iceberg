# Metadata performance baselines

These workloads measure the implementation of issues
[#1654](https://github.com/duckdb/duckdb-iceberg/issues/1654) and
[#1721](https://github.com/duckdb/duckdb-iceberg/issues/1721). They do not implement
predicate reuse, a delete index, or parsed metadata/manifest caches.

The [delete candidate index results](delete-candidate-index-2026-11-11.md)
record step 3's candidate counts, cold/repeated timings, and validation.
The [native partition grouping follow-up](partition-delete-index-2026-11-11.md)
compares the composite-key map with `GroupedAggregateHashTable`.
The [parsed table metadata results](parsed-table-metadata-2026-11-11.md)
record step 4's retained-snapshot scaling, ownership rules, and validation.

## Local baseline runner

Build first. Use the repository's vcpkg toolchain and turn off coverage for timing:

```sh
VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake \
  GEN=ninja EXT_FLAGS='-DCOVERAGE=OFF -DBUILD_BENCHMARKS=ON' make reldebug

# Quick correctness/diagnostic run, followed by the measurement sizes.
python3 scripts/metadata_performance/run.py \
  --output .catalogs/metadata-reduced --samples 3
python3 scripts/metadata_performance/run.py --size regression \
  --output .catalogs/metadata-measurement --samples 5
```

The runner uses Python's standard library and the built CLI plus `iceberg_mock`.
It reads initialization from `test/configs/mock_native.json`. No Python DuckDB
wheel, Docker, active catalog switch, network service, or external credentials
are needed. Its immutable local warehouses and scripted LoadTable responses
remain under the new output directory. Output directories must not already exist.
Run processes serially; the runner compares `--threads 1 8` sequentially.

Select a named case with `--case narrow_padded_in`, a workload with
`--workload delete|manifest|snapshots`, and modes with `--modes staleness etag`.
Each selected case is independently initialized. `--case` filters measurement,
not data generation. `--build build/release` selects another built CLI.

For unreferenced partitioned deletes, use the separate runner below. It requires
`fastavro` from `scripts/requirements.txt` in addition to the CLI. It generates
8/64/512 composite partitions, four data and delete files per partition, and
NULL partition values. Before publishing its private fixtures, it removes the
optional `referenced_data_file` field values and updates manifest lengths while
preserving Avro schemas and metadata. This forces candidate lookup through the
partition index. Every query checks its result and a separate query checks index
builds, candidate visits, and predicate evaluations.

```sh
python3 scripts/metadata_performance/partition_delete_index.py \
  --output .catalogs/partition-before
# Rebuild the changed implementation, then reuse exactly the same files.
python3 scripts/metadata_performance/partition_delete_index.py \
  --source .catalogs/partition-before --output .catalogs/partition-after
```

The partition runner always measures one/eight threads and cold/warm modes
serially, with five samples by default. It saves cases, environment, samples,
profiles and diagnostic logs, and prints wall-time medians.

| Mode | Database/connection/attachment | File-byte cache | LoadTable response |
|---|---|---|---|
| `cold` | Fresh CLI process and database for every sample | Disabled | Initial HTTP 200; no parsed response |
| `warm` | Same connection and attachment after one untimed primer | Enabled, primed | Repeated HTTP 200 and response parsing |
| `staleness` | Same connection and attachment after one untimed primer | Enabled, primed | `MAX_TABLE_STALENESS '1 hour'`; already parsed response reused, no GET |
| `etag` | Same connection and attachment after one untimed primer | Enabled, primed | Scripted ETag then HTTP 304; already parsed response reused |

“Fresh” always means a new database here, never a second connection to an
existing database. OS page caches are uncontrolled in every mode; `cold` does
not claim a cold disk. Enabling the byte cache is not evidence of a parsed
manifest cache. Staleness and ETag reads now share parsed table metadata; they
still parse manifests. An ordinary HTTP 200 creates a new parsed metadata
generation. Normal native mock responses have no ETag: the harness explicitly
scripts one and leaves the native
provider available for every 304. A separate diagnostic query verifies the
expected GET/200/304 behavior without enabling logging in timing samples.

## Workload sizes and assertions

| Size | Months × INSERTs × rows/file | DELETEs × files affected | Data manifests | Live delete entries |
|---|---|---|---|---|
| `reduced` | 4 × 4 × 28 | 1 × 4 | 4 | 4 |
| `regression` | 20 × 12 × 100 | 2 × 40 | 12 | 80 |
| `full` | 20 × 150 × 1,000 | 10 × 80 | 150 | 800 |

Every size also builds a no-delete control. Manifest merging is explicitly
disabled, making the full workload's manifest count differ from the original
issue's 51 merged data manifests. Current main packs each DELETE's vectors into
one Puffin file; live delete-entry count and physical delete-file count are
asserted separately. Layout checks assert data files, physical rows, live delete
entries, data/delete manifests, Puffin files, and live rows before measurement.

The date queries preserve the original literal IN filter and dynamic semi-join
key filter: wide IN/BETWEEN/no date predicate, narrow IN/padded IN/BETWEEN.
Twenty-month cases use 560 wide dates and 44 narrow dates, padded to 550.
Each result is checked against an independent Python row oracle. In particular,
the narrow BETWEEN scan is not assumed to return the same intermediate rows as
the IN predicate. Individual profiles retain this difference.

For #1721, `regression` and `full` both use 1,000/5,000/25,000 one-row files in
one manifest, with 18 columns and a query selecting one file; and
1/10,000/35,000 synthetic retained snapshots referencing one unchanged manifest
list. The reduced sizes are 32 entries and 1/100 snapshots. File/manifest counts
and actual snapshot counts are checked before measurement. Synthetic history is
provided by scripted native LoadTable responses, not thousands of commits.
Synthetic snapshots retain the required operation summary but omit repeated
writer summary metrics, keeping requests within the native helper's 16 MiB limit.

## Diagnostics and artifacts

Enable the existing logger with `CALL enable_logging('Iceberg', storage='memory')`
and read `duckdb_logs()` after the query has completed and its planners have been
destroyed. Prepared statements or other owners can extend that lifetime. Filter
`type='Iceberg' AND message LIKE 'Iceberg metadata phase=%'`.

| Phase / scope | Counters | Meaning |
|---|---|---|
| `delete_planning` / `planner_view` | `scan_tasks`, `delete_candidate_visits`, `delete_predicate_evaluations` | One lifetime total per filtered planner view that produces tasks. Repeated visits count again. Predicate evaluations exclude the helper's no-filter fast return. Views sharing a provider keep separate totals. |
| `delete_candidate_index` / `manifest_load` or `server_provider` | `delete_index_builds` | Completed indexes published for selected delete manifests. Includes transaction-local manifests; excludes manifests rejected before loading. Views sharing a provider reuse these indexes. No cross-query index cache is implied. |
| `manifest_parse` / `avro_scan` | `manifest_parses`, `manifest_entries_converted` | One lifetime total per Avro manifest scan. Parses count initialized manifest readers; entries count successfully decoded chunks before pruning, including deleted entries. Data/delete and eager scans can emit separate events. In-memory transaction entries and manifest lists are excluded. Partial/failed scans can emit partial work. |
| `table_metadata_conversion` / `catalog_initialization` | `table_metadata_conversions`, `snapshots_converted` | One event after successful `FromTableMetadata` during catalog response initialization, including metadata-location overrides. This excludes standalone path reads, create-table constructors, and metadata-history reconstruction. |
| `table_metadata_cache` / `catalog_initialization` | `parsed_metadata_hits` | Reuse of the immutable payload belonging to the validated LoadTable response generation, through staleness or HTTP 304. Emits zero conversions. |
| `table_metadata_copy` / `table_mutation` | `table_metadata_copies`, `snapshots_copied` | A transaction writer detaches from immutable metadata or an outstanding scan owner. Ordinary reads, scan credentials, and transaction copies share owners instead. Commit-specific format copies are outside this counter. |

Candidate totals now count the path/partition buckets visited before provider,
transaction, sequence, predicate and bounds checks. The planner adds them
atomically once per data-file lookup; predicates count successful initialization
of per-view cache entries. Parsing uses one atomic addition per manifest/chunk.
Index builds, candidate visits and predicates measure separate work. Data-file
and manifest pruning still run even when a lookup finds no delete candidates.
There is no new public telemetry API or per-entry logging. Parsed manifests and
delete indexes remain scoped to their existing query owners; parsed table
metadata is shared across queries as described above.

Every output directory contains:

* `environment.json`: revisions, dirty diff, runtime extension versions, build
  configuration, CLI/harness hashes, process semantics, and run arguments.
* `layouts.json`, `cases.json`: checked sizes, expected results, exact SQL, and
  scripted metadata responses.
* `samples.json`, `*.profile.json`: each wall time, process CPU time from the CLI
  timer (user + system across threads), result, scan rows/files, and full profile.
* `*.logs.json`: separate diagnostic queries, including native protocol evidence.
* `summary.md`: medians; correctness never depends on a timing threshold.

The measured wall time includes the persistent CLI's pipe round trip and result
serialization, but excludes process startup, attachment, setup, and profiling
configuration. Raw profiles also retain engine timings. These are end-to-end
measurements, not isolated conversion CPU measurements.

## Regression infrastructure and correctness tests

`Regression.yml` sets up the practical 240-file/80-entry deletion case and the
1,000-entry manifest case after the fixture is generated, using the existing
Python/unittest conventions:

```sh
.venv-spark4/bin/python -m pytest -vv \
  test/python/benchmark/test_metadata_performance.py \
  --unittest-binary build/release/test/unittest --test-python-verbosity verbose
python duckdb/scripts/regression/test_runner.py \
  --old base/build/release/benchmark/benchmark_runner \
  --new build/release/benchmark/benchmark_runner \
  --benchmarks .github/regression/metadata_performance.csv --verbose --threads 2
```

The setup writes `.catalogs/metadata-performance-regression/load.benchmark` from
the existing catalog profile; benchmark definitions include it rather than
copying connection defaults. All eight benchmarks share the fixture's read-only
tables and use repeated queries with byte caching and normal catalog
revalidation. The existing regression runner preserves individual timing
samples. Synthetic snapshot timing and larger manifest cases stay opt-in.

Run the small regression and counter tests serially:

```sh
build/reldebug/test/unittest --test-config test/configs/mock_native.json \
  test/sql/local/catalog_test_config_setup/catalog_agnostic/delete/test_delete_planning_baseline.test
build/reldebug/test/unittest --test-config test/configs/mock_native.json \
  test/sql/local/native_catalog/metadata_diagnostics.test
build/reldebug/test/unittest --test-config test/configs/mock_native.json \
  'test/sql/local/native_catalog/load_table_etag*'
```

`scripts/metadata_performance/workloads.py` is the source for workload SQL and
expected results. After editing it, regenerate checked-in definitions with
`python3 scripts/metadata_performance/generate.py`, then `make format-fix`.
Large timings are never part of the ordinary sqllogictest suite.
