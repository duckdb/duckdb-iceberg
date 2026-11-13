# Metadata-only DELETE benchmarks

Extracted from [PR #1287](https://github.com/duckdb/duckdb-iceberg/pull/1287),
revision `8f2fa03ff18d01100ac5cb44d5021d3745ff57da`, so the workloads can land
before the optimization and measure its effect when it lands.

| Benchmark | Workload | Deleted rows | Surviving rows |
| --- | --- | ---: | ---: |
| `metadata_only_delete_whole_partition` | Delete one of eight days, with 500,000 rows per day | 500,000 | 3,500,000 |
| `metadata_only_delete_many_partitions` | Delete 127 of 128 days, with 1,000 rows per day | 127,000 | 1,000 |
| `metadata_only_delete_partial_partition` | Delete the first 72,500 seconds of one day, split into 1,000-row files | 72,500 | 13,900 |
| `scan_after_metadata_only_delete` | Count rows after deleting 64 of 128 days | 64,000 | 64,000 |
| `row_level_delete_uncovered_predicate` | Delete one row per day using a non-partition column | 128 | 127,872 |

All five use format-version 2 tables and run with either row-level or
metadata-only DELETE. Checks assert the affected/returned count, the number of
surviving rows, and the absence of rows matching the deleted predicate. They do
not depend on an EXPLAIN label, log message, manifest layout, or delete-file count
introduced by the optimization. `result_query` performs these checks outside
the timed query.

The four timed DELETE workloads use `require_reinit` to recreate their input
before every measured run, outside the timer. The scan workload performs its
DELETE during `load`, separately for each binary, and then repeats the same
read. Do not share an already-deleted table between versions: the physical
layout produced by each DELETE is what the scan benchmark compares.

The partial-partition case has two partition fields, `day(ts)` and
`truncate(1000, id)`. All files share the same day, but their timestamp bounds
differ. The cutoff fully covers 72 files, partly covers one, and excludes 14.
The control removes only one row from each file, so it continues to exercise
row-level deletes even with the optimization.

## Running and comparing

Use the fixture catalog, as in the other file-pruning benchmarks. If it is
already running, reuse it. Otherwise, `make fixture` starts it; these workloads
generate their own tables and do not need `make fixture-data`.

Build a benchmark runner in each checkout, with `VCPKG_TOOLCHAIN_PATH` set:

```sh
BUILD_BENCHMARK=1 EQUALITY_DELETE_WRITES_ENABLED=1 make release
```

The equality-delete build flag matches the existing file-pruning regression
group; these five workloads themselves write positional deletes.

Run the extracted benchmarks serially from the checkout containing them:

```sh
./build/release/benchmark/benchmark_runner \
  'benchmark/file_pruning_benchmarks/(metadata_only_delete_.*|scan_after_metadata_only_delete|row_level_delete_uncovered_predicate).benchmark' \
  --threads=2
```

For a before/after comparison, put the same benchmark definitions in both
checkouts. Use the same DuckDB revision, build configuration, and thread count.
Run each binary's own setup and all benchmark processes sequentially: they
recreate the same named tables in the shared fixture catalog.

The existing file-pruning step in `.github/workflows/Regression.yml` discovers
these `.benchmark` files automatically. No new CI step or committed CSV is
needed. Land the benchmarks first, then include that commit in the optimization
branch so its merge base contains the workloads.
