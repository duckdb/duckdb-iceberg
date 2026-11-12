# Positional-delete performance cases

These cases follow the catalog-independent `benchmark/scan_tasks` setup and use
the standard DuckDB benchmark runner. They read 8,388,608 rows with two BIGINT
columns and apply a Parquet positional-delete file. Cases cover 0.1% sparse,
50% alternating, 50% contiguous, 90% scattered, and empty delete sets, each with
one and eight execution threads.

Setup writes local Parquet files and constructs scan descriptors outside the
timer. The timed query reads and materializes deletes, reads the data, applies
the delete filter, and computes count and two sums. Expected results come from
the deterministic input arithmetic, independently of the delete implementation.
The empty case retains an empty delete descriptor as a control for bitmap work;
it is not a no-delete-descriptor scan.

```sh
BUILD_BENCHMARK=1 make release
./build/release/benchmark/benchmark_runner --root-dir "$PWD" \
  'benchmark/performance/positional_.*' --timed-runs 5
```

The runner verifies results and performs a warmup before timed repetitions.
Run cases serially: setup overwrites generated files in `duckdb_benchmark_data`.
Compare revisions with the same build settings, dependencies, worker counts,
and otherwise idle host, retaining all samples. The existing regression workflow
discovers `.benchmark` files in this directory and compares old/new runners.

These cases intentionally emphasize delete-processing CPU and memory. They use
warm local filesystem pages with DuckDB's external file cache disabled, and
exclude catalog/manifest planning. Use the existing TPC-H mutation benchmarks
and wide/remote scans to assess how these gains translate to other workloads.
