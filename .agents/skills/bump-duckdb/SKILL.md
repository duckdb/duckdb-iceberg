---
name: bump-duckdb
description: Bump DuckDB in duckdb-iceberg, or validate an existing bump, by comparing core revisions, running added and changed SQL tests against the fixture catalog, and updating justified skips in fixture_duckdb_tests.json.
---

# Bump DuckDB

Use the bundled [scripts/list_changed_tests.py](scripts/list_changed_tests.py) to select tests between two DuckDB commits. It emits sorted repository-relative paths for added, modified, renamed, and type-changed SQL tests. It includes `.test` and `.test_coverage`; `--include-slow` adds `.test_slow`. Deleted files and non-test files are excluded. Python 3.10+ and Git are the only dependencies.

## Establish the bump

Read the checkout's `AGENTS.md` and `duckdb/AGENTS.md`. Inspect the parent repository and both submodule statuses before changing revisions. When validating an already checked-out bump, use its existing target; do not advance it again.

Record the old and new **DuckDB commit IDs**, separately from the parent extension's commits. For an existing PR, obtain the old gitlink from the verified PR base, not `HEAD^`: follow-up fixes may be newer than the bump. For a new bump, capture the current gitlink before updating it. Resolve the requested target to an exact commit; clarify only if the intended release or branch is unknown.

For a new bump, check out the resolved target in `duckdb/` while preserving local submodule changes, then update the parent repository's applicable version references. Resolve and pin a compatible CI-tools revision when that update is part of the bump.

Inspect the core diff beyond test files for API/build changes. If `extension-ci-tools` is being bumped, inspect its diff too, especially reusable workflow inputs, build flags, and test commands. Keep its gitlink, `ci_tools_version`, and applicable `uses: duckdb/extension-ci-tools/...@...` references aligned. Find the live references in `.github/workflows/`; do not assume a previous pin or mechanically update unrelated dependency versions.

## Generate the test list

From the extension root, set `bump_skill_dir` to the directory containing this `SKILL.md` and set `bump_old_duckdb` / `bump_new_duckdb` to the resolved core commits. For a PR, the old ID can be obtained with `git rev-parse "${bump_base_extension_ref}:duckdb"`; the new ID is `git -C duckdb rev-parse HEAD` after the intended checkout.

```sh
bump_report_dir=$(mktemp -d "${TMPDIR:-/tmp}/duckdb-bump.XXXXXX")
python3 "$bump_skill_dir/scripts/list_changed_tests.py" \
  duckdb "$bump_old_duckdb" "$bump_new_duckdb" test/sql \
  > "$bump_report_dir/changed-tests.txt"
```

Check the helper's exit status before using its output. Save the resolved revisions with the report. The helper compares the two commits directly; it does not use a merge-base or include uncommitted edits. Ensure the checked-out core and test binary correspond to the target revision.

Read `.github/workflows/test-fixture-catalog-duckdb-tests.yml` for the current selection and per-test options. Its `test/sql/*` selection explicitly excludes `.test_slow`, matching the helper's default. Use `--include-slow` only when the requested scope includes them. Review changes to shared test inputs, includes, and helpers separately: a direct file diff cannot find all affected tests, so expand the selection where needed.

Keep an empty list as an explicit "no changed tests" result. **Never pass an empty input file to unittest**: it can fall back to the default suite. This result also does not validate other source changes in the bump.

## Prepare the fixture run

Use `test/configs/fixture_duckdb_tests.json`. `fixture.json` is for this extension's own catalog suite and does not substitute for the upstream-test config.

Read the root `Makefile`, `extension-ci-tools/makefiles/duckdb_extension.Makefile`, `make/util.mk`, `make/catalogs/fixture.mk`, and the fixture workflow before building or starting services. Check existing build freshness against the target core, extension changes, and build configuration. Rebuild stale artifacts through the repository's build targets with its vcpkg toolchain. A pre-existing binary is not evidence that it includes the bump; standard test targets do not build it first. Keep the local extension repository current if the build loads extensions from it.

Inspect `.catalogs/.active_catalog` and actual container health. Reuse a healthy fixture when available. These upstream tests create their own tables and need catalog/storage services only: use `make fixture` when setup is needed, not `make fixture-data`. The fixture target stops services and removes volumes; account for existing user data and the authorized scope before resetting a running catalog. Use the existing config and catalog Makefile for endpoints and credentials.

## Run and diagnose

Run from the extension root with `--test-dir duckdb`. List entries remain `test/sql/...`, without a `duckdb/` prefix. Run one unittest process at a time because these tests share catalog state.

Set `bump_unittest` to the verified binary. For a nonempty list with no per-test option overrides:

```sh
if test -s "$bump_report_dir/changed-tests.txt"; then
  "$bump_unittest" --order lex --test-dir duckdb \
    --input-file "$bump_report_dir/changed-tests.txt" \
    --test-config test/configs/fixture_duckdb_tests.json \
    --warn NoTests --emit-on-skip \
    > "$bump_report_dir/fixture-tests.log" 2>&1
fi
```

Preserve the runner exit status and inspect its assertions, failures, and skip summaries. For per-test details, use `--emit-test-events` if supported by the target runner. An event with `status: ok` can still accompany a `[SKIP_TEST]` caused by `skip_error_messages`; inspect both. Do not interpret zero selected tests, config exclusions, unmet requirements, or error-message skips as successful test execution.

Preserve the workflow's special invocation options. Currently `test/sql/alter/drop_col/test_drop_col_not_null.test` runs separately with `--sort-style rowsort`; if it appears in the delta, remove it from the general list and run it once with that override. Do not apply rowsort to the entire suite.

Use `--input-file` or invoke one exact path at a time. Do not pass a shell-expanded list as positional filters: some runner versions combine those arguments into a filter that selects no tests. Confirm the reported test names match the intended list.

For a failing case, inspect the old/new test diff and reproduce it individually with the same fixture config and CI options. Separate infrastructure/setup errors from Iceberg incompatibilities. Where useful, run the case against the same core binary without the fixture config to determine whether the expectation depends on native DuckDB storage. Fix regressions introduced by the bump when feasible; track unresolved failures explicitly.

## Update skips from findings

Edit only `test/configs/fixture_duckdb_tests.json` for fixture-specific upstream incompatibilities. Add the exact `test/sql/...` path to an existing group with the same concrete reason, or add a narrowly described group:

```json
{
  "reason": "Describe the observed unsupported operation or native-storage assumption",
  "paths": ["test/sql/category/affected_test.test"]
}
```

Base each new skip on reproduced evidence. A test being new, changed, or failing is not itself a reason to skip it. Do not add blanket directory skips, generic "new bump" groups, or broad `skip_error_messages` to make the run green. Do not modify upstream expectations to hide a fixture limitation.

Report changed tests already excluded by the config, including tests covered by existing error-message skips; those do not need redundant path entries. If their changes may remove the original limitation, retest them with only their relevant skip removed in a temporary diagnostic config. Remove obsolete skips after a successful run. Preserve other skip reasons and avoid duplicate paths; do not rewrite the whole config merely to add a finding.

After edits, run `jq empty test/configs/*.json`, repeat the affected cases and the complete selected list serially, and distinguish deliberately skipped cases from passes. Broaden to the relevant directory or the full fixture workflow when shared behavior changed or the remaining risk warrants it.

Finish with the old/new core and CI-tools revisions, the saved list and log locations, counts of executed/passed/skipped/failed tests, reasons for config changes, and any validation that remains incomplete.
