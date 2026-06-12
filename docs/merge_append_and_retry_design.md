# Design: MergeAppend strategy (#790) + Commit retries (#786)

> Status: Implemented (Phases A-F), compiling, and validated end-to-end against **AWS S3 Tables** and a
> local **Docker `apache/iceberg-rest-fixture` + MinIO** catalog, including merge/compression/delete
> and concurrent stress tests. This document is both the original design and the running review log:
> sections 0-9 are the design + pre-implementation correctness analysis (with inline `STATUS:`/
> `IMPLEMENTED` markers where the code resolved an item); section 10 records the real-catalog
> hardening, the connection-caching workaround, the SQLite-fixture concurrency limitation, and the
> performance investigation; section 11 records the final review round (correctness fixes,
> simplifications, and test additions/changes).
> Goal: A complete, retry-safe solution covering both DATA and DELETE manifest merging.
> Delivery: **Both features ship together in a single release / PR.** The phases in section 6 are internal milestones of one PR, not independent releases.
> Authoritative references: Apache Iceberg Java `SnapshotProducer` / `MergingSnapshotProducer` / `ManifestMergeManager`, and PyIceberg `_MergeAppendFiles` / `_ManifestMergeManager`.

---

## 0. Why the two issues are coupled

- **#790 (MergeAppend)**: Merge manifests to reduce the number of files and speed up scan planning.
- **#786 (Retry)**: On a commit conflict, refresh metadata, recompute, and retry instead of aborting immediately.

The two are tightly coupled and **must ship in one release**:

1. **Merge interacts with sequence-number materialization.** Merging rewrites manifests; if done naively it materializes sequence numbers on entries that should stay inherited, so a merged manifest would have to be rewritten on retry anyway (the code already carries a `RETRY_BLOCKER` comment). Correction A1 (section 8) states the precise rule: merged-in committed entries keep their historical sequence numbers, new entries stay inherited. Either way the regenerate-on-each-attempt model of retry is what makes merge retry-safe.
2. **Single-release removes the default-value hazard.** Because retry (Phase D) and merge (Phase B) go live at the same time, there is no transition window where "merge defaults to `true` but retry does not yet exist". This is precisely what lets us safely align the merge-enabled default with Java (`true`) -- see correction A2 in section 8.
3. **Retry is on by default from day one**, which makes the V3 concurrent-delete correctness backstop (problem B5 in section 8) a hard release requirement, not a follow-up.

Implementation order within the single PR: first build the retry framework (make `apply` a pure function + add the retry loop), then plug merging in as one step of `apply`. See section 6.

---

## 1. Current state of the code (factual baseline)

### 1.1 Commit flow
- `IcebergTransaction::Commit()` (`iceberg_transaction.cpp:390`) -> `DoTableUpdates` (`:443`) -> `GetTransactionRequest` (`:309`).
- `GetTransactionRequest` creates an `IcebergCommitState` per table, iterates `transaction_data.updates` calling `update->CreateUpdate(...)` (`:343`), and builds the REST `CommitTableRequest`.
- Finally `DoTableUpdates` calls `IRCAPI::CommitMultiTableUpdate` / `CommitTableUpdate` to send the request to the catalog.

### 1.2 Manifest / snapshot construction
- Core logic is in `IcebergAddSnapshot::CreateUpdate` (`iceberg_add_snapshot.cpp:163`):
  - `ConstructManifestList` (`:125`): handles existing manifests from the previous snapshot (introduced by #728: rewrites them if `altered_manifests` hits, otherwise carries them over as-is).
  - lines 212-226: adds each newly written manifest of this transaction (`manifest_files`) one by one via `AddNewManifestFile` into the new manifest list (**FastAppend**).
  - `manifest_list::WriteToFile` (`:228`) writes the manifest list file.
  - Pushes the new snapshot into `commit_state.created_snapshots` and produces the `add-snapshot` REST TableUpdate.

### 1.3 commit_state initialization (key for retry)
- `IcebergCommitState` ctor (`iceberg_table_update.cpp:7`):
  - `next_sequence_number = table_metadata.last_sequence_number + 1`
  - `next_row_id = table_metadata.next_row_id`
  - **These values come from the current metadata; on retry they must be recomputed from the refreshed metadata.**

### 1.4 Data file writes vs metadata writes (foundation of retry feasibility)
- **Parquet data files** are written during the operator Sink phase (`iceberg_insert.cpp` / `iceberg_delete.cpp` / `iceberg_copy.cpp`), independent of commit.
- **manifest / manifest_list / snapshot** are constructed during the `Commit()` phase.
- Therefore on **retry the data files can be reused; only the manifest/manifest_list/snapshot metadata must be rewritten** -- this is exactly why optimistic concurrency is feasible.

### 1.5 Current error handling
- `IRCAPI::CommitMultiTableUpdate` (`catalog_api.cpp:372`) / `CommitTableUpdate` (`:403`): any non-200/204 throws `InvalidConfigurationException`, **without distinguishing a 409 conflict from other errors, and with no retry**.

### 1.6 Data structures
- `IcebergManifestFile` (`iceberg_manifest_list.hpp:78`): contains `content` (DATA/DELETE), `partition_spec_id`, `manifest_length`, `sequence_number`, `min_sequence_number`, the various counts, and `first_row_id`.
- `IcebergManifestListEntry` (`:115`): `file` + `manifest_entries`.
- `IcebergManifestEntry` (`iceberg_manifest.hpp:122`): `status` (ADDED/EXISTING/DELETED) + `data_file`, with lazy-materialization accessors `Set/Get SnapshotId / SequenceNumber / FileSequenceNumber`.
- `IcebergManifestList` (`:131`): `AddNewManifestFile` (sets seq/snapshot_id), `AddExistingManifestFile` (as-is), `AddToManifestEntries`, `GetManifestListEntries`.
- `IcebergCommitState` (`iceberg_table_update.hpp:36`): `latest_snapshot` / `created_snapshots` / `next_sequence_number` / `next_row_id` / `manifests` (final manifest list to write) / `table_change` (REST request).
- `IcebergTableMetadata::GetTableProperty(string)` (`iceberg_table_metadata.cpp:485`): reads a table property.

---

## 2. Authoritative reference points (Java + Python)

### 2.1 Table properties and defaults (Java and PyIceberg do NOT fully agree)
| Property | Java default | PyIceberg default |
|---|---|---|
| `commit.manifest-merge.enabled` | **`true`** (`TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT = true`) | **`false`** (`MANIFEST_MERGE_ENABLED_DEFAULT = False`) |
| `commit.manifest.min-count-to-merge` | `100` | `100` |
| `commit.manifest.target-size-bytes` | `8388608` (8 MB) | `8388608` (8 MB) |
| `commit.retry.num-retries` | `4` (i.e. 5 attempts total) | n/a (PyIceberg has no retry loop) |
| `commit.retry.min-wait-ms` | `100` | n/a |
| `commit.retry.max-wait-ms` | `60000` (1 min) | n/a |
| `commit.retry.total-timeout-ms` | `1800000` (30 min) | n/a |
| `commit.status-check.num-retries` | `3` | n/a |
| `commit.status-check.min-wait-ms` | `1000` | n/a |
| `commit.status-check.max-wait-ms` | `60000` | n/a |
| `commit.status-check.total-timeout-ms` | `1800000` | n/a |
| `write.manifest.compression-codec` | `gzip` | n/a (uses `write.avro.compression-codec`, default `gzip`) |

> Correction (this used to claim both default to `true`): **Java defaults `merge-enabled` to `true`, PyIceberg defaults it to `false`.** They do not agree. Which default this project picks is therefore a real design decision, not "follow the consensus" -- see section 4 and 9.2. The min-count (100) and target-size (8 MB) values *do* agree between Java and PyIceberg.

### 2.2 Merge algorithm (Java `ManifestMergeManager` / PyIceberg `_ManifestMergeManager`)
1. Java merges `content==DATA` and `content==DELETE` manifests **separately** (it has both a `mergeManager` and a `deleteMergeManager`). **PyIceberg merges DATA only**: `_MergeAppendFiles._process_manifests` passes only DATA manifests to `_ManifestMergeManager` and returns DELETE manifests untouched. So "both merge DATA and DELETE" is wrong -- only Java does. This design follows Java (DATA+DELETE), which is stricter than PyIceberg; the extra DELETE handling carries the sequence-number risk in correction A1 and needs its own tests.
2. Group by `partition_spec_id`.
3. Within a group, first-fit bin-packing with weight = `manifest_length`, `target_weight = target-size-bytes`, `lookback=1`, `largest_bin_first=false`, packing from the tail (`pack_end`: reverse -> pack -> reverse).
4. Merge decision (per bin):
   - bin holds only 1 manifest -> not merged, kept as-is.
   - bin contains **any manifest tagged `NEW_THIS_TRANSACTION`** and has `< min-count-to-merge` manifests -> not merged (avoids repeatedly rewriting small new manifests together with large old ones). Note: a single commit can produce *several* new manifests (split by content type, by partition spec, or by a rolling writer), so the guard is "the bin contains at least one new manifest", not "the bin contains the single first manifest". Each input manifest carries an explicit `ManifestSource` tag (B18); the guard does not rely on a positional `new_manifest_count`.
   - otherwise -> merge the bin, writing a new manifest.
5. Entry status when writing the merged manifest: entries belonging to the current snapshot keep ADDED/DELETED; all other non-deleted entries are written as EXISTING (with their original sequence numbers -- see correction A1).

### 2.3 Retry mechanism (Java `SnapshotProducer.commit()`) -- PyIceberg has no retry, so Java is the sole reference
- `Tasks.foreach(...).retry(num).exponentialBackoff(min,max,total,2.0).onlyRetryOn(CommitFailedException).run(...)`.
- Each attempt:
  1. `refresh()` reloads the latest metadata into `base`.
  2. `sequenceNumber = base.nextSequenceNumber()`, `parentSnapshotId`, and `nextRowId` are all recomputed from the new base.
  3. `apply(base, parent)` regenerates the manifest list (including merging).
  4. Writes a **brand-new manifest-list file** (filename embeds the attempt number).
  5. Builds updates+requirements (the optimistic-concurrency assert-ref-snapshot-id) -> catalog commit.
- **snapshotId stays stable across attempts** (memoized; only re-rolled on collision with an existing id); **sequenceNumber / parent / rowId / all manifests are recomputed each attempt**.
- Exception classification:
  - Only `CommitFailedException` (409 conflict) is retried.
  - `CommitStateUnknownException`: rethrown as-is, **never clean up files** (the commit may have succeeded).
  - Other RuntimeExceptions (e.g. ValidationException): not retried, cleaned up when safe.
- Cleanup:
  - `cleanAll()` (hard failure): deletes every manifest-list this producer wrote + every uncommitted manifest.
  - `cleanUncommitted(committed)` (after success): deletes manifests not referenced by the winning snapshot, plus orphan manifest-lists left by failed attempts.
  - If the winning snapshot cannot be loaded (eventual consistency), cleanup is skipped.
- Status check on unknown outcome: Java has a second property family `commit.status-check.*` (num-retries=3, min-wait=1s, max-wait=60s, total-timeout=30min), implemented in `BaseMetastoreTableOperations.checkCommitStatus`. On an ambiguous failure it re-loads the table and checks whether the **new metadata-location it wrote** became the current location or appears in `previousFiles()` -- not by snapshot id. Our REST-based equivalent checks the per-table `snapshot_id` instead (we do not own metadata-file writing). See B13.

---

## 3. Implementation plan for this repository

### Phase A: Make "apply" a pure function (paves the way for retry, and gives merging a clean entry point)

**Goal**: Refactor "build manifest/manifest_list/snapshot from the current metadata state" into a repeatable pure function whose input is the refreshed state and whose output is the manifest list and snapshot.

**Changes**:
- In `IcebergAddSnapshot`, split `CreateUpdate` into:
  - `ApplyTableChanges(base, parent_snapshot, pending_changes, attempt_state) -> produces new_manifest_list (including merging) + snapshot`, a pure function callable repeatedly per attempt. It takes the **freshly refreshed** `base` metadata and parent snapshot as explicit arguments (C1), and never reads a captured `IcebergTableInformation` (B16).
  - The parts that write the manifest-list, build the snapshot, and generate the REST update are wrapped in an outer layer, invoked by the retry loop per attempt.
- `next_sequence_number` / `next_row_id` / `latest_snapshot` are derived from the refreshed `base` at the start of each attempt -- not mutated in place on a long-lived `commit_state` (the per-attempt `attempt_state` carries them; see Phase D and B15/B16).

**Note**: Currently multiple insert/update/delete operations in a transaction chain `CreateUpdate` (once per add_snapshot). #786 requires **bundling all data changes of a table into a single snapshot**. That is the focus of Phase C; Phase A keeps the existing "one snapshot per alter" semantics unchanged and only does the pure-function refactor, to lower risk.

### Phase B: Merge module (#790 main body, DATA + DELETE)

**New files**:
- `src/include/catalog/rest/api/iceberg_manifest_merge.hpp`
- `src/catalog/rest/api/iceberg_manifest_merge.cpp`

**Interface**:
```cpp
struct ManifestMergeConfig {
    bool enabled;                 // commit.manifest-merge.enabled (Java default true; final default per section 4/9.2)
    idx_t min_count_to_merge;     // commit.manifest.min-count-to-merge (default 100)
    int64_t target_size_bytes;    // commit.manifest.target-size-bytes (default 8MB)
    static ManifestMergeConfig FromTableMetadata(const IcebergTableMetadata &md);
};

enum class ManifestSource : uint8_t { NEW_THIS_TRANSACTION, CARRIED_OVER };

// Each input manifest is tagged with its origin so the min-count guard does not
// rely on positional assumptions (B18).
struct MergeInputManifest {
    IcebergManifestListEntry entry;   // file-level metadata; entries may be unloaded
    ManifestSource source;
};

// Pure logic: first-fit bin-packing by manifest_length; returns index groups per bin.
// Decision uses file-level metadata only, no entry reads (section 3 Phase B item 3).
vector<vector<idx_t>> BinPackManifests(const vector<int64_t> &lengths, int64_t target_weight);

// Result of merging one content type. For streamed (large) bins the entries are NOT
// returned materialized (B19); callers rely on file-level metadata + artifact paths.
struct MergeResult {
    vector<IcebergManifestListEntry> manifests;   // file-level metadata for the new manifest list
    vector<string> written_artifact_paths;        // for attempt cleanup (section 8-F)
};

MergeResult
MergeManifests(vector<MergeInputManifest> &&input,
               IcebergManifestContentType content,   // DATA and DELETE merged separately, never mixed
               const ManifestMergeConfig &config,
               CopyFunction &avro_copy, DatabaseInstance &db,
               IcebergCommitState &commit_state, int32_t schema_id);
```

**Merge execution logic** (aligned with Java/PyIceberg):
1. Group by `partition_spec_id`.
2. Per group, `BinPackManifests` (first-fit, weight = manifest_length, lookback=1, packing from the tail).
3. Apply the merge decision per bin per section 2.2 item 4. **Decide first, read later:** the manifest-list reader already provides `manifest_length` and `partition_spec_id` at the manifest-file level (`iceberg_manifest_list_reader.cpp:56-62`), so grouping, bin-packing and the min-count decision are made using only file-level metadata, without reading any entries. Only the bins that are actually going to be merged have their entries scanned. This matters because merge is enabled by default: scanning every existing manifest's entries on every commit (most of which will not merge under the default `min-count=100`) would be a severe regression.
4. Merge: for the bins selected in step 3, read the entries of each manifest and write a new manifest. **Two code paths (see 9.1):** a small bin uses a batched read (`ScanExistingManifestFile`, `iceberg_add_snapshot.cpp:39`) into a vector and `manifest_file::WriteToFile`; a large bin uses the streaming writer + running aggregator (9.1) so all entries are never held at once. Either way, recompute counts and (V3) first_row_id. **`min_sequence_number` is the merge author's responsibility:** set the merged manifest's `min_sequence_number` to `min(data_sequence_number of all its entries)` and `has_min_sequence_number=true` *before* handing it to `AddNewManifestFile`. If left unset, `AddNewManifestFile` (`iceberg_manifest_list.hpp:149`) will default it to the current (largest) sequence number, which makes scan planning's `seq > X` pruning believe the manifest holds no older data -> silently skips reading historical rows. When recomputing the manifest-level partition field summary, pass the **bin's own `partition_spec_id`**, not `table_metadata.default_spec_id` -- `ManifestPartitions::Create` defaults to `default_spec_id` (`iceberg_manifest_list.cpp:101`), which would be wrong for a merged manifest belonging to an older spec and could cause partition pruning to skip live data. (Grouping by spec, step 1, guarantees a single spec per bin.)
5. Entry status and sequence-number materialization (see correction A1 in section 8 -- this is subtle):
   - Entries pulled in from an **already-committed** manifest carry a determined historical `data_sequence_number` / `file_sequence_number`. Materializing those values is harmless for retry, because they will never change. Write them as `existing` with their original sequence numbers (mirroring Java's `ManifestWriter.existing`).
   - Entries that are **new in this transaction** (status `ADDED`) must stay un-materialized (sequence number NULL -> inherited from the manifest file at read time). They must NOT be demoted to `existing` by the merge, otherwise the spec forces their sequence number to be materialized and Phase C's retry immunity breaks (`WriteToFile` only allows NULL sequence numbers for `ADDED` entries, `iceberg_manifest.cpp:350-369`).
   - Entries already `DELETED` keep their status and sequence numbers.

**Entry point**: Inside Phase A's `ApplyTableChanges`, after writing this transaction's new manifests and after `ConstructManifestList` has carried over / rewritten the existing manifests, take the final manifest list and:
- Split into a DATA subset and a DELETE subset.
- Call `MergeManifests` on each.
- Reassemble the merged results into `new_manifest_list`.
- The snapshot's added stats are accumulated **before** merging (merging does not change logical added counts).

### Phase C: Single-snapshot bundling (#786 prerequisite)

**Goal**: Combine all insert/update/delete operations of a transaction into a **single** snapshot, and stop materializing the following fields (so manifest_entry is immune to retries):
- `manifest_entry.snapshot_id`
- `manifest_entry.sequence_number`
- `manifest_entry.file_sequence_number`
- `data_file.first_row_id`

**Changes**:
- `IcebergTransactionData` aggregates multiple `AddSnapshot`/`AddUpdateSnapshot` calls into one pending change set (new data files + delete files + altered_manifests). Critically, it stores the **raw** pending data/delete `IcebergManifestEntry`s and predicates, and does NOT pre-build manifest-list entries: today `AddSnapshot`/`AddUpdateSnapshot` eagerly call `IcebergManifestListEntry::CreateFromEntries` with a `bogus_snapshot_id` and a `temp_sequence_number` (`iceberg_transaction_data.cpp:101,133,137`). That eager construction bakes in attempt-specific values and is incompatible with retry. The actual manifest-list entries must be generated only inside each attempt's `ApplyTableChanges`, against the refreshed base.
- For UPDATEs targeting transaction-local (uncommitted) data: the `_row_id` inferred by the scan should be `NULL` for transaction-local data (explicitly required by #786), to avoid invalidation by inserts during the retry gap. This must be handled where row_id is read in `iceberg_update.cpp`.
- When writing manifests, no longer hard-code snapshot_id/sequence_number early (use the existing lazy-materialization accessors); inject the current attempt's values only when writing the manifest-list.

**Risk**: Touches the transaction core structure, with a large regression surface. Commit incrementally, validating each item together with Phase D's tests.

### Phase D: Retry loop (#786 main body)

**New**: Read `commit.retry.*` configuration + a retry driver.

**Change point**: Wrap a retry loop around `IcebergTransaction::DoTableUpdates` (or outside `GetTransactionRequest`). State is **per table** (B15), not a single snapshot id:
```
config = ReadRetryConfig(table_metadata)        // num-retries=4, backoff..., status-check.*
// stable per-table state, fixed across attempts:
for each table T in transaction: T.snapshot_id = NewSnapshotId()
for attempt in 0..=config.num_retries:
    RefreshAllTableMetadata()                    // reload latest metadata for every table (do NOT reuse cached existing_manifest_list, B16)
    for each table T:
        T.commit_state = rebuild from refreshed metadata (parent, next_seq, next_row_id)
        T.commit_state.RebindPendingChanges(refreshed T)   // B16: never read stale table_info
        ApplyTableChanges(T)                        // Phase A/B: regenerate manifests (incl. merge), write a fresh manifest-list (name embeds attempt)
        BuildUpdatesAndRequirements(T)           // refresh assert-ref-snapshot-id; abort if schema/spec moved (B7); validate delete/overwrite conflicts (B5/B8/B9)
    try:
        CommitToCatalog(all tables, one atomic request on the multi-table endpoint)
        UpdateLocalCacheFromResponse(...)        // C5: use the commit response's LoadTableResult, or expire+reload
        break                                    // success
    catch ConflictException (409):               // retry only on conflict
        CleanupThisAttemptArtifacts()            // manifests + manifest-lists this attempt wrote; NEVER data files (B11); NEVER merged-in historical sources (F)
        if last attempt: AbortTransaction(); rethrow
        sleep(exponential backoff + jitter)      // C3
    catch AmbiguousFailure (timeout/conn reset/5xx):   // state unknown
        status = StatusCheckLoop(all tables, config.status_check)   // B13: did our snapshot_id(s) land?
        if status == ALL_COMMITTED: resolve cleanup against the found snapshots (8-F); break
        else if status == NONE_FOUND and attempts remain:
            // The server provably did not accept our snapshot ids -> safe to treat as a fresh
            // attempt. Clean this attempt's manifests (never data files), then retry.
            CleanupThisAttemptArtifacts(); sleep(backoff + jitter)
        else:
            // Either partial (some tables' ids present -> catalog inconsistency) or
            // status-check budget exhausted -> we cannot prove success or failure.
            rethrow as CommitStateUnknown, deleting nothing (8-F)
    catch NonRetryable (validation/other):
        CleanupThisAttemptArtifacts(); AbortTransaction(); rethrow
```
Notes: data files are written once at Sink (1.4) and reused across all attempts. The snapshot id is stable per table; everything else (sequence number, parent, row-id range, manifests, manifest-list) is regenerated each attempt.

**Exception distinction**: In the commit functions in `catalog_api.cpp`, throw an **identifiable conflict exception type** (a new `IcebergCommitConflictException` or reuse DuckDB's `TransactionException`) based on HTTP 409 / the Iceberg error body's `CommitFailedException` type; the retry loop catches only that. Other status codes keep the existing exceptions.

**Cleanup**: Align with Java `cleanAll` / `cleanUncommitted`. Note that the existing `CleanupFiles` (`iceberg_transaction.cpp:645`) deletes data files; the retry scenario needs new logic to "delete the manifest/manifest-list written this attempt (not the data files, which are reused across attempts)", and must be skipped when the catalog disallows deletes (`attach_options.allows_deletes`).

---

## 4. Scope and boundaries

- **Merge scope**: DATA and DELETE manifests are merged **independently** (aligning with Java; PyIceberg only does DATA, but Java does both, so we take the more complete Java approach). No merging across partition_spec_id. No mixing of content.
- **DV (deletion vector, V3)**: Java has `mergeDVs()` to merge multiple DVs for the same referenced_data_file. This plan **does not implement DV merging in the first version** (marked TODO), because it is tightly coupled with concurrent-delete merging and the repo's V3 delete path is still evolving; DELETE manifest bin-packing merge is still done.
- **Retry scope**: Retry applies to `add-snapshot`-style data changes. **DDL-only** transactions (schema/rename/drop with no data write) are out of scope for retry and keep current behavior. A transaction that mixes **DDL + data** is not silently retried -- it is guarded per B12a (spec/sort-order change + data write fails fast on conflict) and B7 (other semantic guards abort rather than re-stamp).
- **Defaults**: `min-count-to-merge` (100) and `target-size-bytes` (8 MB) match both Java and PyIceberg and are adopted as-is. `merge-enabled` does **not** agree across references (Java `true`, PyIceberg `false`), so it is an explicit decision: the baseline is Java parity (`true`), with section 9.2 proposing a possibly-different DuckDB default only if a benchmark justifies it. Retry/status-check defaults follow Java. This is the single source of truth for defaults; section 9.2 is a candidate change to the `merge-enabled`/`min-count` defaults, gated on benchmarks, not a contradiction of this line.

---

## 5. Testing strategy (as built)

Three layers, mapped to what each can actually verify given the build/runtime constraints.

### 5.1 C++ logic unit tests -- `test/cpp/test_merge_retry_logic.cpp` (in CI, also locally runnable)
Run via `make test_logic` (wired into the `BuildIceberg` CI job, on every push/PR) or directly with a
single dependency-free command (only zlib):
`c++ -std=c++17 -lz test/cpp/test_merge_retry_logic.cpp -o /tmp/t && /tmp/t`.
It is standalone because the DuckDB `unittest` binary runs SQL logic tests and the extension is a
dynamically-loaded module, so production C++ functions cannot be linked into a standalone binary
without pulling in all of DuckDB. Each block reproduces one production function byte-for-byte and
names the file it mirrors, so a divergence shows up as a failing test in review. It covers
branch/boundary logic the SQL integration tests cannot reach. Covered:
- `BinPackManifests`: empty, single, all-fit, exact fill, cross-bin, lookback no-reorder, adjacent
  merge, no-loss/no-duplicate over a random weight set.
- `ShouldMergeBin`: single-manifest not merged, all-carried-over merges regardless of min, the
  new-manifest guard fires below min and clears at min.
- Retry backoff: exponential factor 2, clamp to max, min==max, INT64-overflow guard; **decorrelated
  jitter** (`DecorrelatedBackoffMs`): tight window early, widening, max clamp, overflow.
- **Retry config parsing** (`FromTableMetadata`): defaults; `num-retries=0` allowed but wait fields
  reject 0; negative/garbage -> default; `min>max` normalized to `min=max`.
- **`MostLenient` cross-table fold**: takes max num-retries / max-wait / total-timeout and min
  min-wait, preserving `min<=max`.
- **`total-timeout-ms` enforcement** (mirror of the retry-loop guard): attempt 0 never aborts even
  with a tiny budget; aborts when a single wait exceeds the budget; aborts when cumulative elapsed
  would (overflow-safe comparison, no summation).
- **Retry-After consumption**: `-1` -> use backoff; honored value; capped at `max_wait_ms`.
- Commit-status classifier: 409 **and 429** -> CONFLICT (429 = retryable rate-limit, rejected before
  apply, no status-check needed); 408/500/502/503/504 -> UNKNOWN; other -> FATAL.
- **Retry-After header parse** (`ParseRetryAfterMs`): numeric seconds -> ms; empty/HTTP-date/garbage/
  negative -> -1; **huge value clamps to one day** (int64-overflow guard on `*1000`).
- **Ancestry / rollback guard** (`IsAncestorOf`): self, linear chain, diverged (concurrent rollback)
  detected, missing-link -> false (safe), self-referential-parent guard (no infinite loop).
- **Codec resolution** (`ResolveAvroCodec`): default deflate, gzip/deflate alias, none/uncompressed,
  unsupported -> error, **delete-forbidden catalog -> null (uncompressed)**.
- **Delete-entry attribution** (`DeleteEntryReferencedDataFile`): V3 referenced_data_file, V2
  filename-bounds fallback, spanning bounds -> none, equality delete -> none.
- **validateNoNewDeletes** decision: pre-existing delete is not a conflict, a newer delete on a
  targeted file is, an unrelated file is not, a DELETED-status entry is skipped.
- **Avro OCF deflate recompression**: a hand-built uncompressed OCF (two blocks) is recompressed and
  decoded back; asserts the codec metadata flips to `deflate`, the schema is preserved, and each
  block round-trips byte-identical via a real DEFLATE/INFLATE pass. Plus a zigzag-varint round-trip
  over boundary values incl. negatives. This is a genuine correctness check, not just a mirror.

Independently, the OCF recompression algorithm was cross-validated against **fastavro** (a
spec-compliant non-DuckDB reader): a file recompressed by the identical algorithm reads back with
`codec=deflate` and byte-identical records, for single-block and many-block inputs.

### 5.2 Catalog-free SQL test -- read side of manifest compression (in `make test`)
`test/sql/local/iceberg_scans/iceberg_manifest_compression_read.test`: the persistent
`lineitem_iceberg` fixture's manifests are DEFLATE-coded (Spark/Java default), so reading its
metadata and running a full scan exercises the deflate manifest -> manifest-list -> data-file chain
through the production reader, no catalog needed. Locks the read-side compression contract.

### 5.3 Catalog-required SQL integration tests (run by the lakekeeper/polaris/nessie/fixture CI jobs)
Under `test/sql/local/catalog_test_config_setup/catalog_agnostic/`, gated by
`require-env CATALOG_TEST_CONFIG_SETUP`. These cover the write/retry/validate paths that fundamentally
need a real REST catalog:
- `insert/test_merge_append.test`: low min-count -> manifests merge across commits; data correctness.
- `insert/test_merge_append_disabled.test`: `enabled=false` -> FastAppend, one manifest per commit.
- `insert/test_merge_append_v3_deletes.test`: V3 table, merge across DATA + DELETE manifests / DVs
  with multiple delete rounds; surviving rows stay correct.
- `insert/test_commit_retry_config.test`: the full `commit.retry.*` property family is accepted; a
  normal commit succeeds; `num-retries=0` still commits. (Implementation note: `num-retries`,
  `min/max-wait-ms` and `total-timeout-ms` are all honored -- the retry loop stops if the next backoff
  would push cumulative elapsed time past `total-timeout-ms`, mirroring Java; an earlier cut parsed
  `total-timeout-ms` but never enforced it, which was fixed in self-review.)
- `insert/test_manifest_compression.test`: **write side of compression** -- default deflate, the gzip
  alias, `none` (skip recompression), and compression+merge together; each writes then reads back to
  prove the freshly written (compressed and/or merged) manifest container is spec-valid.
- `transactions/test_commit_retry_concurrent.test`: 8 connections insert into one table at once;
  losers hit 409, refresh, regenerate against the new parent, and retry; the test asserts all 8 land
  exactly once -- the end-to-end happy-path retry.
- `transactions/test_commit_retry_concurrent_delete.test`: 4 connections concurrently DELETE
  overlapping rows (same data files). Exactly one wins; the losers hit 409, refresh, and
  `ValidateRetrySafe` detects the concurrent delete on a data file they also target -> each aborts
  (FATAL `Cannot retry commit ...`) rather than silently double-deleting. Asserts the *final-state
  invariants* (a `statement maybe` per loop iteration tolerates the per-connection abort): non-target
  groups fully intact (no unrelated data touched), `id % 4 == grp` holds (no corruption), ids unique
  (no duplication), and the surviving grp=0 rows form a clean prefix (no non-prefix/double delete).
  This drives the three `ValidateRetrySafe` FATAL throw-sites end-to-end (the offline logic tests
  cover only the decision predicates).

**CAS-strength gating of the two concurrent tests.** Both `test_commit_retry_concurrent*` tests
additionally `require-env LAKEKEEPER_SERVER_AVAILABLE`. Reason found empirically (see section 10.3):
the `apache/iceberg-rest-fixture` used by the fixture CI job is **SQLite-backed and does not enforce
serializable commits under true parallelism** -- it returns HTTP 200 to commits that should get a 409,
silently dropping them, which no client retry can recover (the lost commits never see a 409 to retry
on). These tests therefore cannot pass on the SQLite fixture and are gated to the Lakekeeper
(Postgres-backed, real compare-and-swap) CI job, where concurrent commits behave like a production
catalog. Concurrent correctness was also verified manually against AWS S3 Tables (50-way, zero loss).
They skip cleanly (not fail) on the fixture/other jobs.

These all live under directories the existing catalog CI jobs already glob
(`test/sql/local/catalog_test_config_setup/*`), so they run with **no CI-config change** -- they are
picked up automatically by the fixture / lakekeeper / nessie / polaris jobs (the concurrent pair only
firing where `LAKEKEEPER_SERVER_AVAILABLE` is set).

Fault-injection E2E (fixture catalog + a second mitmproxy on its own port running
`scripts/fault_injection_addon.py`, wired into the fixture CI job via `HTTP_FAULT_PROXY`). The addon
has two markers (keyed off the table name in the commit path, each firing once so the eventual real
attempt passes through):
- `catalog_custom_setup/fixture/commit_status_check_fault_injection.test` (marker
  `inject_unknown_once`): the addon lets the catalog actually apply the commit, then rewrites its
  successful response to a 502 in the *response* hook -- the "UNKNOWN but committed" case. The test
  asserts the extension resolves it via the status-check (re-load the table, find the committed
  snapshot) as success: rows present, no data loss, no spurious retry, no erroneous cleanup. Drives
  `UNKNOWN -> CheckCommitStatus -> ALL_COMMITTED`.
- `catalog_custom_setup/fixture/commit_status_check_none_committed.test` (marker
  `inject_fail_before_apply_once`): the addon answers the first commit POST with a 502 in the *request*
  hook, **without forwarding it to the catalog**, so the commit provably never applied. The extension
  sees UNKNOWN, status-checks (snapshot absent -> NONE_COMMITTED), and retries; the second attempt
  (budget exhausted) reaches the catalog and succeeds. Asserts the row lands exactly once (no
  double-apply, no loss). Drives `UNKNOWN -> CheckCommitStatus -> NONE_COMMITTED -> retry-to-success`.
Both assert a distinctive `ICEBERG_FAULT_INJECTED ... hook=<request|response> ...` log line so a
mis-wired proxy / unmatched marker cannot let the test pass without exercising the targeted path.

### 5.4 Deliberate scope: what is NOT automated, and the tradeoff for it
The C++ logic tests **are** run in CI (5.1) -- a `make test_logic` target plus one `BuildIceberg`
step. The fault-injection E2E for both resolvable UNKNOWN outcomes (ALL_COMMITTED and NONE_COMMITTED)
**is** wired in (5.3, fixture CI), and the `ValidateRetrySafe` FATAL paths are now covered end-to-end
by the concurrent-delete test (5.3). What remains not-yet-automated:

1. **The multi-table "partial commit" UNKNOWN branch** (`CheckCommitStatus` finds *some* but not all
   of a multi-table transaction's snapshot ids -> surface UNKNOWN, delete nothing). Deterministically
   forcing a real atomic multi-table endpoint to land some tables but not others would require a
   fabricated/inconsistent catalog the harness cannot produce faithfully; forcing it would yield a
   fragile, unrealistic test. The branch's logic is unit-reasoned and the data-safety default (never
   delete on an unresolved outcome) is conservative. **Why acceptable:** the two single-table UNKNOWN
   resolutions are E2E-covered; the multi-table partial case is a catalog-inconsistency corner that
   only changes whether we surface "unknown" vs retry, never whether we delete files (we never do on
   UNKNOWN).

Execution status: the C++ logic layer (5.1) runs locally with the command above; the catalog-free SQL
(5.2) runs in `make test`; the catalog-required tests (5.3) were **executed against a real Docker
fixture catalog (apache/iceberg-rest-fixture + MinIO) and against AWS S3 Tables** during development
(all green -- see section 10), in addition to running in the catalog CI jobs.

How to run: `make test_logic` (or the one-line `c++ ... test_merge_retry_logic.cpp` command) for the
C++ layer (5.1); `make test` for catalog-free SQL (5.2); the catalog CI jobs (or `make fixture` + a
local catalog, `make lakekeeper` for the concurrent pair) for the catalog-required tests (5.3).

---

## 6. Implementation order (single PR, internal milestones)

All four phases land in **one PR / one release**. The ordering below is the internal build order, not a release schedule. The PR is not considered complete until Phase D and the section 8 corrections are all done.

1. **Phase A** (pure-function refactor of apply) + unit-test scaffolding -> compiles, no regression of existing tests.
2. **Phase B** (merge module DATA+DELETE) + merge unit tests + SQL merge tests.
3. **Phase C** (single-snapshot bundling + row_id NULL-ification) + related tests.
4. **Phase D** (retry loop + exception classification + cleanup) + retry unit/integration tests.

Each phase compiles and is reviewable as a separate commit inside the PR, but none ships on its own. Because retry is on by default from the first release, the V3 concurrent-delete backstop (problem B5) and correction A1 must be resolved before merge.

---

## 7. Risk register

| Risk | Mitigation |
|---|---|
| Phase C touches the transaction core structure, large regression surface | Commit incrementally; keep an old-path switch; thorough unit tests |
| Incorrect snapshot summary stats after merging | Merge happens after the added stats are accumulated; unit-test the counts |
| Retry cleanup accidentally deletes data files | Strictly distinguish manifests (deletable/rewritable) from data files (reused across attempts, not deleted) |
| CommitStateUnknown mis-cleanup causes data loss | That exception is rethrown as-is, never cleaned up (aligned with Java) |
| Concurrent-conflict tests are flaky | Provide a test hook; mark test_slow / CI-only if needed |
| DV merge complexity | Not done in the first version, marked TODO; DELETE manifests are still bin-packed; V3 concurrent-delete conflict must ABORT, not silently retry (see B5) |
| Snapshot operation hard-coded to OVERWRITE after bundling | Classify APPEND/DELETE/OVERWRITE per change set (B14) |
| Stale `table_info` reference after refresh | Rebind pending changes to refreshed metadata each attempt (B16) |
| DELETE metrics misattributed to DATA summary | Branch on `manifest_file.content` when accumulating metrics (B17) |
| Multi-table fallback path is not atomic | Restrict retry to atomic endpoint or document non-atomic semantics (C7) |
| Status-check misjudges an unknown outcome | Look up by per-table `snapshot_id`; multi-table is all-or-none; on doubt surface unknown and delete nothing (B13/B15) |
| Streaming merge writer/aggregator not yet implemented | Cap merge bins at the batched threshold until the incremental writer exists; large bins simply not merged (9.1) |
| Same-transaction spec/sort-order change + data write | v1 fails fast on retry instead of rewriting (B12a) |
| Cached `existing_manifest_list` goes stale on retry | Re-read parent manifest list each attempt; do not reuse the cache (B16) |

---

## 8. Open problems & corrections (read before coding)

This section records the critical review of the plan against the Java and PyIceberg implementations. Items **A** are corrections to earlier statements; items **B** are gaps that must be closed; section **F** is the unified artifact-ownership/cleanup model; items **C** are improvements; items **E** are effectiveness notes and test invariants. Correctness blockers that must be resolved before the corresponding step ships include (non-exhaustive) **A1, B5, B12a, B13, B15, B16, B17, B19**.

### A. Corrections to earlier statements

**A1. "Merge does not materialize sequence numbers" was too absolute, and the existing rewrite helper is a trap to reuse (correctness blocker).**

Mechanism, stated precisely (an earlier draft of this section described it wrongly). `ManifestFileNeedsToBeRewritten` (`iceberg_add_snapshot.cpp:83-109`) does, per entry:
```
seq = entry.GetSequenceNumber(manifest_file);   // for ADDED with no seq, returns manifest_file.sequence_number
entry.SetSequenceNumber(seq);                    // materialize
if (entry.status == ADDED) entry.status = EXISTING;   // demote
```
For an EXISTING entry, `GetSequenceNumber` returns its *original* materialized value, so re-materializing it is harmless. The helper is therefore correct *for its current caller*, which only ever runs it on **carried-over, already-committed** manifests (`ConstructManifestList:138`) whose `sequence_number` is a settled historical value.

The trap: the merge step (method Y, see section 8 preamble) feeds **new-data manifests** through the same path. A new-data manifest's `sequence_number` is *this attempt's* new value, so an `ADDED` entry gets materialized to that new sequence number and demoted to `EXISTING`. That breaks two things at once:
- **Retry immunity (Phase C):** `WriteToFile` (`iceberg_manifest.cpp:350-369`) only allows a NULL/inherited sequence number for `ADDED` entries; once demoted+materialized, the entry is pinned to this attempt's sequence number and must be rewritten on every retry.
- **Snapshot stats (was listed separately as D2):** if a merged manifest's `added_files_count` then counts those demoted entries inconsistently, `ADDED_*` / `TOTAL_*` metrics double-count. Note the current code only calls `AddManifestFile` on the genuinely-new `uncommitted_manifest_files` (`CreateUpdate:212-226`), never on carried-over manifests -- so stats stay correct *only as long as* the merge keeps "logically added" decoupled from "physically packed".
- **DELETE manifests, far worse (was listed separately as D3):** a delete file's `data_sequence_number` determines which older data files it applies to. If a delete entry is re-sequenced to the new manifest's sequence number, its scope changes and the result is a **silent wrong delete (over- or under-deletion)**, not just a bad statistic.

**Resolution:** do NOT blindly reuse `ManifestFileNeedsToBeRewritten` for the merge. The merge must distinguish (1) entries pulled in from already-committed manifests -- keep their original materialized sequence numbers, write as `existing`; from (2) entries that are new in this transaction -- keep them `ADDED` with NULL/inherited sequence numbers, never demote them. This is exactly what Java does (`ManifestWriter.existing` preserves the original data sequence number; only genuinely new files inherit). This rule is identical for DATA and DELETE manifests, but a violation on DELETE corrupts data while on DATA it only corrupts stats. Phase B section 3 item 5 reflects this.

Two-level sequence-number clarification (so the rule above is not misread): the *manifest-file-level* `sequence_number` and the *entry-level* `data_sequence_number` are different things. A merged manifest is a brand-new physical file created by the current snapshot, so its manifest-level `sequence_number` correctly becomes the current snapshot's sequence number (this is what `AddNewManifestFile` stamps, and it matches Java). What must be preserved are the *entry-level* sequence numbers of merged-in committed entries (rule 1 above) and the manifest's `min_sequence_number` (Phase B item 4). Do not try to keep the old manifest-level sequence number.

**A2. The `merge-enabled = true` default interacts with delivery order.**
With `true`, every commit may scan and rewrite existing manifests even when there are no deletes, changing the write path and file layout for all current INSERT users (and likely shifting golden values in existing SQL tests). Java can default to `true` because it has a mature retry safety net. **Because this work ships in a single release (section 0), retry is present from day one, so the default `true` is acceptable.** It would NOT be acceptable to ship merge with `true` before retry exists. Action: budget for updating existing SQL test expectations (manifest counts / paths) as part of this PR.

### B. Gaps that must be closed

**B1. Avoid writing-then-rewriting new manifests (wasted IO + orphans).**
Today `CreateUpdate:216` already writes each new manifest via `WriteManifestListEntry`. If Phase B then reads them back, merges, and rewrites, the first write is wasted and leaves an orphan file. Java avoids this by keeping new data manifests in `cachedNewDataManifests` and only writing the final form after the merge decision. **Resolution:** in `ApplyTableChanges`, do not eagerly write new-data manifests before the merge decision; decide the final layout first, then write once. Any intermediate file that is still produced must be tracked for cleanup. **Cleanup granularity (was E3):** a *source* manifest that was merged in but is still referenced by a retained historical snapshot must NOT be deleted -- deleting it breaks time travel. Cleanup deletes only manifests/manifest-lists this attempt newly wrote and that the winning snapshot does not reference (Java's `cleanUncommitted(committed = winning snapshot manifests)`), never the merged-in historical sources.
> STATUS: PARTIALLY IMPLEMENTED (orphan-tracking half done; write-once optimization deferred as not
> worth it). The current code still writes each new manifest first (`WriteManifestListEntry`) and may
> then repack some into fresh merged files, so a merged-away pre-merge manifest IS an orphan on disk.
> The leak is now closed: `ApplyTableChanges` records each pre-merge manifest path in
> `commit_state.written_metadata_paths` *as it is written* (before the merge can repack it), then adds
> the manifest-list and any merge-product paths afterwards, de-duplicated via a set. So every file
> physically written this attempt is tracked and cleaned up on a failed/aborted commit (subject to
> `allows_deletes`; over-tracking is safe because cleanup only runs for uncommitted tables, never for a
> committed snapshot's files). The *write-once* optimization (decide the merged layout before writing,
> Java's `cachedNewDataManifests`) is deliberately NOT done: it is a non-trivial restructuring of the
> apply path to save one write of a small (KB-MB) manifest only when a merge fires, and the orphan it
> would avoid is already tracked+cleaned. Cost > benefit, so it stays deferred; correctness (no leak)
> is achieved by the tracking instead.

**B2. V3 `first_row_id` global continuity across a merge.**
Row lineage in V3 is globally contiguous: `snapshot.first_row_id` plus each manifest's added/existing rows. Merging changes the number and order of manifests, so recomputing `first_row_id` (Phase B item 4) must keep the global assignment contiguous and consistent with `snapshot.first_row_id` / `next_row_id` (`CreateUpdate:207,220`). **Resolution:** define a single deterministic pass that assigns `first_row_id` to manifests in their final (post-merge) order, and run merge before that assignment, not after. Add a unit test asserting contiguity. **Per-attempt reset (was E2):** each retry attempt must restart the row-id assignment from the *refreshed* `metadata.next_row_id`; never carry over the `next_row_id` advanced by a failed attempt, or row ids will gap or overlap.

> IMPLEMENTED (conservative, correctness-first, and nearly cost-free): the Iceberg v3 spec requires
> that a row's `first_row_id` is a stable identifier and MUST be preserved (never reassigned) when a
> manifest is rewritten/merged. Already-committed ("carried-over") manifests have settled
> first_row_ids, so the merge folds them safely and the merged manifest keeps the minimum
> `file.first_row_id` it absorbs. This transaction's brand-new data manifests, by contrast, have not
> yet had per-entry first_row_id materialized -- those rows rely on manifest-level inheritance
> assigned later in `manifest_list::WriteToFile`. Merging not-yet-assigned rows with already-assigned
> rows in one manifest would break that inheritance basis; doing it correctly means materializing
> every entry's first_row_id before merging, which needs end-to-end V3 row-lineage tests to validate
> (a mistake silently corrupts row identity). So new V3 data manifests are left unmerged. The cost is
> negligible: on the next commit they are carried-over and become mergeable, so V3 manifest growth
> still converges -- merely delayed by one commit. V2 has no row lineage and merges everything.

**B3. Retry under multi-table transaction commit.**
The default commit path is the multi-table transaction endpoint (`CommitMultiTableUpdate`, `iceberg_transaction.cpp:454-466`), which is atomic across tables. The retry loop in section 3-D is written as if single-table. **Resolution:** the retry unit is the whole transaction commit: on a conflict, refresh and re-`apply` *all* tables in the transaction, rebuild the combined request, and re-POST. Per-table partial success is not possible on the transaction endpoint (it is atomic); for the single-table fallback path, retry per table. Document both paths explicitly in Phase D.

**B4. How `CommitStateUnknown` is identified in this repo.**
"Never clean up on unknown state" (section 2.3) requires a concrete mapping in the HTTP layer (`catalog_api.cpp`), which today only distinguishes 200/204 from everything else. **Resolution mapping:** network timeout / connection reset / 5xx -> state unknown (rethrow, no cleanup); HTTP 409 (or Iceberg `CommitFailedException` body) -> retryable conflict; other 4xx -> definite failure (cleanup allowed). Introduce an `IcebergCommitConflictException` and an `IcebergCommitStateUnknownException` and classify responses in the commit functions.

**B5. Skipping DV merge has a correctness consequence under concurrent deletes (correctness blocker).**
#786 explicitly calls out that on retry, if a concurrent commit added a delete targeting the same `referenced_data_file`, the local delete must be combined with it, otherwise the retry loses a deletion. Since DV merge is deferred, a silent retry of a V3 delete could produce a wrong result. **Resolution (hard requirement for this release):** when a V3 delete/overwrite operation hits a commit conflict and the refreshed metadata contains a concurrently-added delete for an overlapping `referenced_data_file`, the operation must **ABORT with a clear error rather than silently retry**. Plain appends (no deletes) and non-overlapping deletes may still retry. This keeps correctness while DV merge remains TODO. Add a validation step (analogous to Java `validateAddedDVs`) in the retry loop for delete/overwrite snapshots.

**B6. Confirm a way to assert manifest counts in SQL tests.**
The integration tests (section 5.2) assume a metadata function can report "how many manifest files the current snapshot has". This has not yet been confirmed to exist in the extension. **Resolution:** before writing the SQL tests, verify the available metadata table functions (e.g. `iceberg_metadata`, `iceberg_snapshots`, a manifest-listing function); if none exposes manifest counts, either add one or assert indirectly (e.g. via row counts + snapshot summary fields such as `manifests-created` / `manifests-replaced`). Do not write tests that cannot actually observe the merge.

**B7. Retry must rebuild ALL requirements from the refreshed base, not just `assert-ref-snapshot-id`.**
`GetTransactionRequest` attaches several requirements (`iceberg_transaction.cpp:345-377`): `assert-ref-snapshot-id`, `assert-current-schema-id`, `assert-table-uuid`, etc. Section 3-D only mentions refreshing the snapshot-id requirement. If a concurrent commit changed, say, the schema, a requirement that keeps asserting the *old* `current-schema-id` will fail on every attempt -> the retry loop spins until it exhausts its budget and then fails, instead of either succeeding or failing fast. **Resolution, with a deliberate distinction (do not just blindly re-stamp):**
- A changed *parent snapshot* is the normal optimistic-concurrency case -> refresh `assert-ref-snapshot-id` and retry.
- A changed *schema / spec / sort-order* means the world our data was written against moved. Re-deriving the requirement from the new base could mask a genuine incompatibility. The safe behavior is to **abort with a clear error** when the property our write depends on changed (e.g. our data was encoded against schema S and the table is now on schema S'), rather than silently re-stamping and committing against a schema we never validated. Only requirements that are pure concurrency guards (snapshot ref) are auto-refreshed; semantic guards (schema/spec) that actually changed cause an abort.
This mirrors Java, which rebuilds requirements from the freshly refreshed base each attempt but still runs `runValidations` that can throw a non-retryable `ValidationException`.

**B8. Validate that delete targets actually exist (borrowed from Java; we are currently weaker than Java here).**
`ManifestFileNeedsToBeRewritten` (`iceberg_add_snapshot.cpp:98`) decides a file is removed via `deletes.IsInvalidated(file_path)`, but if a delete target is not found in *any* current manifest it is **silently ignored**. Java exposes `failMissingDeletePaths` / `failAnyDelete` on its `ManifestFilterManager` precisely to fail fast when an expected delete target is gone (e.g. a concurrent commit already removed it). Without this, a delete that should have matched but matched nothing produces a silently-wrong result rather than an error. **Resolution:** track which delete targets were matched during the rewrite pass; if any expected target matched zero entries across all scanned manifests, abort with a clear error. v1 default is a hard error (matching Java's `failMissingDeletePaths`); a relaxed "warn instead" mode is explicitly out of scope for v1 to avoid leaving a silent-wrong-result loophole. This is a correctness improvement over the current code and reaches parity with Java's filter manager. (Complements B5, which is about concurrently-added DVs; B8 is about expected deletes that have vanished.)

**B9. Targeted conflict validation before retry, not blind retry (borrowed from Java `validateAddedDataFiles` / `validateNoNewDeletes`).**
Section 3-D currently retries on any `assert-ref-snapshot-id` failure (parent moved), regardless of whether the concurrent change actually conflicts with this commit. Java does better: between the starting snapshot and the refreshed parent it inspects only the manifests/entries that intersect this operation's partitions/filter (`validateAddedDataFiles`, `validateNoNewDeletesForDataFiles`, `validateAddedDVs`), so a concurrent change in an unrelated partition does not block, and a real conflict fails fast instead of after exhausting retries. **Resolution, scoped honestly:** for plain APPEND (#790's main case) no validation is needed -- an append never conflicts with another append, so it always retries safely. For DELETE / OVERWRITE, add a local validation step on the refreshed metadata before re-committing: if a concurrently-added data file or delete intersects this operation's partition/filter, abort (non-retryable) rather than silently producing a wrong result. This keeps appends maximally retryable while making deletes correct, and is strictly better than the catalog's coarse-grained `assert-ref-snapshot-id` alone. Note: validation runs locally against refreshed metadata; it does not require any new catalog API. **Prerequisite:** B8/B9 validation needs inputs that must be captured in the pending change set and survive into each retry attempt -- the original delete/overwrite predicate, case-sensitivity, the starting snapshot id (the parent at operation time), and the affected partition set. Without persisting these, the validation cannot be re-run on the refreshed metadata. **Lineage guard:** if the refreshed parent cannot be shown to be a descendant of the starting snapshot (ancestry traversal breaks, e.g. a concurrent rollback), abort non-retryable rather than validating against an unrelated history (mirrors Java's `validationHistory` ancestry check).

> STATUS: IMPLEMENTED (file-existence + new-delete + ancestry) / DEFERRED (partition-predicate pruning).
> `IcebergTransaction::ValidateRetrySafe` runs after `RefreshForRetry` re-reads the refreshed parent
> and performs three local checks (no new catalog API), modeled on Java's validationHistory:
>  1. **Ancestry / rollback guard** (`IcebergTableMetadata::IsAncestorOf`): the refreshed parent must
>     still descend from the snapshot this transaction started against. The starting snapshot id is
>     captured once on the first apply and kept stable across retries
>     (`IcebergTransactionData::starting_snapshot_id`, set in `CacheExistingManifestList`, deliberately
>     not reset by `RefreshForRetry`). A concurrent rollback that breaks the ancestry chain -> abort
>     non-retryable. A missing link in the chain (elided snapshot) is treated as "cannot prove
>     ancestry" -> abort, the safe choice.
>  2. **validateDataFilesExist**: every data file we add deletes against must still be live in the
>     refreshed parent's DATA manifests; a concurrent removal -> abort.
>  3. **validateNoNewDeletes**: scanning the refreshed parent's DELETE manifests, if any delete entry
>     references one of our targeted data files with a sequence number newer than our starting
>     snapshot's, a concurrent commit raced us with an overlapping delete -> abort.
> The targeted set is the union of `altered_manifests` (V3 DV replacement) and the data file each of
> our own pending DELETE manifest entries applies to. That per-entry attribution
> (`DeleteEntryReferencedDataFile`) uses `referenced_data_file` on V3 and falls back to the
> FILENAME_FIELD_ID column bounds on V2 (where the spec does not store `referenced_data_file`; lower
> == upper identifies the single data file), so the check covers **both** V2 positional deletes and
> V3 deletion vectors. Pure appends have an empty targeted set and skip all three checks.
> EFFICIENCY: the refreshed parent's DATA+DELETE manifests are read in a **single batched AvroScan**
> (the multi-file reader routes each manifest's entries back to its own slot), not one scan per
> manifest. DEFERRED (not v1) and explicitly *rejected* as an optimization here: finer
> partition/predicate *pruning* of which manifests to scan (`ManifestMatchesFilter`). It would skip
> non-intersecting manifests, but (a) it needs a `TableFilterSet`/partition set the transaction layer
> does not have (the predicate is resolved to row ids before `IcebergDelete`), and (b) a concurrent
> delete that conflicts is by definition in the *same* partition we are deleting from, so pruning
> saves work precisely where there is nothing to find while adding a path where a spec/transform
> mis-judgment could drop a real conflict -- trading correctness for cold-path IO. The batched scan
> is the right, zero-risk efficiency win; partition pruning is not worth it. NOTE: the
> concurrent-removal and concurrent-new-delete branches need a real REST catalog with a competing
> writer to exercise end-to-end; covered structurally here and unit-tested in 5.1.



**B10. There is no manifest-level orphan cleanup today; merge + retry makes this acute.**
`CleanupFiles` (`iceberg_transaction.cpp:688`) only calls `TryRemoveFile` on `data_file.file_path` -- it never deletes manifest or manifest-list files. Under the current FastAppend, a failed commit already leaks its manifest files. With merge + retry this gets much worse: every failed retry attempt writes a fresh manifest-list plus merged manifests, all of which leak. Java has `cleanAll()` (hard failure: delete every manifest-list and uncommitted manifest this producer wrote) and `cleanUncommitted(committed)` (after success: delete anything the winning snapshot does not reference). **Resolution:** implement a manifest-level cleanup, modeled on Java: track every manifest and manifest-list this transaction writes; on hard failure delete all of them; on success delete those not referenced by the winning snapshot. Respect `attach_options.allows_deletes` (skip when the catalog forbids deletes, e.g. S3 Tables). This is a new capability the current code lacks entirely, not just a tweak.

> IMPLEMENTED: `IcebergCommitState::written_metadata_paths` records every manifest-list and
> snapshot-owned manifest written during apply; `GetTransactionRequest` carries them into the
> persistent `IcebergTransactionData::written_metadata_paths` (accumulating across retry attempts);
> `CleanupFiles` deletes them on a failed/aborted transaction (only manifests created by this
> snapshot are recorded, so carried-over historical manifests are never deleted). Honors
> `allows_deletes`. UNKNOWN-outcome commits skip cleanup via the `Commit()` catch (B11/B13).

**B11. A failed retry attempt must NOT trigger data-file cleanup (data-loss hazard).**
`CleanupFiles` is invoked from the `Commit()` catch block (`iceberg_transaction.cpp:434`) and deletes the data files of every pending snapshot. That is correct when the *whole transaction* aborts. But a single failed retry *attempt* (a 409 conflict that will be retried) must keep its data files -- the next attempt reuses them (this is the very foundation of retry, section 1.4). If a failed attempt ran the existing `CleanupFiles`, it would delete the data files that the retry still needs, corrupting the commit. **Resolution:** the retry loop must distinguish "attempt failed, will retry" (clean up only this attempt's freshly written manifests/manifest-list, never data files) from "transaction aborted, no more retries" (then, and only then, the existing data-file cleanup is allowed, and only for non-`CommitStateUnknown` outcomes). This is a control-flow requirement on where `CleanupFiles` may run relative to the retry loop.

**B12. A merge that yields an empty manifest must not be written (`D_ASSERT` / UB).**
`manifest_file::WriteToFile` asserts `!manifest_entries.empty()` (`iceberg_manifest.cpp:430`). The merge can legitimately produce an empty result (e.g. every entry in a bin became DELETED and was filtered out). Writing it would hit the assert in debug and be undefined behavior in release. Java/PyIceberg drop such manifests before writing via their `shouldKeep` filter. (This corrects an earlier note that treated "empty manifest removal" as out of scope: that was true only for the carry-over path; the *merge* path is new code in this PR and can newly produce empties, so it must handle them.) **Resolution:** in the merge, if the aggregated entry set is empty, do not call `WriteToFile` and do not add the manifest to the list; if a merged bin would collapse to nothing, drop it.
> STATUS: IMPLEMENTED, with an ordering bug found + fixed in self-review: the first cut checked for
> emptiness in the *caller* (`MergeManifests`) only **after** `MergeBin` had already called
> `WriteToFile`, so an empty bin still hit the `D_ASSERT`. Fixed by bailing out inside `MergeBin`
> (returning a sentinel entry with no `manifest_entries`) **before** `CreateFromEntries`/`WriteToFile`;
> the caller drops any entry with empty `manifest_entries`.

**B12a. Same-transaction schema/spec/sort-order change + data write breaks retry immunity (called out verbatim by #786).**
#786 states that changes to `partition-spec-id` and `sort-order-id` invalidate the "manifest_entry never needs rewriting" assumption when they happen *in the same transaction* that creates the manifest_entry, because the partition values / sort-order id are materialized into the entry against a spec that another concurrent commit might also be introducing. Section 8-B7 only covers *concurrent* schema/spec change; it does not cover *our own* transaction changing the spec and writing data in one go. **Decision for v1 (hard requirement):** if a transaction both changes partition-spec / sort-order (or schema in a way that affects partition value encoding) **and** writes data files, do not retry it -- fail fast with a clear error if a conflict occurs (the first commit attempt may still succeed; only the retry is disallowed). The per-attempt manifest-rewrite path that would make this retryable is explicitly deferred to a later enhancement, not v1.

**B13. Commit "state unknown" needs a status-check phase, not just rethrow (borrowed from Java `commit.status-check.*`).**
Section 2.3/B4 say to rethrow on unknown state and never clean up. That is safe but incomplete: the commit may actually have landed, and the caller is left not knowing. Java does this in the `TableOperations` layer, not in `SnapshotProducer`: `BaseMetastoreTableOperations.checkCommitStatus` re-loads the table and checks whether the **new metadata-location** it tried to write is now the current metadata-location or appears in `previousFiles()`, retried under `commit.status-check.*` (num-retries=3, min-wait=1s, max-wait=60s, total-timeout=30min). The metadata-location check is the authoritative Java mechanism; "look for my snapshot id" is the REST-friendly equivalent we will use because this extension does not own metadata-file writing.
**Resolution:** after an ambiguous commit failure (timeout/connection reset/5xx), run a bounded status-check loop (`commit.status-check.*`): refresh the table and check whether this attempt's `snapshot_id` (stable across attempts, section 2.3) is present in the target ref's history. If present -> success (resolve cleanup against the *found* snapshot, see section 8-F). If still absent after the budget -> surface as unknown **without deleting data files**. For the multi-table path the check is all-or-none (B15). This is a concrete capability the repo lacks entirely today (`status-check` / `CommitStateUnknown` do not exist).

> STATUS: IMPLEMENTED. Both halves are done. (1) Data-safety: an UNKNOWN outcome (timeout / 5xx /
> connection reset) is classified (`IcebergCommitException` with `IcebergCommitOutcome::UNKNOWN`), is
> never blindly retried (could double-apply) and never triggers `CleanupFiles` (could delete data of
> a commit that actually landed). (2) Positive status-check: `IcebergTransaction::CheckCommitStatus`
> re-loads each uncommitted table via `IRCAPI::GetTable` and tests whether this transaction's stable
> `snapshot_id`(s) are present in the refreshed metadata. The prerequisite -- a per-attempt
> `snapshot_id` that is stable across retries -- is now satisfied: `IcebergAddSnapshot` caches the
> minted id (`GetSnapshotId()`) instead of re-minting on every `ApplyTableChanges`. Resolution is
> all-or-none for the atomic multi-table endpoint (B15): all expected ids found -> treat as
> committed (skip cleanup, no re-POST); none found -> safe to retry; some-but-not-all -> surface as
> unknown and delete nothing. NOTE: deterministic verification of the some-but-not-all and
> double-commit-avoidance branches requires a real REST catalog with injected ambiguous failures;
> covered structurally + by the offline logic tests, not yet by an end-to-end concurrency test.

**B13a. Transport-level failures bypass the UNKNOWN classification (data-corruption bug found in self-review; fixed).**
The UNKNOWN classification (B13) keys off the HTTP **status code** of the commit response. But a
transport-level failure -- connection reset, timeout, DNS -- does not produce a status code: DuckDB's
HTTP layer (`HTTPUtil::Request`) *rethrows* the exception once retries are exhausted (it only returns
a synthetic error response when `try_request` is set, which the commit POST does not set). So a
dropped connection on the commit POST threw a generic `HTTPException`/`IOException` straight past
`ClassifyCommitStatus`, out of the retry loop's `catch (IcebergCommitException&)`, into `Commit()`'s
generic `catch (std::exception&)` -> **unconditional `CleanupFiles()`**. If the catalog had actually
applied the commit and only the response was lost (the canonical ambiguous case), this deleted the
data files of a committed snapshot = silent corruption. Two code paths had this hole: the commit POST
itself (`CommitMultiTableUpdate`/`CommitTableUpdate`) and the status-check's own `GetTable`
(`CheckCommitStatus`), plus a latent throw in `ValidateRetrySafe` (`GetSnapshotById` throws when the
starting snapshot id is reachable-as-a-parent-link but not a map key after a concurrent rollback).
**Resolution:** wrap the commit request so a thrown transport error is converted to
`IcebergCommitException(UNKNOWN)` (a commit POST that failed in transit is ambiguous by definition);
wrap the status-check `GetTable` so a thrown failure returns `StatusCheckResult::UNKNOWN` (still
ambiguous, never clean up); and make the starting-snapshot lookup non-throwing (absent -> skip the
sequence-number narrowing, still correct). After the fix, every ambiguous outcome -- whether it
surfaces as a 5xx status or as a transport throw -- funnels through UNKNOWN, so files are never
deleted for a commit that may have landed.


**B14. Single-snapshot bundling must classify the snapshot operation; today it is hard-coded to OVERWRITE.**
`IcebergAddSnapshot::CreateUpdate` sets `new_snapshot.operation = OVERWRITE` unconditionally (`iceberg_add_snapshot.cpp:191`). Once Phase C bundles all of a transaction's changes into one snapshot, the operation type drives both the snapshot summary and (in Java) which validations run. A pure append reported as OVERWRITE is misleading to other engines and to incremental readers. **Resolution:** classify per bundled change set -- inserts only => `APPEND`; deletes only (no new data) => `DELETE`; updates / mixed insert+delete / partial overwrites => `OVERWRITE`. The repo's enum is `IcebergSnapshotOperationType { APPEND, REPLACE, OVERWRITE, DELETE }` (`iceberg_snapshot.hpp:11`); use `OVERWRITE` (not `REPLACE`) for row-level update/mixed writes -- `REPLACE` is reserved for compaction/rewrite-without-logical-change and must not be emitted here. Set the operation accordingly before building the snapshot. The operation type also selects the retry-time validation path: `APPEND` needs no conflict validation (an append never conflicts with another append, B9), while `DELETE`/`OVERWRITE` must run the B5/B8/B9 validations on the refreshed metadata. (This is pre-existing behavior, but Phase C makes it materially wrong, so it is in scope here.)
> STATUS: IMPLEMENTED. The operation is now the one the *caller* declared when staging the change,
> threaded into `IcebergAddSnapshot` via its constructor: INSERT stages `APPEND`, DELETE stages
> `DELETE`, UPDATE stages `OVERWRITE` (`iceberg_transaction_data.cpp` AddSnapshot/AddUpdateSnapshot ->
> `IcebergAddSnapshot(table_info, operation)`), and `ApplyTableChanges` writes `new_snapshot.operation
> = operation` instead of a hard-coded OVERWRITE. A subtlety that ruled out re-deriving the label from
> manifest contents: a plain DELETE also populates `altered_manifests`, so a content/altered-based
> classifier would mis-label it OVERWRITE -- the caller's declared intent is the authoritative source
> and is also simpler. The label is informational here (it never feeds this extension's own read path,
> and ValidateRetrySafe keys on the actual delete content, not on this enum), but it matters for
> external incremental readers (Spark/Trino/PyIceberg append scans, CDC). Verified end-to-end against
> AWS S3 Tables by capturing the commit POST body: INSERT -> `"operation":"append"`, DELETE ->
> `"delete"`, UPDATE -> `"overwrite"`.

**B15. Retry state must be per-table for the multi-table commit path.**
A naive retry loop would keep a single `snapshot_id`. But the default path is the atomic multi-table `transactions/commit` endpoint, which commits several tables at once. Each table has its own stable snapshot id, parent snapshot, sequence number, manifest-list, and cleanup artifact set. (The Phase D pseudocode reflects this with per-table `T.snapshot_id` and per-table `commit_state`.) **Resolution:** the retry loop holds per-table state keyed by table-key: `{stable snapshot_id, parent_snapshot_id, next_sequence_number, next_row_id, written artifacts}`. On a conflict, refresh and re-`apply` *every* table in the transaction, rebuild the combined request, and re-POST once. The snapshot id is stable per table across attempts; everything else is recomputed per table per attempt. **Unknown-state on the multi-table endpoint is all-or-none:** because the endpoint is atomic, the status check (B13) must find *every* table's expected `snapshot_id` to declare success; finding some-but-not-all is a catalog inconsistency -> surface as unknown and delete nothing.

**B16. Pending changes must be rebound to the refreshed metadata each attempt (stale-reference hazard).**
`IcebergTableUpdate` and `IcebergCommitState` both hold `const IcebergTableInformation &table_info` (`iceberg_table_update.hpp:41,75`). Retry refreshes the table metadata, but if the update/commit objects still point at the pre-refresh `IcebergTableInformation`, `apply` will read stale schema/spec/sequence/row-id and silently build the wrong snapshot. **There is a second stale source:** `IcebergTransactionData::CacheExistingManifestList` caches the parent snapshot's manifest list into `existing_manifest_list` on the first operation and skips re-reading when `!alters.empty()` (`iceberg_transaction_data.cpp:27-28`). On retry the parent moved, so the cached list is stale and would miss manifests added by the concurrent commit. **Resolution:** decouple pending changes (data files, delete predicates, altered-manifest sets) from any captured `IcebergTableInformation`, and at the start of each attempt (a) rebind to the freshly refreshed `table_info` and (b) re-read the parent manifest list from the refreshed parent snapshot rather than reusing `existing_manifest_list`. This is the concrete mechanism behind C1's "explicit `apply(base, parent)`" -- without it the pure signature is not actually pure.

**B17. Snapshot summary must carry DELETE-side metrics, not only DATA metrics.**
`IcebergSnapshotMetrics` only knows `ADDED/DELETED_DATA_FILES`, `ADDED/DELETED_RECORDS`, `TOTAL_DATA_FILES`, `TOTAL_RECORDS` (`iceberg_snapshot.cpp:88-93`). Java additionally tracks `total-delete-files`, `added-delete-files`, `removed-delete-files`, `added-position-deletes`, `added-equality-deletes`, plus the manifest-count summary (`manifests-created` / `manifests-kept` / `manifests-replaced` from `buildManifestCountSummary`). With DELETE manifests now merged and bundled, computing only DATA metrics will produce wrong/missing summary fields for delete-bearing snapshots, and risks miscounting a DELETE manifest's `added_files_count` as `added-data-files`. **Resolution, staged:** (1) the hard requirement that must not ship broken -- when accumulating from a manifest, branch on `manifest_file.content` so a DELETE manifest never contributes to data-file/records totals; (2) add the delete-side metric keys (`*-delete-files`, `*-pos-deletes`, `*-eq-deletes`) and the manifest-count keys so the summary is complete. (1) is a correctness blocker; (2) can be staged but is needed for parity with Java's summary.

**B18. `MergeManifests` must mark new-vs-existing per manifest, not via a `new_manifest_count`.**
An earlier interface passed `new_manifest_count` and the min-count guard assumed the "new" manifests are a known prefix. After grouping by spec and splitting DATA/DELETE, that positional assumption breaks. **Resolution:** tag each input manifest with its origin (`NEW_THIS_TRANSACTION` vs `CARRIED_OVER`) explicitly via `ManifestSource` (now in the Phase B interface), and have the guard test "bin contains any new-this-transaction manifest". The positional `new_manifest_count` parameter has been removed from the interface accordingly.

**B19. The streaming/large-bin merge (9.1) is incompatible with returning fully-materialized entries.**
`MergeManifests` is declared to return `vector<IcebergManifestListEntry>` whose `manifest_entries` are populated. True streaming for a large bin means entries are written straight to the new manifest writer and never all held in memory, so they cannot also be returned materialized. **Resolution:** the merge produces manifest-file metadata (path, counts, min_sequence_number, partitions) plus the artifact path for cleanup; it does not return entry vectors for streamed bins. Downstream code (cleanup, stats) must rely on pending-change bookkeeping and manifest-file-level metadata, not on re-reading merged entries. Small bins may still return entries; the API must allow "entries omitted".

### F. Artifact ownership and cleanup model (unifies B1/B10/B11/B13)

Cleanup correctness is spread across B1/B10/B11/B13/section 7; collect it into one ownership model so it cannot be implemented piecemeal. Every file the transaction touches falls into exactly one class:

| Artifact | Owner / lifetime | On attempt-retry | On hard abort | On success |
|---|---|---|---|---|
| Data files (parquet) written at Sink | Transaction; reused across attempts | keep | delete (only if not `CommitStateUnknown`, and catalog `allows_deletes`) | keep |
| New manifests written this attempt | This attempt | delete | delete | delete if not referenced by winning snapshot |
| Manifest-list written this attempt | This attempt | delete | delete | delete the losing-attempt lists; keep the winning one |
| Merged manifests produced this attempt | This attempt | delete | delete | delete if not referenced by winning snapshot |
| Carried-over / merged-in historical source manifests | The table / historical snapshots | never delete | never delete | never delete (still referenced by older snapshots; deleting breaks time travel) |
| Anything, when outcome is `CommitStateUnknown` | unknown | n/a | **never delete** | resolved by B13 status-check |

Rule of thumb: only delete what *this attempt* newly wrote, only when the outcome is definitively known, and never delete data files between retry attempts. Skip all deletes when `attach_options.allows_deletes` is false (e.g. S3 Tables manages GC itself).

**Resolving the "winning snapshot" for cleanup.** After success (direct or status-check-resolved), the committed set is the manifest list of the snapshot identified by *our* per-table `snapshot_id`, looked up in the refreshed metadata -- not the table's `current_snapshot`. A concurrent commit may have already added a newer snapshot on top of ours, so `current` is not necessarily ours. Look up by id (mirrors Java's `ops.refresh().snapshot(newSnapshotId)`); if the lookup fails (eventual consistency), skip cleanup rather than guess.

### C. Improvements

**C1. Give `ApplyTableChanges` an explicit `apply(base, parent)` signature.**
Pass the refreshed base metadata and parent snapshot as explicit arguments (like Java) instead of relying on mutated `commit_state` internals. A pure signature makes retry correctness easier to reason about and to unit-test.

**C2. Recompute the snapshot summary wholesale per attempt.**
Java clears and rebuilds the summary at the end of `apply` (added/deleted/created/replaced manifest counts). Merging changes summary fields such as `manifests-created` / `manifests-replaced`, so incremental accumulation can leave stale values after a retry. Rebuild the summary from scratch each attempt rather than accumulating.

**C3. Add jitter to the backoff.**
Java's `Tasks` backoff can include jitter to avoid thundering-herd retries. Keep exponential factor 2 (min 100ms, max 60s, total 30min) and add optional jitter.
> IMPLEMENTED, and deliberately stronger than Java's equal-jitter. `iceberg_retry.cpp` provides both
> `BackoffMs` (plain exponential, used by the offline logic tests) and `DecorrelatedBackoffMs`, the
> latter being what the live retry loop uses:
> `sleep = min(max_wait, random_between(min_wait, prev_sleep * 3))`, seeded with `prev_sleep =
> min_wait`. This is AWS's "decorrelated jitter" recommendation: the first few retries stay tight
> (near `min_wait`), and the window only widens as failures accumulate -- which is exactly the desired
> behavior for the thousands-of-concurrent-Lambdas workload (a brief conflict should re-fire quickly,
> a sustained pileup should spread out). Equal jitter (`base/2 + rand(base/2)`) was evaluated and
> rejected as a regression: it always waits at least half the full exponential delay, so it cannot
> stay tight early. The classic "full jitter" is the degenerate case of decorrelated with no memory;
> decorrelated dominates it.
>
> **`total-timeout-ms` is enforced** (an earlier cut parsed it but never honored it -- fixed in
> self-review). The loop stops before sleeping if the next backoff would push cumulative elapsed time
> past `total-timeout-ms`, mirroring Java, with an `attempt > 0` guard so at least one retry always
> happens regardless of the timeout.
>
> **`Retry-After` is honored** (Java's `ExponentialHttpRequestRetryStrategy` does this too). When a
> 429/503 response carries a `Retry-After` header, the server-requested wait takes precedence over the
> computed backoff, capped at `max_wait_ms`. `ParseRetryAfterMs` (a pure function, unit-tested in 5.1)
> handles both the delta-seconds and the HTTP-date forms.

**C4. Reuse one read-modify-write path with the existing #728 rewrite logic.**
Both the merge step and `ConstructManifestList`'s delete-driven rewrite (`iceberg_add_snapshot.cpp:125`) read, modify, and rewrite manifests. Define the order between them (carry-over/delete-rewrite first, then bin-pack merge over the result) and share the read-modify-write helper to reduce duplication and bug surface. Caveat: the shared helper must honor correction A1 (do not demote/re-sequence new `ADDED` entries), so the current `ManifestFileNeedsToBeRewritten` cannot be reused verbatim.

**C5. Refresh the local table cache after a successful commit -- ideally from the commit response.**
Java does `ops.refresh().snapshot(id)` after commit. This repo caches table state in `table_request_cache`; after a successful (possibly retried) commit, subsequent queries in the same session must not read pre-commit metadata. **Cheap path:** `IRCAPI::CommitTableUpdate` (`catalog_api.cpp:403`) currently discards the HTTP response body, but a successful Iceberg commit returns a `LoadTableResult` containing the new metadata (the repo already parses `LoadTableResult` elsewhere, `catalog_api.cpp:207-209`). Use that response to update the cache directly instead of issuing a second reload. Fall back to `table_request_cache.Expire` + reload only when the response is unavailable (e.g. multi-table endpoint, or a status-check-resolved success). Already partially done for staleness (`iceberg_transaction.cpp:480`); make it unconditional on successful data commits.

**C6. Honor `write.manifest.compression-codec` when writing manifests.**
`manifest_file::WriteToFile` sets no Avro codec (`iceberg_manifest.cpp:653-657`); Java defaults manifests to gzip (`write.manifest.compression-codec`). Uncompressed manifests are larger, which both hurts read IO and skews the bin-packing weight (`manifest_length`) relative to other engines' manifests in the same table.
> IMPLEMENTED (in-repo, no cross-repo change). Earlier this was marked BLOCKED because the `avro`
> COPY function rejects a codec option and avro-c's codec-capable writer APIs
> (`avro_file_writer_create_with_codec`) only take a local `path`/`FILE*`, bypassing DuckDB's
> FileSystem and therefore object stores (S3/Azure/GCS) -- a functional regression we will not take.
> Resolution: post-process the file the COPY function writes. `iceberg_avro_codec::RecompressManifestFile`
> (`src/core/metadata/manifest/iceberg_avro_codec.cpp`) parses the uncompressed Avro Object Container
> File, re-compresses each data block with **raw DEFLATE** (Avro's "deflate" codec) using DuckDB's
> vendored miniz directly (`duckdb_miniz`, negative window bits -- `MiniZStream::Compress` cannot be
> used as it adds a gzip wrapper), rewrites the `avro.codec` metadata key, and writes the result
> **through DuckDB's FileSystem**. Crucially, to stay object-store-safe it does *not* reopen/truncate
> the file the COPY wrote: when compression is requested, `WriteToFile` points the COPY at a temp path
> (`<final>.uncompressed.tmp`), recompresses that into the final path as a single brand-new
> `FILE_CREATE_NEW` write, then removes the temp. Object stores (S3/Azure/GCS) only support writing a
> fresh object once -- no truncate, no reopen-for-overwrite, no server-side rename (verified: S3
> rejects opening a file for both read and write, and provides no `MoveFile`) -- so this
> temp-then-fresh-write shape is the only correct one; an in-place rewrite would have thrown on S3.
> `ResolveAvroCodec` maps the table property (`gzip`/`deflate` -> deflate, `none`/`uncompressed` ->
> null, default deflate to match Java); unsupported codecs (snappy/zstd -- miniz only does deflate)
> throw rather than silently writing the wrong thing. Both `manifest_file::WriteToFile` and
> `manifest_list::WriteToFile` use it, and the manifest length used as the bin-packing weight is
> measured **after** compression so the weight reflects on-disk bytes. Verified independently: an
> uncompressed Avro file recompressed by the exact same algorithm is read back by fastavro (a
> spec-compliant non-DuckDB reader) with `codec=deflate` and byte-identical records, for both
> single-block and many-block files.
>
> ROBUSTNESS fixes from self-review: (1) the temp file is read back with the location-based
> `FileHandle::Read(buf, n, 0)`, which loops until fully read -- the plain `Read(buf, n)` can
> short-read on object stores and would have parsed a truncated buffer. (2) the OCF reader's bounds
> check guards against integer overflow (a corrupt varint length could wrap `pos + n`). (3) the temp
> `.uncompressed.tmp` is removed even when recompression throws (wrapped in try/catch + rethrow), so a
> failure does not leak an orphan that the normal cleanup path does not track.
>
> COST / why not a single write: the temp-then-recompress flow does one extra write + read + delete
> versus a hypothetical "COPY writes compressed directly". Eliminating it is **not possible in this
> repo**: the avro COPY writer's in-memory API (`avro_file_writer_create_from_writers_with_metadata`)
> takes no codec (fixing it is a cross-repo change to duckdb-avro / duckdb-avro-c), and DuckDB exposes
> no in-memory-FS protocol that would let the COPY write to a buffer instead of a temp file. The
> overhead is on small (KB-MB) manifest files and is negligible next to the REST commit round-trip,
> so it is accepted rather than worked around.
>
> RELATION TO #790 (correcting an earlier ROI doubt): manifest compression is not orthogonal to the
> merge feature. The bin-packing weight is each manifest's on-disk `manifest_length`; if this
> extension wrote uncompressed manifests while Spark/Java wrote deflate ones, the same table would
> hold mixed-codec manifests with non-comparable byte sizes, skewing the merge's size-based packing.
> Honoring the codec keeps the weight consistent across engines, so C6 actively supports #790 in
> addition to matching Java's default -- it earns its place in this PR.

**C7. Multi-table retry has no atomicity on the single-table fallback path.**
When the catalog lacks the `transactions/commit` endpoint, the code commits each table separately (`iceberg_transaction.cpp:467-478`). If table 2 conflicts after table 1 already committed, retrying table 2 cannot un-commit table 1 -- the multi-table change is not atomic on this path. **Resolution:** document this explicitly; for v1 either restrict cross-table retry to the atomic transaction endpoint, or accept and clearly state the non-atomic semantics of the per-table fallback (matching today's non-retry behavior, just with per-table retries layered on).

**C8. DuckDB's post-sink data files make retry input-replay-free (a real advantage over PyIceberg).**
PyIceberg warns that a `RecordBatchReader` input is drained once, so a naive retry after `CommitFailedException` re-appends zero rows. In this extension, data files are already written during Sink finalization (section 1.4) before commit, so retry only regenerates metadata and never needs to replay the input. Worth stating as a design advantage; no action beyond preserving this property (do not move data-file writing into the retry loop).

### E. Effectiveness and test invariants

**E1. Be honest about what the default config actually does.**
Bin-packing weighs manifests by `manifest_length` against an 8 MB target, and the `min-count-to-merge=100` guard only fires on the bin containing the new manifest. A typical INSERT produces one small (few-KB) manifest, so with defaults a merge only happens after roughly 100 commits accumulate. This matches Java/PyIceberg behavior and is *not* a bug, but it means **the automatic append-time merge is intentionally non-aggressive**; the unbounded-growth pain described in #790 is only partially addressed by defaults. Aggressive compaction in the Iceberg ecosystem is normally driven by a separate `RewriteManifests` maintenance action, not by append-time merge. **Action:** document the default behavior clearly so users do not expect automatic aggressive compaction, and note that lowering `min-count-to-merge` (or a future rewrite-manifests maintenance op) is how you get tighter manifest counts. Do not over-claim that this feature alone keeps manifest count bounded under default settings.

**E2. The core merge invariant to test: data equivalence.**
The most important property of merging is that it is a pure physical reorganization: the scan result must be identical before and after a merge. Section 5's tests count manifests and check row-id continuity, but the primary assertion must be **`SELECT *` (ordered) is identical pre- and post-merge**, including under partitioning, deletes, and V3 row lineage. Add this as the first-class invariant test.

**E3. Test multi-round merging, not just one round.**
A manifest produced by merging becomes an "existing" manifest in the next commit and can be merged again. Add tests that run several merge rounds and assert no data loss/duplication, no sequence-number drift, and no first_row_id drift across rounds. Single-round tests are insufficient.

**E4. Correctness scenarios the test matrix must add (beyond section 5).**
The current test plan does not exercise the highest-risk paths surfaced above. Add:
- DELETE manifest merge preserves each delete file's `data_sequence_number` (B17/A1): a delete merged with older delete manifests still deletes exactly the right rows -- assert row-level results, not just counts.
- Snapshot operation classification (B14): insert-only => APPEND, delete-only => DELETE, mixed => OVERWRITE in the committed snapshot summary.
- Same-transaction spec/sort-order change + data write (B12a): asserted to fail-fast (or rewrite correctly if that path is implemented).
- Commit `state unknown` (B13): simulate ambiguous failure; status-check resolves to the right success/failure and never deletes data files.
- Commit succeeded server-side but the response was lost (the most dangerous unknown path): the request landed but the client saw a timeout; status-check must find the snapshot and return success (no duplicate re-apply, no data deletion).
- Stable snapshot-id collision: a refreshed metadata already contains our chosen `snapshot_id` but it is not ours -> reroll (or abort), never adopt a foreign snapshot.
- Multi-table retry (B15): concurrent conflict on one table of a multi-table transaction retries the whole transaction and commits once; multi-table unknown is all-or-none.
- Artifact cleanup (section 8-F): after a forced mid-retry failure, no orphan manifests/manifest-lists remain, and no data file or historical source manifest is ever deleted.
- Large-bin memory (9.1): a bin near `target-size-bytes` merges without materializing all entries (guard against peak-memory blow-up).
- Delete-target-missing (B8): a delete whose target is absent in all manifests fails fast by default.

---

## 9. Where we can surpass Java / PyIceberg (not just match them)

The goal is to ship something better than the reference implementations, while staying spec-compliant and interoperable. This section lists the *differentiators* (things the others do not do) separately from the parity gaps in section 8-B8/B9 (things we must add just to not be worse).

### 9.1 Vectorized batched reads for small bins, streaming for large bins (DuckDB advantage + a lesson learned from the competition)

Java merges by reading manifests one at a time through a worker pool; PyIceberg uses an executor but is still per-manifest Python-level iteration. DuckDB-iceberg already has `AvroScan::ScanManifest`, which accepts a **vector of manifests** and reads them through the vectorized `MultiFileReader` engine. The current `ScanExistingManifestFile` (`iceberg_add_snapshot.cpp:39`) pushes only one manifest per scan, but the underlying interface supports many.

**Differentiator:** when a bin is selected for merge, hand the bin's manifests to a single `AvroScan` and read their entries in one vectorized pass, rather than looping one manifest at a time. This leverages DuckDB's columnar execution for a faster merge at essentially zero extra implementation cost (the interface already supports it).

**But here Java/PyIceberg are actually better in one respect, and we should learn from them: memory.** They read and write manifests through `CloseableIterable`, i.e. streaming one entry at a time, so peak memory is O(1) regardless of manifest size. A naive "read the whole bin into a `vector<IcebergManifestEntry>` then write" is O(bin). And the blow-up is large: a bin is capped at `target-size-bytes` (8 MB) of *compressed manifest*, but each parsed entry carries a full `IcebergDataFile` (column_sizes / value_counts / bounds maps, split_offsets, etc.), so 8 MB of manifest can expand to hundreds of MB of in-memory entries. Doing several bins at once would multiply that.

**Resolution -- hybrid, getting the best of both:**
- Process **one bin at a time** (never all bins concurrently), so peak memory is bounded by a single bin.
- For a bin whose total `manifest_length` is below a small threshold, use the vectorized batched read (fast, DuckDB-native).
- For a large bin, fall back to streaming: read entries and append them to the new manifest writer incrementally, without materializing the whole bin. This matches Java's O(1)-per-bin memory while still beating per-manifest Python iteration.
- This makes us at least as memory-safe as Java/PyIceberg and faster on the common (small-bin) case.

**Implementation prerequisite (otherwise streaming cannot be built):** today `manifest_file::WriteToFile` takes a fully materialized `vector<IcebergManifestEntry>` and computes counts / `min_sequence_number` / partition field summary in one pass over that vector (`iceberg_manifest.cpp:427`, `ManifestPartitions::Create`). Streaming requires a new incremental writer + a running aggregator that, as each entry is appended, updates added/existing/deleted counts and rows, the running `min(data_sequence_number)`, and the partition lower/upper bounds + contains_null/contains_nan -- without holding all entries. The streamed path therefore needs this aggregator implemented first; if it is not built, the design must cap merge bins at the batched threshold and document that large bins are simply not merged (still correct, just less aggressive).

**Note on the bin-packing weight.** Bin-packing weighs manifests by their on-disk `manifest_length`. This repo's `WriteToFile` does not currently set an Avro compression codec (`iceberg_manifest.cpp:653-657`), whereas other engines default to gzip, so a table can contain manifests written with different codecs and therefore different bytes-per-entry. The `manifest_length` weight is still a valid size proxy for `target-size-bytes` (the spec defines the budget in bytes), but the entry-count-per-byte is not uniform across mixed-codec manifests. Adding manifest compression is now tracked as improvement C6 (not out-of-scope); until then, keep this weight caveat in mind.

### 9.2 A more useful default than "effectively off" (honest, benchmark-gated)

As section 8-E1 documents, `min-count-to-merge = 100` means append-time merge almost never fires under defaults; Java/PyIceberg lean on a separate `RewriteManifests` maintenance action to actually bound manifest growth. We do not yet have such a maintenance action, so on this extension the default effectively leaves #790's pain unsolved.

**Differentiator (candidate):** choose a more aggressive default `min-count-to-merge` (e.g. a value in the 8-16 range, TBD) so that the read-side benefit of #790 materializes by default, instead of requiring users to tune a property or run a maintenance job. This is fully spec-compliant: `commit.manifest.*` are per-table properties that every engine may set differently, and the choice is invisible to readers (a reader does not care how entries are grouped into manifests). The win is directly the #790 goal: fewer manifests -> faster scan planning, out of the box.

**Self-critique / guardrails (why this is NOT in the design as a fixed number yet):**
- It trades commit-time IO (more frequent rewrites) for read-time speed. That trade is only justified if measured. **This must be benchmark-gated**: measure scan-planning time and commit latency across a range of `min-count` values on representative workloads before fixing a number. Do not pick a number by intuition.
- It must remain a single property the user can raise back to 100 (or any value) to get exact Java parity; we change only the *default*, not the knob.
- `target-size-bytes` (8 MB) still caps each merged manifest, so a lower count cannot create pathologically large manifests.
- **Acceptance criteria for changing the default**: only lower the default if a benchmark shows scan-planning latency improves materially (e.g. p95 down a clear margin on a many-manifest table) while commit latency does not regress beyond an agreed bound (e.g. p95 commit +<= a small fixed budget). If the trade does not clearly favor reads, keep the Java default of 100.

### 9.3 Explicitly rejected ideas (to prevent scope creep)

- **Adaptive / heuristic merge triggering** (merge based on growth rate or small-file ratio instead of a fixed count): rejected. It diverges from the predictable, spec-defined behavior other engines expect, is hard to test and explain, and is out of scope. The right lever is the existing property plus a sane default (9.2), not a new heuristic.
- **SQL-queryable merge/retry observability** (table functions reporting "how many manifests merged / how many retries"): a genuine DuckDB-only nice-to-have, but out of scope for #790/#786. Listed here only as possible future work; not part of this PR.

### 9.4 Parity items, for the record

- **Retry itself is already a differentiator over PyIceberg**, which has no optimistic-concurrency retry loop at all (a conflict aborts immediately). Implementing section 3-D brings us to Java parity and ahead of PyIceberg; the unified design (merge as a pure step of `apply`, re-run each attempt) is the same shape as Java, so this is parity-with-Java rather than a new advantage -- noted so we do not over-claim it.

---

## 10. Real-catalog hardening, connection caching, and performance (post-implementation)

Everything above was the design; this section records what changed once the implementation was
exercised against **real catalogs** (AWS S3 Tables, and a local Docker `apache/iceberg-rest-fixture`
+ MinIO), plus the performance investigation. These are not hypotheticals -- each item below was
observed on a running catalog and fixed/validated.

### 10.1 Bugs found only against a real REST catalog (S3 Tables), all fixed

1. **Stale `ClientContext` in `RefreshForRetry` / `CacheExistingManifestList` -> S3 secret resolution
   crash.** The retry refresh re-reads the parent manifest list, which resolves storage secrets; the
   `transaction_data`'s captured operator-time context is no longer active at commit time. Fix: thread
   the live commit-time `context` into `RefreshForRetry(context)` and `CacheExistingManifestList(...,
   context)` (mirrors Java committing against one live context throughout).
2. **Manifest filename reuse across retry attempts -> HTTP 412.** Object stores that create with
   If-None-Match (S3 Tables, `FILE_FLAGS_FILE_CREATE_NEW`) reject re-writing the same object. Each
   attempt now mints a fresh UUID path in `WriteManifestListEntry` / `RewriteManifestFile` /
   `WriteManifestListEntry`'s manifest-list path, so every attempt writes a never-before-seen object.
3. **Commit path used read-isolation `GetSchemaVersion` -> `1970` metadata-log error.** The commit
   must target the *current* (refreshed) metadata, not the as-of-transaction-start metadata the read
   path rewinds to via the metadata log. Added `IcebergTableInformation::GetCurrentSchemaEntry()` (no
   snapshot-isolation rewind) and use it on the commit path. Mirrors Java's SnapshotProducer applying
   against the freshly refreshed base.

### 10.2 Request-count reductions (validated on S3 Tables)

Steady-state INSERT on S3 Tables went from **7 -> 5 HTTP requests** per commit:
- **HEAD eliminated.** `manifest_file::WriteToFile` previously read the file size back (a HEAD round
  trip on object stores) to set `manifest_length`. The `duckdb-avro` fork now implements
  `copy_to_get_written_statistics` (a `bytes_written` counter funneled through the single `WriteData`
  chokepoint), so the uncompressed path takes the byte count directly from the COPY; the compressed
  path already got it from `RecompressManifestFile`. A graceful fallback to `GetFileSize()` remains if
  a COPY without that capability is ever used.
- **Redundant table GET eliminated.** On a successful single-table commit, the Iceberg REST
  `UpdateTable` response carries the full new `LoadTableResult`. `CommitTableUpdate` now parses it and
  refills `table_request_cache` (C5), so the next operation reuses fresh post-commit metadata instead
  of issuing a GET. Best-effort: an empty/partial body or a parse error just leaves the normal GET
  path to refresh (the commit already succeeded). Consumed only when `max_table_staleness` is set
  (otherwise the cache entry is stored already-expired and the GET happens anyway). The multi-table
  path expires the cache instead (its response shape is catalog-dependent and not parsed per-table).

The default steady-state INSERT is thus: GET parent-manifest-list + PUT data + PUT manifest + PUT
manifest-list + POST commit = 5 requests, matching Java; 6 without `max_table_staleness` (keeps the
table GET). DROP/RENAME are unchanged (separate endpoints, no retry, out of scope).

### 10.3 HTTP connection reuse: a real workaround, and the SQLite-fixture CAS limitation

**Connection caching is the single biggest latency lever, and it is a setting, not code.** DuckDB's
httpfs opens a fresh TCP+TLS connection per file request by default; for a 5-request commit that is 5
TLS handshakes. The httpfs fork in use ships a sharded, cross-request, per-host connection pool
(`httpfs_connection_caching.cpp`) gated by `SET httpfs_connection_caching=true` (default off). With it
on, a commit's requests to the same host reuse one keep-alive connection (verified:
`connection_cache_hit=5, miss=0` for a 5-request commit), roughly halving wall time on a
cross-region/high-RTT link. **Recommendation for the thousands-of-Lambdas workload: enable it.** This
is upstream DuckDB behavior (tracked in duckdb-httpfs issue #292, "better TCP connection pool"); the
iceberg extension cannot change the per-request connection model, only reduce request count (10.2) and
recommend the setting.

**SQLite-backed test catalogs cannot enforce optimistic concurrency.** The local Docker
`apache/iceberg-rest-fixture` uses a `jdbc:sqlite` backend. Under true parallel commits it returns
HTTP 200 to commits that should get a 409 (the compare-and-swap on the catalog row is not reliably
isolated under SQLite locking), silently dropping the losing commits. This is **not** an extension
bug: the dropped commits never see a 409, so no client retry can recover them; on a real CAS catalog
(AWS S3 Tables, Lakekeeper/Postgres) the same workload is lossless. Consequence for tests: the two
`test_commit_retry_concurrent*` tests are gated to `LAKEKEEPER_SERVER_AVAILABLE` (section 5.3), and
concurrent correctness is validated on S3 Tables (up to 50-way, distinct-count == row-count, zero
loss). Sequential and low-contention commits pass on the SQLite fixture fine; only true-parallel
conflict resolution needs a real catalog.

### 10.4 Performance reality (so nobody mis-diagnoses it later)

- **The per-commit cost is dominated by RTT x request-count plus the catalog's commit POST
  processing**, not by anything in the merge/retry code. On a cross-region/home link (~40 ms RTT,
  worse through a corporate VPN that DNS-hijacks AWS to a local proxy and re-does TLS per request) a
  commit is hundreds of ms; in-region (Lambda, sub-ms RTT, connection caching on) it is ~tens of ms
  with the catalog POST (~100-150 ms server-side) as the floor.
- **Single-ref optimistic concurrency is serial by nature.** N concurrent writers to one table ref
  produce at most one winner per round; the others retry. So "1000 Lambdas hammering one ref" is
  bounded by N x (one successful commit cycle), regardless of `num-retries`. The fix for genuine
  high-throughput is to **shard writes across multiple tables/refs** (then compact), not to tune
  retries. Tuning `commit.retry.max-wait-ms` smaller mainly reduces the tail (a writer that lost many
  rounds), not the median (which is the serial commit cycle).
- **`commit.retry.num-retries` default is 4 (Java parity).** For thousands of concurrent writers on
  one ref this is far too low -- raise it explicitly (hundreds), and note that on a weak (SQLite) test
  catalog raising it does nothing because losses surface as false 200s, not 409s. This is a
  per-table property, deliberately not changed from Java's default (a different default is a
  benchmark-gated decision, like 9.2).

---

## 11. Final review round: fixes, simplifications, and test additions

A full re-read of the implementation after it was working produced the following. Each was verified
(compile + logic tests + real-catalog run) before being kept.

### 11.1 Correctness fixes
- **B14 snapshot operation classification** -- see B14 STATUS. Was hard-coded OVERWRITE; now the
  caller-declared operation (append/delete/overwrite), verified on S3 Tables.
- **B1 pre-merge manifest orphan tracking** -- see B1 STATUS. Pre-merge manifests that the merge
  repacks into new files are now recorded in `written_metadata_paths` as they are written (then the
  manifest-list and merge products, de-duplicated), so a failed/aborted commit cleans them up instead
  of leaking (delete-capable catalogs only; S3 Tables GCs its own).
- **`Retry-After` integer overflow** -- `ParseRetryAfterMs` multiplied untrusted seconds by 1000,
  which could overflow int64 and wrap to a negative/tiny wait. Now clamps to one day before the
  multiply. Unit-tested (huge value -> one day; exact-day boundary).
- **Multi-table retry config** -- the retry loop read `commit.retry.*` from an arbitrary "first"
  table. It now folds every table's config via `IcebergRetryConfig::MostLenient` (max num-retries /
  max-wait / total-timeout, min min-wait, `min<=max` preserved), so no table's more-permissive policy
  is silently dropped on the atomic multi-table path. Unit-tested.

### 11.2 Simplifications (behavior-preserving)
- **`SendCommitPost` + `ThrowCommitResponseError` helpers** in `catalog_api.cpp`. The single- and
  multi-table commit functions had byte-identical blocks for (a) converting a transport-level throw to
  `IcebergCommitException(UNKNOWN)` and (b) classifying a non-2xx response + honoring `Retry-After`.
  This safety-critical logic is now defined once (a divergence between the two paths could leak files
  or mis-retry). Net ~30 fewer lines, no behavior change.
- Removed leftover per-commit diagnostic logging (`cache refill OK ... body=N bytes`, SKIP, FAILED)
  from the cache-refill path; the refill itself stays.

### 11.3 Test additions / changes
- C++ logic tests (`test/cpp/test_merge_retry_logic.cpp`, `make test_logic`, in CI): added mirrors +
  cases for `DecorrelatedBackoffMs`, retry-config parsing (defaults / num-retries=0 / negative-garbage
  / min>max), `MostLenient`, `total-timeout-ms` enforcement, `Retry-After` consumption, and the
  `Retry-After` overflow clamp. All pass.
- `test_merge_append.test` strengthened: asserts the **exact** merged manifest count (=1, not a `<=`
  bound) and that `EXISTING`-status entries appear (proving the ADDED->EXISTING demotion during
  merge). Calibrated on S3 Tables (manifest=1, ADDED:1 EXISTING:2).
- `test_commit_retry_concurrent_delete.test` (new): drives the `ValidateRetrySafe` FATAL paths E2E
  (section 5.3), gated to Lakekeeper.
- `commit_status_check_none_committed.test` (new) + the `inject_fail_before_apply_once` addon marker:
  covers the `UNKNOWN -> NONE_COMMITTED -> retry-to-success` path (section 5.3).
- Both `test_commit_retry_concurrent*` gated to `LAKEKEEPER_SERVER_AVAILABLE` (the SQLite fixture
  cannot pass them, section 10.3).
- A **fragile assertion was deliberately reverted**: an attempt to assert the on-disk Avro codec in
  `test_manifest_compression.test` via `read_blob` failed because (a) `read_blob` does not accept a
  subquery/lateral-join path argument in sqllogictest, and (b) the codec is catalog-dependent (S3
  Tables forces uncompressed via `allows_deletes=false`). The on-disk codec IS verified on a
  delete-capable catalog by the Docker Node test harness, and the recompression algorithm is verified
  offline against fastavro (5.1); a portable, non-fragile SQL assertion was not worth forcing.

### 11.4 Things deliberately NOT changed (to avoid over-engineering)
- **`WriteAvroGlobalState::BytesWritten()` lock-free read** in the avro fork: harmless under the
  current single-threaded `REGULAR_COPY_TO_FILE` mode (the read happens after all writes complete).
  Making it atomic would add overhead to the hot `WriteData` path for a race that cannot occur unless
  the writer is parallelized (which would require broader changes anyway). Flagged, not changed.
- **B1 write-once optimization** (decide merge layout before writing): the orphan it avoids is already
  tracked + cleaned; restructuring the apply path to save one small-file write only when a merge fires
  is cost > benefit. Deferred (see B1 STATUS).
- **Multi-table partial-commit UNKNOWN E2E test** (section 5.4): cannot be constructed faithfully with
  the available harness; the branch never deletes files, so the safety risk is nil.
