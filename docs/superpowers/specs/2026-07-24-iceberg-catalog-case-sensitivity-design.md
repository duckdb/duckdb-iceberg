# Iceberg catalog identifier case sensitivity — design

> **Note on this revision:** this design was originally written against an older
> release branch. The implementation was rebased onto `main`, and this document
> has been updated to
> match the actual current architecture, which changed substantially in the
> interim (a unified `IcebergTransactionTableState`/`IcebergTableStatus`
> mechanism replaced the old three-map transaction bookkeeping, table rename
> support was added, and a new schema-property-update path was added). The
> core problem diagnosis and design decisions below are unchanged; the file/
> line citations and some mechanism descriptions have been refreshed.

## Problem

The duckdb-iceberg extension has an unintentional, inconsistent case-sensitivity
behavior for catalog/schema/table identifier lookups against a REST catalog.
**Critically, the actual mechanism differs between schema/namespace lookups
and table lookups.** This was caught by direct testing against a real attached
Iceberg catalog: table reads came back case-sensitive, contradicting an
initial "warm cache makes it insensitive" theory. Tracing the actual code
confirmed why.

**Schema/namespace lookups** (`IcebergSchemaSet::GetEntry`,
`src/catalog/rest/iceberg_schema_set.cpp`) genuinely follow a
cold-sensitive/warm-insensitive split:
- Cold (nothing cached, and only for the `DEFAULT_SCHEMA` case — non-default
  schemas aren't even verified eagerly, see the "Deferred non-default-namespace
  existence check" edge case below): the literal user-typed name is sent
  straight to `VerifySchemaExistence`, which 404s on a case mismatch against
  a real (exact-string) Iceberg REST catalog.
- Warm (schema already in `entries`, e.g. from a prior listing or lookup):
  `entries.find(name)` — a `case_insensitive_map_t` — returns the cached
  entry directly, **with no re-verification**, so a case-mismatched *cached*
  lookup succeeds regardless of casing.

**Table lookups** (`IcebergTableSet::GetEntry`,
`src/catalog/rest/iceberg_table_set.cpp`) do **not** follow this split — they
are effectively always case-sensitive on real access, and prior listing does
not change this. `GetEntry` delegates cache insertion/replacement to a helper,
`IcebergTableSet::CreateEntryInternal`:

```cpp
auto it = entries.find(name);
if (it != entries.end()) {
    old_entry = std::move(it->second);
    it->second = make_shared_ptr<IcebergTableInformation>(std::move(table));
} else {
    it = entries.emplace(name, make_shared_ptr<IcebergTableInformation>(std::move(table))).first;
}
```

Even though `entries` is a `case_insensitive_map_t` and `entries.find(name)`
*would* match a previously-listed table under different casing, this helper
is invoked from `GetEntry` with the caller's freshly-constructed
`IcebergTableInformation(ic_catalog, schema, table_name)` — built from the
literal, possibly-wrong-case `table_name` the user typed. Any pre-existing
match is overwritten in place with this literal-cased object, which is then
fetched fresh via `IRCAPI::GetTable(..., table.name)` using that same literal
casing. Against a real (exact-string) REST catalog, this 404s. A `SHOW
TABLES` beforehand does not help — the correctly-cased cache entry it
populated is exactly what gets overwritten here.

The *only* place tables show any insensitivity today is a narrow, accidental
edge case: `GetEntry` first checks
`iceberg_transaction.GetLatestTableState(table_key)` (backed by
`current_table_data`, a `case_insensitive_map_t<IcebergTransactionTableState>`
on `IcebergTransaction`). If this exact table was already resolved earlier in
the *same transaction* under some other casing, the case-folded `table_key`
match returns the already-resolved state directly, without re-fetching —
reusing the correctly-loaded entry regardless of casing. This only fires for
a *repeat* access within one transaction after a prior *successful* access; a
first-touch lookup with the wrong casing (the common case, and what
"insensitivity doesn't apply" is actually describing) always hits the
overwrite-and-refetch path above and fails.

There's also a pre-existing internal inconsistency independent of the above,
in `IcebergTransaction` (`src/include/catalog/rest/transaction/iceberg_transaction.hpp`):
`created_schemas`/`deleted_schemas` are plain (case-sensitive)
`unordered_set<string>`, while `listed_schemas`/`looked_up_entries` are
`case_insensitive_set_t` (case-insensitive), and `current_table_data` (the
unified table-state map) is `case_insensitive_map_t`. A newer field,
`schema_property_updates` (`case_insensitive_map_t<SchemaPropertyUpdates>`,
added to support `set_iceberg_schema_properties`/
`remove_iceberg_schema_properties`), has the same shape. Under
`case_sensitive=true`, `current_table_data` being unconditionally
case-insensitive is a real bug in its own right (see §2 below): dropping
table `Foo` then querying distinct table `foo` in the same transaction would
incorrectly report `foo` as dropped, because the state map folds case
regardless of the catalog's requested mode. A second, structurally identical
map lives on `IcebergTransactionAlterUpdate` (a per-alter-block struct in
`src/include/catalog/rest/transaction/iceberg_transaction_update.hpp`):
`updated_tables` (`case_insensitive_map_t<IcebergTableInformation>`) and
`committed_tables` (`case_insensitive_set_t`), used while building and
committing a batch of table alterations.

Separately, a field literally named `case_sensitive` already exists in this
repo (`src/include/rest_catalog/objects/plan_table_scan_request.hpp`). This is
the Iceberg REST OpenAPI spec's `PlanTableScanRequest.case-sensitive` field,
governing case sensitivity of **column-name matching in server-side
scan-planning filter pushdown**. It is unrelated to catalog/table identifier
lookup and currently unwired — this extension never calls the
plan-table-scan endpoint. Noted here only so it isn't confused with this
work.

## Goal

Add a `case_sensitive` boolean ATTACH option that makes catalog/schema/table
identifier matching behave consistently and correctly according to the
requested mode, cold or warm.

## Scope

**In scope:** catalog/schema/table (namespace) identifier resolution only.

**Explicitly out of scope: column names.** `SELECT MyCol FROM tbl` binds
against DuckDB core's `Binding::name_map`
(`duckdb/src/include/duckdb/planner/table_binding.hpp`) and `ColumnList::
name_map` (`duckdb/src/include/duckdb/parser/column_list.hpp`), both
hardcoded `case_insensitive_map_t` members consulted through **non-virtual**
methods (`Binding::TryGetBindingIndex`, `ColumnList::GetColumnIndex`/
`GetColumn`), and `BindContext::AddBaseTable` unconditionally constructs a
concrete `TableBinding` — there is no factory/virtual seam for an extension
to substitute its own binding class. This is a real limitation we're
accepting for now, not solving here; it would require a DuckDB core patch,
not an extension change. (Scan/filter pushdown is unaffected either way:
`IcebergMultiFileReader` matches columns by numeric Iceberg field-id, not by
name string.)

**Setting scope: per-ATTACH only**, not a global `SET`. The extension does
have precedent for global extension-wide settings
(`config.AddExtensionOption` in `src/iceberg_extension.cpp`:
`unsafe_enable_version_guessing`, `iceberg_via_aws_sdk_for_catalog_interactions`,
`iceberg_test_force_token_expiry`), but we're deliberately not using that
pattern here — different attached catalogs may reasonably want different
casing rules, and there's no requirement for a uniform default across all of
them.

**Default: unset changes nothing.** If `case_sensitive` is not passed to
ATTACH, behavior is exactly what it is today (cold-sensitive,
warm-insensitive-by-cache-accident for schemas; effectively-always-sensitive
for tables). No regression risk for existing users/queries. Correctness
guarantees only apply once a user explicitly passes `case_sensitive=true` or
`case_sensitive=false`.

## Design

### 1. ATTACH option

Add `case_sensitive` (bool, tri-state: unset / true / false) to
`IcebergAttachOptions` (`src/include/iceberg_attach.hpp`), parsed in
`IcebergAttach` (`src/iceberg_attach.cpp`)
alongside the existing options, following the same parsing convention
(`else if (lower_name == "case_sensitive") { ... }`).

### 2. Storage: `CaseAwareIdentifierMap<T>` and `CaseAwareIdentifierSet`

Two small wrapper classes — one map-shaped (templated on the value type
`T`), one set-shaped — each holding *either*:

- a real `duckdb::case_insensitive_map_t<T>` / `case_insensitive_set_t`
  (reusing DuckDB core's existing, battle-tested case-insensitive containers
  as-is — not reimplementing case folding), when the catalog's resolved mode
  is insensitive, or
- a plain `std::unordered_map<string, T>` / `std::unordered_set<string>`,
  when the mode is sensitive.

The backend is chosen once, at construction, from the catalog's resolved
`case_sensitive` value (falling back to whatever the *current* default
container choice is per call site, when unset — see "Default" above).

**Portability note discovered during implementation:** on at least one
standard library (libc++), `unordered_map<string, T>::iterator` and
`case_insensitive_map_t<T>::iterator` are the *same underlying type* — the
iterator/node type does not depend on the Hash/KeyEqual template parameters.
This means the natural design of two constructors overloaded only on these
parameter types fails to compile ("constructor cannot be redeclared"). The
wrapper's iterator classes use distinctly-*named* static factories
(`MakeSensitive`/`MakeInsensitive`) instead of overloading, which is correct
regardless of whether a given std lib unifies the two iterator types or not.

**The wrapper must carry a tri-state mode, not just a binary backend
choice.** Container backend (insensitive vs. sensitive) is binary, but for a
per-field default-insensitive container, "insensitive because unset" and
"insensitive because `case_sensitive=false` was explicitly requested" use the
*identical* backend, yet must behave differently for collision detection
(§4): explicit insensitive mode raises an ambiguity error on a same-fold
collision; unset must silently overwrite exactly as it does today, per
Scope's "unset changes nothing" guarantee. The wrapper therefore stores which
of `{Unset, Sensitive, Insensitive}` it was constructed with, uses that to
pick the container backend (`Unset` and `Insensitive` both select the
case-insensitive container; `Sensitive` selects the plain one), but only
performs the §4 collision check on insert when its stored mode is
`Insensitive` — never for `Unset`, even though they share a backend.

Two distinct backends (rather than always storing case-insensitively and
layering an exact-match check on top) matter for correctness: Iceberg's spec
permits two case-distinct names (`Foo` and `foo`) to coexist server-side in
the same namespace. If we always stored in a `case_insensitive_map_t`
regardless of mode, inserting both would collide/overwrite in the cache
before an exact-match check ever ran. Genuinely separate plain containers for
sensitive mode avoid this.

**Applies to:**
- `CaseAwareIdentifierMap<T>`:
  - `IcebergSchemaSet::entries` (`unique_ptr<CatalogEntry>`),
    `IcebergTableSet::entries` (`shared_ptr<IcebergTableInformation>`).
  - `IcebergTransaction::current_table_data`
    (`IcebergTransactionTableState`) — the unified per-transaction table
    state map that replaced the old separate deleted/updated/requested
    tracking. This is where the cross-case drop-then-query bug in the
    Problem section lives.
  - `IcebergTransactionAlterUpdate::updated_tables`
    (`IcebergTableInformation`) — the per-alter-block table map, constructed
    from `transaction.GetCatalog().attach_options.case_sensitivity_mode`
    (this struct doesn't have direct access to `IcebergTransaction::catalog`,
    which is private, so it goes through the public `GetCatalog()` accessor).
  - `IcebergTransaction::schema_property_updates` (`SchemaPropertyUpdates`) —
    keyed by schema name, backing `set_iceberg_schema_properties`/
    `remove_iceberg_schema_properties`. Not present in the original version
    of this design; included now because it's a direct structural analog of
    `created_schemas`/`deleted_schemas` and is squarely a schema-identifier
    concern.
- `CaseAwareIdentifierSet`:
  - `IcebergTransaction::created_schemas`/`deleted_schemas`
    (currently plain `unordered_set<string>`) and `listed_schemas`/
    `looked_up_entries` (currently `case_insensitive_set_t`) — closing the
    pre-existing case-sensitive/insensitive inconsistency between these four
    fields as part of this change.
  - `IcebergTransactionAlterUpdate::committed_tables`.
- **Not migrated, deliberately:**
  - `IcebergTransaction::tables` (`case_insensitive_map_t<shared_ptr<
    IcebergTableInformation>>`) — write-only/keep-alive (referenced tables
    that must stay alive for the transaction's duration); never looked up by
    name, so case-sensitivity of its keys has no observable effect.
  - `TableTransactionInfo::table_requests` (`case_insensitive_map_t<idx_t>`,
    local to `IcebergTransaction::GetTransactionRequest`) — a transient,
    single-commit-call structure correlating already-resolved table keys to
    request indices; not a user-facing identifier resolution path.
  - `IcebergTransactionAlterUpdate::committed_tables` is migrated (see
    above), but the local `TableTransactionInfo` it interacts with is not,
    per the previous bullet.

**External consumers that also need updating**, since they touch these
fields with raw STL semantics rather than through the owning class's own
methods:
- `entries`: `tables.GetEntries().find(table_name)` /
  `tables.GetEntriesMutable().erase(table_name)` in
  `src/catalog/rest/catalog_entry/schema/iceberg_schema_entry.cpp`
  (`.find`/`.end`/`.erase` work unchanged on the wrapper).
  Pure-iteration consumers (`for (auto &it : schema_set.GetEntries())` in
  `src/function/ducklake/iceberg_to_ducklake.cpp`) need no change at all,
  since `begin()`/`end()` are part of the wrapper's normal surface.
- `created_schemas`/`deleted_schemas`: populated via `.insert()` in
  `IcebergCatalog::CreateSchema`/`DropSchema`
  (`src/catalog/rest/iceberg_catalog.cpp`) — the only insertion points for
  these two sets; a `.find()==.end()` existence check in
  `IcebergSchemaEntry::CreateTable` (`iceberg_schema_entry.cpp`) needs to
  become `!contains(name)`, since the set wrapper doesn't expose `find`/`end`.
- `schema_property_updates`: `src/function/metadata/
  iceberg_schema_properties_functions.cpp` uses `.find()`/`.end()`/
  `operator[]` on this map. All three need to exist on the wrapper (added
  `operator[]`, mirroring `unordered_map`'s own semantics: it looks up via
  the backend's own hash/equality, so on the insensitive backend it can only
  ever find-or-create the *same* case-folded slot — it cannot introduce a
  new case-distinct entry, so it's safe to leave it bypassing the §4
  ambiguity check entirely). This file needed **no source changes** — every
  call it makes is already covered by the wrapper's surface.
- `IcebergTableSet::CreateEntryInternal`'s rollback path,
  `entries[table_name] = std::move(old_version)`, uses `operator[]` for the
  same reason: it's overwriting a slot for a key already known to exist (the
  branch is only reached when an existing entry was already found and
  displaced earlier in the same call), not inserting a new identifier.
- `IcebergTransactionAlterUpdate::HasUpdates() const` iterates
  `updated_tables` from a `const` method, which requires const-qualified
  `begin()`/`end()`/`find()`/`contains()`/`count()` on the map wrapper — the
  original (pre-redo) version of this wrapper only had non-const overloads,
  which would fail to compile here. The wrapper needs a proper
  `const_iterator` mirroring the mutable `iterator`.

The wrapper's method surface follows STL container naming:
`CaseAwareIdentifierMap<T>` needs `insert`, `emplace` (returning
`pair<iterator, bool>`), `find`, `contains`, `count`, `at`, `operator[]`,
`erase` (key- and iterator-based), `empty`, `clear`, plus const overloads of
the read-only members. `CaseAwareIdentifierSet`
needs `insert` (returning a bool "was newly inserted"), `contains`, `count`,
`erase`, `empty`, `clear`, and iteration.

### 3. Cold-path REST lookup behavior

The storage wrapper only helps once something is cached — and per the
Problem section, "cached" doesn't even help *tables* today because of the
overwrite-and-refetch pattern in `IcebergTableSet::CreateEntryInternal`. This
section covers the REST round trip taken on a genuine first-touch lookup,
which is where the fix for tables actually has to happen:

- **`case_sensitive=true` (explicit):** for schemas, unchanged from today —
  a single direct GET/HEAD with the literal user-typed casing
  (`VerifySchemaExistence`), no extra round trip; we rely on the REST
  catalog's own spec-compliant exact-string matching to 404 on a mismatch,
  and don't add a defensive echoed-name check (no response body/name field
  exists to check against). **For tables, this mode requires no code change
  to the core `GetTable` path at all** — the overwrite-and-refetch pattern
  already described in Problem *is* correct, exact-string behavior; it just
  needs the transaction-bookkeeping fix in §2 (`current_table_data` and
  `IcebergTransactionAlterUpdate`'s maps becoming sensitive-mode-aware) so
  the narrow same-transaction-repeat insensitivity edge case can't leak
  through either.
- **`case_sensitive=false` (explicit):** call the namespace/table **List**
  endpoint first, scan returned names case-insensitively for a match, then
  proceed using the **canonical, server-returned name** — not the user's
  literal string. This matters most for tables: `IcebergTableSet::GetEntry`
  now checks `entries.find(table_name)` for an existing case-insensitive
  cache hit first — if found, uses that entry's canonical key
  (`cached->first`) for everything downstream; only when nothing is cached
  does it call the new `ResolveCanonicalNameViaList` helper (lists tables via
  `IRCAPI::GetTables`, matches case-insensitively, throws on ambiguity) to
  get the canonical name before constructing/committing the
  `IcebergTableInformation`. For schemas, the equivalent
  `ResolveCanonicalNameViaList` helper lists namespaces via `IRCAPI::
  GetSchemas`; `IcebergSchemaSet::GetEntry` already returns a cached
  `entries.find(name)` match without re-verifying, so making the *cold* path
  do a List-then-canonical-match brings cold and warm behavior in line with
  each other. This is an accepted extra round trip on cold lookups, traded
  for correctness (agreed: correctness over speed).
- **Unset (default):** unchanged for both — schemas keep today's
  cold-sensitive/warm-insensitive-via-cache split; tables keep today's
  effectively-always-sensitive behavior (including the narrow
  same-transaction-repeat edge case). No regression risk.

Once a canonical name has been resolved (explicit insensitive mode) or a
schema has been cached (any mode, per today's behavior), results populate
the relevant `CaseAwareIdentifierMap`, so same-transaction subsequent lookups
under that canonical name are served locally.

### 4. Error handling & edge cases

- **Ambiguous case-insensitive match — both the single-lookup and the
  bulk-listing path:** if List returns multiple names that fold to the same
  case-insensitive key (e.g. both `Foo` and `foo` exist), this is ambiguous
  under `case_sensitive=false` in two places, not just one:
  - The single-name List-then-match cold lookup (§3): looking up `foo`
    resolves ambiguously among candidates — `ResolveCanonicalNameViaList`
    throws directly when it finds two differently-cased matches.
  - **Bulk listing itself** (`IcebergSchemaSet::LoadEntries`/
    `IcebergTableSet::LoadEntries`, used by `Scan()` for `SHOW TABLES`/
    `SHOW SCHEMAS` and by incidental listing): this populates the same
    case-insensitive container via ordinary insert/emplace calls. Without an
    explicit check, inserting both `Foo` and `foo` here would silently
    collide/drop one entry via normal map insert semantics — which directly
    contradicts "raise an error rather than silently picking one." The
    insert path must detect a same-fold collision at insert time (not just
    at lookup time) and raise the same ambiguity error, so `SHOW TABLES`
    under `case_sensitive=false` against a namespace with case-colliding
    names fails loudly instead of quietly hiding one of them.

  In both cases: raise a clear catalog error naming all matching candidates
  and suggesting `case_sensitive=true` or an exact-case reference, rather
  than silently picking one or dropping one.

  **Critically, this collision check is gated by the wrapper's tri-state
  mode (§2), not by "which container backend is active."** `entries` and
  several other fields use the case-insensitive container both when
  `case_sensitive=false` is explicit *and* when the option is left unset
  (today's default). The collision check must fire only when the wrapper's
  stored mode is `Insensitive` (explicit). Under `Unset`, even though the
  same `case_insensitive_map_t` backend is in use, insert must behave
  exactly as it does today — silently overwrite/collide on same-fold names —
  per Scope's "unset changes nothing" guarantee and the "Unset + incidental
  listing" bullet below. Sensitive mode's plain container never hits this
  path at all, since it keeps case-distinct entries physically separate.
- **Mixed catalogs in one query:** well-defined without special handling —
  the option is per-ATTACH, so each catalog resolves under its own rule; no
  cross-catalog ambiguity.
- **Unset + incidental listing:** if a listing happens under the default/
  unset mode for an unrelated reason (e.g. `SHOW TABLES`), behavior is
  unchanged from today, and today's behavior differs by kind (see Problem):
  for **schemas**, the `case_insensitive_map_t`-backed cache genuinely gives
  incidental insensitive matching once warm; for **tables**, listing does
  *not* grant insensitive matching — a first-touch lookup with mismatched
  casing still hits the overwrite-and-refetch path and 404s, regardless of a
  prior `SHOW TABLES`. This asymmetry is deliberately preserved as-is under
  `Unset`, since "unset changes nothing" is the guarantee, not "unset means
  consistently insensitive."
- **Namespace vs. table:** one mode applies uniformly to both namespace and
  table identifiers under a given catalog; Iceberg doesn't distinguish
  between them either.
- **Table rename (`ALTER TABLE ... RENAME TO`):**
  `IcebergTransaction::RenameTable` takes the literal `new_name` supplied by
  the user and creates a brand-new table entry under that exact name — this
  is a creation, not a lookup of an existing possibly-mistyped identifier, so
  it deliberately does *not* go through canonical-name resolution regardless
  of mode (same reasoning as `CREATE TABLE`).
- **Deferred non-default-namespace existence check:** today,
  `IcebergSchemaSet::GetEntry` only calls `VerifySchemaExistence` for the
  `DEFAULT_SCHEMA` branch; for any other schema name, namespace existence
  isn't actually verified up front — it's checked lazily, only after a table
  lookup under that schema fails, in `IcebergSchemaEntry::LookupEntry`
  (`src/catalog/rest/catalog_entry/schema/iceberg_schema_entry.cpp`), using
  whatever `name` the schema entry was already constructed with (today,
  literally as typed). This design's `case_sensitive` mode governs the eager
  `DEFAULT_SCHEMA` path and the table-lookup path directly; the lazy
  non-default-namespace check inherits correct behavior automatically as a
  side effect, since it re-uses the same schema entry's `name` and the same
  (now mode-aware) table lookup — no separate handling needed.

## Testing plan

- **Unit (`CaseAwareIdentifierMap` / `CaseAwareIdentifierSet`):** insensitive
  backend folds case on insert/lookup (delegates to `case_insensitive_map_t`/
  `case_insensitive_set_t`); sensitive backend keeps `Foo`/`foo` distinct;
  `operator[]` on the insensitive backend cannot create a second case-distinct
  entry for an already-present fold; const iteration works from a const
  reference to the wrapper.
- **SQL/integration** (reusing this repo's existing REST catalog test
  harness):
  - Cold lookup, `case_sensitive=true`, schema and table: wrong-case name
    still fails as "does not exist" (regression guard — unchanged from
    today for both kinds).
  - Cold lookup, `case_sensitive=false`, schema and table: wrong-case name
    resolves correctly via list-then-canonical-match. This is the actual
    bug fix, and matters most for **tables**, where today even a prior
    listing doesn't help (see Problem) — the table case is the primary
    regression to guard here, not just the schema case.
  - Warm lookup after `SHOW TABLES`/`SHOW SCHEMAS`, `case_sensitive=true`:
    still requires exact case even though the name was listed — for tables
    this matches today's behavior already; for schemas this is a *new*
    guarantee this design adds.
  - Warm lookup after `SHOW TABLES`/`SHOW SCHEMAS`, `case_sensitive=false`:
    matches regardless of casing for both kinds.
  - Unset, table, cross-case lookup after `SHOW TABLES`: still fails as
    "does not exist" — a regression guard specifically for the asymmetry
    noted in Problem.
  - Ambiguous match on single-name cold lookup (two case-distinct names,
    `case_sensitive=false`): expect the clear ambiguity error, not a silent
    pick.
  - Ambiguous match on bulk listing (`SHOW TABLES`/`SHOW SCHEMAS` against a
    namespace with two case-distinct names, `case_sensitive=false`): expect
    the same ambiguity error, not a silently-dropped entry.
  - Unset/default: existing test suite passes unchanged.
- **Transaction bookkeeping:**
  - `CREATE SCHEMA`/`DROP SCHEMA` under both modes, confirming the
    now-unified `created_schemas`/`deleted_schemas` behavior doesn't
    regress.
  - Cross-case drop-then-query within one transaction under
    `case_sensitive=true`: drop table `Foo`, then query distinct table
    `foo` in the same transaction — must succeed (not be wrongly reported
    as dropped via `current_table_data`). This directly tests the fix to
    `IcebergTransaction::current_table_data` and
    `IcebergTransactionAlterUpdate::updated_tables`/`committed_tables`.
  - `SET`/`REMOVE` table properties (`set_iceberg_table_properties`/
    `remove_iceberg_table_properties`, which go through `ApplyTableUpdate` →
    `IcebergTransactionAlterUpdate::GetOrInitializeTable`) under
    `case_sensitive=true` with a cross-case table name present.

## Explicitly rejected alternatives

- **Global `SET case_sensitive` extension option:** rejected in favor of
  per-ATTACH only, despite precedent for global options in this extension
  (`unsafe_enable_version_guessing` et al.) — different attached catalogs
  may reasonably want different casing rules.
- **Always store case-insensitively, layer an exact check on top for
  sensitive mode:** rejected — would corrupt the cache for catalogs with
  genuinely case-distinct names before any exact check could run.
- **Extending `case_sensitive` to column-name resolution:** rejected for
  this iteration — blocked by DuckDB core's non-virtual, hardcoded
  case-insensitive `Binding`/`ColumnList` machinery and `BindContext`'s
  hardcoded `TableBinding` construction; would require a core patch, out of
  scope for an extension-only change.
- **Migrating `schema_property_updates` too:** included (not rejected) in
  this revision, unlike the original design — see §2. It's a direct
  structural analog of fields already in scope, added upstream after the
  original design was written.
