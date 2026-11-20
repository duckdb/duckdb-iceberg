# Design: a concrete `IcebergCatalog` with pluggable catalog backends

Status: proposal. Starting point: `upstream/main` (`bcef304a0`). It would replace the
inheritance-based design of the `add_jdbc_catalog_phase_1` … `_phase_5` branches.

## 1. Summary

`IcebergCatalog` becomes one concrete DuckDB `Catalog`, much like DuckLake's `DuckLakeCatalog`. Whatever
differs between catalog types lives in an `IcebergCatalogBackend` that the catalog owns, much like
`DuckLakeMetadataManager`. There are two backend families:

- **`IcebergRestBackend`** speaks the Iceberg REST protocol. It's lazy and per object: the server commits,
  vends credentials and plans scans, and ETags revalidate results. Vendor catalogs (AWS Glue's REST endpoint,
  S3 Tables) subclass it.
- **`IcebergMetastoreBackend`** handles catalogs that only store a pointer per table. DuckDB applies the
  commit itself, writes the metadata file and swaps the pointer. It owns an `IcebergMetastore`, which is the
  SQL/JDBC tables, the native Glue API or the Hadoop directory layout.

The backend interface uses neutral types: namespaces, entry ids, loaded tables and views, and status
categories. It has no HTTP codes, no `rest_api_objects::*Response`, and no `allow_forbidden`.
Capabilities that only REST has are exposed in one of three ways:

- capability flags, for the checks shared code makes;
- optional sub-interfaces, for behaviour with a payload (credential refresh, server-side scan planning);
- typed access (`GetBackend().Cast<T>()`), for everything else.

## 2. Problem

### 2.1 On `upstream/main` the catalog *is* the REST client

Every generic code path that DuckDB enters calls REST functions (`IRCAPI::*`, the `Iceberg*Request`
classes in `src/include/catalog/rest/api/catalog_api.hpp`) or inspects REST state. These are the leaks:

| # | Where | What is REST-specific |
|---|---|---|
| L1 | `IcebergCatalog` (`src/include/catalog/rest/iceberg_catalog.hpp`) | Members `auth_handler`, `base_uri`, `version`, `prefix`, `supported_urls`, `endpoint_type`, `warehouse`. Methods `ParsePrefix`, `Configure(rest_api_objects::CatalogConfig, IcebergEndpointType)`, `GetBaseUrl`, `AddDefaultSupportedEndpoints`/`AddGlueEndpoints`/`AddS3TablesEndpoints`. |
| L2 | `IcebergCatalog::CreateSchema`/`DropSchema` (`src/catalog/rest/iceberg_catalog.cpp`) | `IRCAPI::VerifySchemaExistence` (a HEAD request). `DropSchema` requires CASCADE iff `endpoint_type == AWS_GLUE`. |
| L3 | `IcebergSchemaSet::LoadEntriesInternal` (`iceberg_schema_set.cpp`) | `IcebergListSchemasRequest`, which returns `IRCAPISchema`; namespace levels are split on `namespace_separator` (`\x1f`). |
| L4 | `IcebergSchemaEntry` (`catalog_entry/schema/iceberg_schema_entry.cpp`) | `namespace_items` from `IRCAPI::ParseSchemaName`. `LookupEntry` falls back to `VerifySchemaExistence`. `LoadProperties` calls `IRCAPI::GetNamespace`, which returns `GetNamespaceResponse`, and throws `"GetNamespace endpoint returned response code …"`. |
| L5 | `IcebergTableSet` (`iceberg_table_set.cpp`) | `FillEntry` → `IRCAPI::GetTable`. `ApplyLoadResult` branches on `HTTPStatusCode::NotFound_404` and on the REST error type `NoSuchIcebergTableException`, a Glue quirk. `LoadEntriesInternal` → `IcebergListTablesRequest(…, allow_forbidden)`. `EntryMissingFromListing` takes `rest_api_objects::TableIdentifier`. View paths test `supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/views")`. |
| L6 | `IcebergTableSet::CreateNewEntry` | Calls `IRCAPI::CommitNewTable` at statement time (stage-create). Reads the `stage_create_tables`, `skip_create_table_metadata_updates` and `default_table_location_from_namespace` options. Non-staged creates register `TableCreatedInCatalog` for cleanup. |
| L7 | `IcebergTable` (`catalog_entry/table/iceberg_table.hpp/.cpp`) | `config` and `vector<rest_api_objects::StorageCredential> storage_credentials` from the LoadTable response. `GetVendedCredentials`. `LoadCredentials` branches on `catalog.base_uri` starting with `"glue"`/`"s3tables"` and casts `auth_handler` to `SIGV4Authorization`. `RefreshFromCatalog` → `IRCAPI::GetTable`. `initialization_source` is an `IcebergLoadTableResponse`. |
| L8 | `IcebergLoadTableResponse` (`iceberg_table_metadata_cache.hpp`) | Wraps a whole `rest_api_objects::LoadTableResult`, and parses `metadata-location` lazily (`use_metadata_location`). |
| L9 | `LoadTableResultCache`/`LoadTableCachePublication` (`iceberg_catalog.hpp`) | The cache value carries an HTTP `etag`. The publication keeps the cached result so it can send `If-None-Match`. |
| L10 | `IcebergTablePrefetch` (`iceberg_table_prefetch.cpp`) | Schedules `IcebergLoadTableRequest`s on the `IcebergRequestExecutor`. |
| L11 | Scan planning (`planning/scan_plan/iceberg_scan_plan_provider.cpp`, `catalog/rest/api/iceberg_scan_planning.cpp`) | `supported_urls.count(PLAN_ENDPOINT)`. Builds `rest_api_objects::PlanTableScanRequest` in generic planning code. |
| L12 | `IcebergTableSecretProvider::ReVendVendedCredentials` (`storage/iceberg_table_secret_provider.cpp`) | `IRCAPI::GetTableCredentials`. The error reports an HTTP status. |
| L13 | `IcebergTransaction` (`transaction/iceberg_transaction.cpp`) | The whole commit is REST: `StageSingleTableCommit` builds `rest_api_objects::CommitTableRequest`; `CommitMultiTableUpdate`/`CommitTableUpdate` return `CommitResult{HTTPStatusCode}`; `CommitIsRetryable` retries 409 only; `CommitStateUnknown` parses `"status code ("` out of the error text; `MultiTableCommitAvailable` checks `supported_urls`; `DoTableRename` builds `RenameTableRequest`; `DoSchemaCreates` builds `CreateNamespaceRequest`; `DoViewCreates` hand-writes a `CreateViewRequest` JSON body; `DropCreatedTables` reloads and deletes through REST; `CleanupFiles` is gated on `remove_files_on_delete` (S3 Tables). |
| L14 | Transaction restrictions | `GetOrCreateAlter`/`DeleteTable`/`RenameTable` and `Commit` throw `"Iceberg REST Catalog cannot commit this transaction atomically …"`. The single `transaction_update` variant (alter *or* drop *or* rename) and the rule that views commit alone exist because REST has no multi-object transaction. |
| L15 | Attach (`iceberg_attach.cpp`, `iceberg_attach.hpp`) | Everything: `/v1/config` negotiation, `ENDPOINT_TYPE`, OAuth2/SigV4, the AWS presets (`SetAWSCatalogOptions`), and `CreateNamespaceRequest` for `DEFAULT_SCHEMA_POLICY 'create'`. `IcebergAttachOptions` mixes generic options (`default_schema_policy`, `identifier_case`, `table_resolution`, `max_table_staleness_micros`) with protocol options (`catalog_uri`, `prefix`, `encode_entire_prefix`, `stage_create_tables`, `disable_multi_table_commit`, `purge_requested`, `access_mode` delegation …). |
| L16 | Table functions and maintenance | `iceberg_load_table_response`/`iceberg_view_metadata` send raw GETs through `auth_handler`/`GetBaseUrl`. `expire_snapshots_operator.cpp` uses `IRCAPI::ParseSchemaName`. `ReloadIcebergTableShared` (`maintenance_table_loader.cpp`) → `RefreshFromCatalog` → REST. |
| L17 | Layout | Generic code (schema and table entries, transactions, table sets) lives under `src/catalog/rest/`. |

These are **not** leaks. `rest_api_objects::TableMetadata`, `Schema`, `PartitionSpec`, `SnapshotReference`,
`TableUpdate` and `TableRequirement` are the generated JSON model of the Iceberg *spec*. `core/metadata/` uses
them to parse `metadata.json`, and phase 2's `IcebergClientCommit` applies `TableUpdate`s to them without any
server. Section 4.4 keeps them as the neutral representation.

The other maintenance operations (`iceberg_rewrite_data_files`, `iceberg_expire_snapshots`, the copy-on-write
DELETE in `iceberg_copy_on_write_delete.cpp`) and the metadata functions (`iceberg_metadata`, `iceberg_snapshots`, …)
work on `IcebergTable`/`IcebergTableMetadata` and commit through `ApplyTableUpdate`. They become backend-neutral
once L7, L13 and L16 are fixed.

### 2.2 The earlier base-class design still mirrors the REST spec

Phase 1 split upstream into an abstract `IcebergCatalog` and `IcebergRestCatalog`, and phase 3 added an
intermediate `IcebergMetastoreCatalog`. The *lifecycle* hooks (one `Commit`, `AbortTransaction`) and the shared
staging (`IcebergCommitStaging`) were the right moves. The read hooks, however, are the REST endpoints
(`origin/add_jdbc_catalog_phase_1:src/include/catalog/iceberg_catalog.hpp`):

```cpp
virtual APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
LoadSchemaProperties(ClientContext &, const IcebergSchemaEntry &) = 0;
virtual IcebergListTablesResult ListTables(ClientContext &, const vector<string> &, bool allow_forbidden) = 0;
virtual IcebergLoadTableResult LoadTable(ClientContext &, const vector<string> &, const string &) = 0;  // HTTP status
virtual IcebergLoadViewResult LoadView(...) = 0;              // rest_api_objects::LoadViewResult
virtual IcebergTable &CreateTable(ClientContext &, IcebergSchemaEntry &,
                                  const IcebergCreateTableRequest &) = 0;   // from rest/api/
```

`iceberg_catalog_requests.hpp` says so outright: *"They use the REST catalog's objects as the common
representation of catalog state; catalogs that are not backed by a REST server produce the same objects."*
The non-REST catalogs then fake REST:

- `IcebergSqlCatalog::LoadSchemaProperties` (phase 5, `src/catalog/sql/iceberg_sql_catalog.cpp`) builds a
  `GetNamespaceResponse` and sets `status_ = HTTPStatusCode::OK_200`.
- `IcebergMetastoreCatalog::LoadTable` returns `NotFound_404` with a hand-made `IcebergErrorResponse`
  (`type = "NoSuchTableException"`, `code = 404`). For a found table it wraps the parsed metadata in a
  `rest_api_objects::LoadTableResult`.
- `iceberg_load_table_response` branches on `GetIcebergCatalogType() == IcebergCatalogType::REST`.
- Every metastore subclass implements the same eight read hooks: `SchemaExists`, `ListSchemas`,
  `LoadSchemaProperties`, `ListTables`, `SupportsViews`, `ListViews`, plus `LoadTable`/`LoadView` in the
  intermediate class. `allow_forbidden` means nothing to them.
- The hierarchy grows a level for every variation: `IcebergCatalog` → `IcebergMetastoreCatalog` →
  `IcebergSqlCatalog`, with a planned `IcebergGlueRestCatalog` under `IcebergRestCatalog`. DuckDB-facing
  behaviour (planning, entry lookup, transactions) and protocol behaviour share one object, so a subclass can
  change either one by accident.
- Base members that are still REST-shaped: `LoadTableResultCache table_request_cache`, whose payload is the
  REST response, and the `TableCreatedInCatalog` bookkeeping in `IcebergTransaction`.

## 3. Reference designs

| | DuckLake | Iceberg Java | This proposal |
|---|---|---|---|
| DuckDB-facing object | `DuckLakeCatalog`, concrete | `Catalog`, `SupportsNamespaces`, `ViewCatalog` interfaces | `IcebergCatalog`, concrete |
| Varying part | `DuckLakeMetadataManager` (DuckDB/SQLite/Postgres subclasses, REST through `IsRemote()`/`FlushChangesServerSide`), **one per transaction** | `BaseMetastoreCatalog` subclasses (`JdbcCatalog`, `GlueCatalog`, `HadoopCatalog`) plus `RESTSessionCatalog` | `IcebergCatalogBackend`, **one per attached catalog** |
| Table refresh/commit | metadata manager writes rows | `TableOperations.refresh()/commit(base, metadata)`; `BaseMetastoreTableOperations` writes `metadata.json`, subclasses `doCommit` (JDBC `UPDATE … WHERE metadata_location = ?`, Glue `VersionId`, Hadoop rename) | `IcebergMetastore` + per-attempt `IcebergMetastoreCommit`; REST posts updates |
| Loading | bulk: `GetCatalogForSnapshot` loads all schemas/tables of a snapshot | lazy per table (`loadTable` → `newTableOps().current()`) | lazy per table; listings may be bulk (§4.3) |
| REST mode | `DuckLakeCatalog::IsREST()`, `DuckLakeRESTClient` owned by the catalog, flags on the manager | separate `RESTSessionCatalog`, `RESTTableOperations` | `IcebergRestBackend` subclass, sub-interfaces, typed access |

DuckLake's manager is per transaction because it runs SQL on the transaction's metadata connection. Ours has to
be per catalog. The REST backend owns state that outlives transactions: the OAuth token, the endpoint set and
`/config`. Load requests also run on worker threads (`IcebergRequestExecutor`) outside any single transaction's
connection. The per-transaction part of DuckLake's manager corresponds to our per-attempt
`IcebergMetastoreCommit`.

Java's split between the catalog and `TableOperations` corresponds to our split between the backend's catalog
operations and the metastore's `LookupMetadataLocation` + `BeginCommit`. We don't need a `TableOperations` object
per table, because `IcebergTable` already holds the current metadata, and the commit runs per transaction rather
than per table.

## 4. Proposed architecture

```
IcebergCatalog (concrete, final)              DuckDB Catalog: planning, entry sets, transactions, caches
 └── unique_ptr<IcebergCatalogBackend>
      ├── IcebergRestBackend                  Iceberg REST protocol
      │    ├── IcebergGlueRestBackend         ENDPOINT_TYPE 'glue' / glue.<region>.amazonaws.com (later PR)
      │    └── IcebergS3TablesRestBackend     ENDPOINT_TYPE 's3_tables' (later PR)
      └── IcebergMetastoreBackend (final)     client-side commits, pointer swaps
           └── unique_ptr<IcebergMetastore>
                ├── IcebergSqlMetastore       CATALOG_TYPE 'sql' (DuckDB file / SQLite / Postgres)
                ├── IcebergGlueMetastore      CATALOG_TYPE 'glue' (native AWS Glue API)
                └── IcebergFilesystemMetastore CATALOG_TYPE 'filesystem' (Hadoop layout)
```

### 4.1 Who owns what

| Owner | State | Lifetime / threading |
|---|---|---|
| `IcebergCatalog` | `IcebergSchemaSet`, the per-schema `IcebergTableSet` placeholders, the shared loaded-table cache (`MAX_TABLE_STALENESS`, publication generations), the generic attach options, the backend | Attach to detach. Shared by all transactions; existing locks unchanged. |
| `IcebergTransaction` | Everything it holds today: `current_table_data`, `transaction_update`, `created_schemas`, views, `listed_schemas`, `looked_up_entries`, the prefetch | One transaction. |
| `IcebergCatalogBackend` | Protocol state only. REST: auth handler, base URL, prefix, endpoint set, `/config` properties. SQL: attached database name, schema version (V0/V1). Glue: client and region. Filesystem: warehouse root. | Attach to detach (`OnDetach` hook). **Must be thread-safe.** Called concurrently by transactions, by `IcebergRequestExecutor` workers (prefetch, eager resolution, view loads) and by the secret-refresh callback. |
| `IcebergMetastoreCommit` | One publish attempt: a SQL transaction, or Glue's version ids | One attempt, one thread. |

There are two rules, and the existing request classes already follow them (*"execution only fetches and parses;
the caller owns cache lookup and result publication"*):

1. A backend never touches catalog entries, table sets or the transaction. It takes ids and returns values.
2. Publication into caches and entry sets happens in `IcebergCatalog`/`IcebergTableSet`, never in a backend.

### 4.2 Neutral types

```cpp
// src/include/catalog/iceberg_catalog_types.hpp
enum class IcebergEntryKind : uint8_t { TABLE, VIEW };

struct IcebergNamespace {
	vector<string> levels;                       // replaces IRCAPISchema / namespace_items
	string ToString() const;                     // "a.b" for messages and table keys
};

struct IcebergEntryId {
	IcebergNamespace ns;
	string name;
};

struct IcebergListedEntry {
	string name;
	IcebergEntryKind kind;
	string metadata_location;                    // set when the listing already knows it (metastores)
};

//! Outcome categories. HTTP codes, REST error types and SQL errors only appear in 'message'.
enum class IcebergCatalogStatus : uint8_t {
	OK,
	NOT_FOUND,          // namespace or entry doesn't exist (404, no row, no directory)
	NOT_ICEBERG,        // exists but isn't an Iceberg table (Glue: NoSuchIcebergTableException, Hive tables)
	FORBIDDEN,          // not visible to this principal (401/403); a refused listing is not an empty one
	ALREADY_EXISTS,     // create of an existing entry or namespace (409 on create, unique violation)
	CONFLICT,           // a requirement or compare-and-swap failed; retryable if nothing was published
	UNKNOWN_OUTCOME,    // the change may have landed (5xx, timeout, lost connection after publish)
	UNSUPPORTED,        // the backend can't do this
	FAILED              // any other definite failure
};

struct IcebergCatalogError {
	IcebergCatalogStatus status;
	string message;          // keeps today's texts, e.g. "GetTableInformation endpoint returned response code …"
	string error_type;       // e.g. REST "NoSuchTableException"; logs and tests only
	[[noreturn]] void Throw() const;   // maps to CatalogException / PermissionException / TransactionException / HTTPException
};

template <class T>
struct IcebergCatalogResult {
	T value;                             // may be partial with FORBIDDEN (schema listing keeps earlier pages)
	optional<IcebergCatalogError> error;
	bool Ok() const { return !error; }
};

struct IcebergStorageCredential {
	string prefix;
	case_insensitive_map_t<string> config;
};
struct IcebergTableAccess {                 // REST: 'config' and 'storage-credentials'; metastores: empty
	case_insensitive_map_t<string> config;
	vector<IcebergStorageCredential> storage_credentials;
};

//! One immutable, shareable generation of a table (replaces IcebergLoadTableResponse).
//! A revalidation that finds it unchanged returns the *same* shared_ptr; a new version is a new object.
class IcebergLoadedTable {
public:
	string metadata_location;
	//! Opaque to everyone but the backend that produced it: an ETag (REST), the metadata location (SQL, Glue),
	//! the version number (filesystem)
	string version_token;
	IcebergTableAccess access;
	//! Parsed lazily, at most once; from 'metadata_location' or from the embedded metadata
	shared_ptr<const IcebergTableMetadata> GetMetadata(ClientContext &, const IcebergTable &) const;

private:
	optional<rest_api_objects::TableMetadata> embedded;   // the REST response's metadata, if any
	mutable mutex lock;
	mutable shared_ptr<const IcebergTableMetadata> metadata;
};

struct IcebergLoadedView {
	string metadata_location;
	rest_api_objects::ViewMetadata metadata;     // spec model
	case_insensitive_map_t<string> config;
};

//! A new table, everything DuckDB decides (moved out of rest/api/iceberg_create_table_request.hpp)
struct IcebergNewTable {
	string name;
	shared_ptr<IcebergTableSchema> schema;
	IcebergPartitionSpec partition_spec;
	IcebergSortOrder sort_order;
	idx_t format_version;
	case_insensitive_map_t<string> properties;
	string location;                             // empty: the backend chooses
};

struct IcebergCreatedTable {
	shared_ptr<const IcebergLoadedTable> initial;     // metadata to stage the table's first commit against
	bool exists_in_catalog = false;   // published already (REST non-staged); Abort must drop it
	bool needs_assert_create = true;  // first commit carries assert-create
	bool add_create_updates = true;   // false: skip_create_table_metadata_updates
};

//! A new view: name, SQL, dialect, output columns, default namespace (built today in DoViewCreates)
struct IcebergNewView { /* ... */ };
```

`version_token` makes ETag revalidation a neutral feature. REST sends `If-None-Match`. A metastore re-reads
the pointer, which is one indexed SQL row, one Glue `GetTable` or one `version-hint.text`, and keeps the parsed
metadata when the pointer hasn't changed. `MAX_TABLE_STALENESS` and the publication-generation logic of
`LoadTableResultCache` therefore stay generic and unchanged; only the value type changes. That's simpler than
phase 1, where `IcebergRestCatalog` kept a separate map of `load_table_validators` and used
`LoadTableResultCache::Peek`.

### 4.3 Lazy or bulk loading

**Decision: tables and views are loaded lazily for every backend. Listings may be bulk where the backend makes
them cheap.**

- An Iceberg table's columns live in its `metadata.json`. A bulk load in the style of DuckLake's
  `GetCatalogForSnapshot` would read one file per table, which is the cost `table_resolution = LAZY` avoids for
  REST. DuckLake can bulk-load only because its whole catalog is rows in one database.
- What *is* cheap for a metastore is names plus pointers. The SQL backend answers `ListEntries` with one query
  (`SELECT table_name, metadata_location, iceberg_type FROM iceberg_tables WHERE catalog_name = ? AND
  table_namespace = ?`). Each `IcebergListedEntry` carries `metadata_location`, so the table set skips the
  pointer lookup when it later loads the table.
- An optional `ListAllEntries(ctx)` (default: `UNSUPPORTED`) lets `ScanSchemas` plus
  `information_schema.tables` fetch the whole catalog in one SQL query instead of one per namespace. Whether
  to make this consistent per transaction (one SQL snapshot) is question Q4.
- REST keeps its paging, its nested-namespace traversal, its forbidden-tolerant listings and the prefetch.

`IcebergTablePrefetch` and eager resolution run `backend.LoadTable` on the `IcebergRequestExecutor` for every
backend. For SQL, one connection per call (as in phase 3's `Connect()`) keeps this thread-safe. A batched
`LoadTables(ids)` with a default loop can come later if profiling asks for it.

### 4.4 The backend interface

```cpp
// src/include/catalog/iceberg_catalog_backend.hpp
struct IcebergBackendCapabilities {
	bool nested_namespaces = false;
	bool namespace_properties = true;
	bool views = false;                        // list, load, create, drop
	bool rename_table = true;
	IcebergDropNamespaceMode drop_namespace = IcebergDropNamespaceMode::RESTRICT;   // or CASCADE_ONLY (Glue REST)
	//! Several tables publish atomically (REST transactions/commit, SQL)
	bool atomic_multi_table_commit = false;
	//! A table that is created can be part of that atomic multi-table commit (REST: no, with stage-create)
	bool atomic_commit_with_creates = false;
	//! Table changes, drops, renames, namespaces and views publish together (SQL; not REST)
	bool atomic_catalog_commit = false;
	//! DuckDB may delete files it wrote when a commit definitely failed (S3 Tables: no)
	bool delete_written_files = true;
};

//! Backend type ids are strings, so an extension can add one, and Cast<T>() works across shared libraries
//! without RTTI
using IcebergBackendTypeId = const char *;

class IcebergCatalogBackend {
public:
	static constexpr IcebergBackendTypeId TYPE = "iceberg.backend";
	virtual ~IcebergCatalogBackend();

	//! "REST Catalog", "SQL catalog", ... used in user-facing messages ("Iceberg REST Catalog cannot ...")
	virtual string Name() const = 0;
	const IcebergBackendCapabilities &Capabilities() const { return capabilities; }

	//===--- Namespaces ---===//
	virtual IcebergCatalogResult<bool> NamespaceExists(ClientContext &, const IcebergNamespace &) = 0;
	virtual IcebergCatalogResult<vector<IcebergNamespace>> ListNamespaces(ClientContext &,
	                                                                      const IcebergNamespace &parent) = 0;
	virtual IcebergCatalogResult<case_insensitive_map_t<string>> LoadNamespaceProperties(ClientContext &,
	                                                                                    const IcebergNamespace &) = 0;
	//! The namespace parsing of schema names ('a.b', the \x1f separator) moves here from IRCAPI::ParseSchemaName
	virtual IcebergNamespace ParseNamespace(const string &schema_name) const;

	//===--- Tables and views ---===//
	virtual IcebergCatalogResult<vector<IcebergListedEntry>> ListEntries(ClientContext &, const IcebergNamespace &,
	                                                                     IcebergEntryKind kind) = 0;
	virtual IcebergCatalogResult<vector<pair<IcebergNamespace, IcebergListedEntry>>> ListAllEntries(ClientContext &);
	//! 'cached' is a previous generation to revalidate. Returning it unchanged means "not modified".
	//! 'listed' carries what the listing already knows (e.g. the metadata location)
	virtual IcebergCatalogResult<shared_ptr<const IcebergLoadedTable>>
	LoadTable(ClientContext &, const IcebergEntryId &, shared_ptr<const IcebergLoadedTable> cached,
	          optional_ptr<const IcebergListedEntry> listed) = 0;
	virtual IcebergCatalogResult<shared_ptr<const IcebergLoadedView>> LoadView(ClientContext &,
	                                                                          const IcebergEntryId &);

	//===--- Statement time ---===//
	virtual IcebergCreatedTable CreateTable(ClientContext &, const IcebergEntryId &, const IcebergNewTable &) = 0;

	//===--- Commit (section 5) ---===//
	virtual void Commit(ClientContext &, IcebergCommitPlan &plan) = 0;
	//! Undo what was published ahead of the commit (REST non-staged creates). Must not throw
	virtual void Abort(ClientContext &, IcebergCommitPlan &plan) {}

	//===--- Optional REST-shaped capabilities (section 4.5) ---===//
	virtual optional_ptr<IcebergCredentialVending> GetCredentialVending() { return nullptr; }
	virtual optional_ptr<IcebergServerScanPlanning> GetServerScanPlanning() { return nullptr; }

	//===--- Lifecycle ---===//
	virtual void OnDetach(ClientContext &) {}

	//===--- Typed access (section 7.3) ---===//
	virtual bool IsInstanceOf(IcebergBackendTypeId id) const { return StringUtil::Equals(id, TYPE); }
	template <class T> T &Cast();              // throws a friendly error if !IsInstanceOf(T::TYPE)
	template <class T> optional_ptr<T> TryCast();

protected:
	IcebergBackendCapabilities capabilities;   // set by the factory or after /config
};
```

`IcebergCatalog` gets `IcebergCatalogBackend &GetBackend()` and loses every member of L1.

### 4.5 How REST-only behaviour is expressed

| Behaviour | Mechanism | Why |
|---|---|---|
| Multi-table commit, views, rename, nested namespaces, DROP SCHEMA cascade rule, file deletion | **Capability flags** | Shared code needs a yes/no at statement time to fail early with today's messages (L2, L14). Flags are cheap and easy to test, and the metastore backends set them too. |
| Forbidden listings | **Status** `FORBIDDEN` | Not a capability: any backend may refuse. The `allow_forbidden` policy (stay quiet unless this transaction already looked up an entry in the namespace) moves into `IcebergTableSet::LoadEntriesInternal`, which already computes it. |
| ETag revalidation | **Neutral `version_token`** (§4.2) | Every backend can revalidate in its own way. |
| Stage-create, non-staged create, skip-create-updates | **`IcebergCreatedTable` fields** | The backend reports what it did; the generic code adds the transaction state and the create updates (phase 1's `AddCreateTableUpdates`). |
| Vended credentials and their refresh | **Optional sub-interface** `IcebergCredentialVending { LoadCredentials(ctx, id) }`, plus `IcebergTableAccess` on the loaded table | It carries a payload. Native Glue may vend Lake Formation credentials one day, so this doesn't belong in a REST-only object. |
| Server-side scan planning | **Optional sub-interface** `IcebergServerScanPlanning { TryPlan(ctx, table, IcebergScanPlanContext&) }` | The building of `PlanTableScanRequest` moves out of `iceberg_scan_plan_provider.cpp` into the REST backend; the provider only asks `GetServerScanPlanning()`. |
| `/config`, OAuth, prefix, raw REST debugging (`iceberg_load_table_response`, `iceberg_view_metadata`) | **Typed access**: `GetBackend().TryCast<IcebergRestBackend>()` | Purely REST; shared code never needs it. For metastores the two functions return `metadata_location` and `metadata` with NULL `request_url`, as phase 3 did. |

Shared code must never ask "is this REST?". It asks for a capability, a sub-interface or a status. Only REST-
family functions and vendor features use `Cast<T>()`.

## 5. Commit protocol

### 5.1 Shared staging

`IcebergTransaction::Commit` keeps collecting the transaction's changes. It hands them to the backend as an
`IcebergCommitPlan`, which is a view over the transaction:

```cpp
class IcebergCommitPlan {
public:
	// What to publish (read-only)
	const vector<IcebergNamespace> &CreatedNamespaces() const;
	const vector<IcebergNamespacePropertyUpdate> &NamespacePropertyUpdates() const;
	const IcebergTransactionUpdate &TableChange() const;      // alter set | drop | rename (unchanged variant)
	const vector<IcebergNewView> &CreatedViews() const;
	const vector<IcebergEntryId> &DroppedViews() const;
	const vector<IcebergNamespace> &DroppedNamespaces() const;

	// Staging, callable once per attempt: writes manifests and manifest lists, returns requirements + updates
	IcebergStagedTableCommit Stage(IcebergTable &table);
	void RefreshForRetry(const unordered_set<string> &table_keys);   // today's RefreshRetryTables

	// Outcome bookkeeping that the generic code acts on
	void AddWrittenFile(string path);         // removed if the commit definitely failed
	void MarkOutcomeUnknown();                // keep all files, skip dropping created tables
	void OnPublished(std::function<void()>);  // in-memory updates (schema set, table sets, cache eviction)
};

struct IcebergStagedTableCommit {
	IcebergEntryId id;
	string table_key;
	rest_api_objects::CommitTableRequest change;   // the spec's requirements + updates
	vector<string> written_files;
	bool retryable;
	IcebergRetryConfig retry_config;
};
```

`Stage` is today's `StageSingleTableCommit` (phase 1's `IcebergCommitStaging`). It's independent of how the
result is published. A shared `IcebergCommitRetryLoop` holds `IcebergRetryBackoff`, the
`commit.retry.num-retries`/`total-timeout-ms` budget and the "retry only CONFLICT, only if retryable, only if
nothing was published" rule. Both families use it, which merges upstream's `DoSingleTableCommitUpdates` and
`DoMultiTableCommitUpdates` with phase 3's `CommitWithRetries`.

### 5.2 REST backend

The REST backend's `Commit` is upstream's sequence, moved into `IcebergRestCommit` and reusing phase 1's
`iceberg_rest_commit.cpp`:

1. create namespaces, then update properties;
2. tables: `POST transactions/commit` when `atomic_multi_table_commit` holds and there's no staged create, else
   a single `POST …/tables/{t}`. Drops go to `DELETE …/tables/{t}`, renames to `POST tables/rename`;
3. view deletes, then view creates;
4. namespace drops.

HTTP is mapped to statuses at one point: 409 → `CONFLICT`; other 4xx → `FAILED`; 5xx or no status →
`UNKNOWN_OUTCOME`. That replaces the string search in `CommitStateUnknown`; the REST client records the status
on the error. `Abort` is `DropCreatedTables`: a table published by a non-staged create is dropped only if the
catalog still reports the version the create returned (`IsCreatedVersion`). It's skipped when the outcome is
unknown.

### 5.3 Metastore backend

`IcebergMetastoreBackend::Commit` is phase 3/5's `IcebergMetastoreTransactionCommit`, nearly unchanged. Per attempt:

1. `commit = metastore.BeginCommit(ctx)`;
2. namespace creates and property updates;
3. for each table: `Stage`. For an existing table, `base_location = commit.LoadMetadataLocation(id)` and the
   base metadata is parsed; then `IcebergClientCommit::Apply(base, change)` checks the requirements and applies
   the updates, `metastore.WriteMetadata(…)` writes `<N>-<uuid>.metadata.json` (filesystem: `v<N>`), and
   `commit.CreateEntry` or `commit.SwapMetadataLocation(id, base_location, new_location)` publishes it;
4. drop/rename, views (view metadata files written client-side), namespace drops;
5. `commit.Finish()`.

**Atomic and non-atomic metastores.** `IcebergMetastoreCommit::IsAtomic()`:

- **SQL** buffers everything in one database transaction, so `Finish()` is the commit point. A conflict is
  either `UPDATE … WHERE metadata_location = :base` touching zero rows or a SQLite/Postgres serialization error.
  It rolls the whole attempt back, and the attempt is retried.
- **Glue native** publishes each `UpdateTable` with `VersionId` as it goes. **Filesystem** publishes when it
  creates the next version file (temp file + `link()`). Both mark the attempt `HasPublished()` after the first
  entry change.

**Safety rules from the earlier phases, kept as they are:**

- Once a non-atomic attempt has published, a failure is not retried and the written files are kept
  (`MarkOutcomeUnknown`).
- A failure with no definite outcome is reconciled. The metastore reads the entry back
  (`ReconcileEntry`: `APPLIED`/`NOT_APPLIED`/`UNKNOWN`). After a lost `COMMIT` acknowledgement, the SQL
  backend reads its entries back. `UNKNOWN` keeps the files and fails with "commit state unknown".
- Non-atomic backends allow **one table per transaction**: the capability `atomic_multi_table_commit = false`
  is checked at statement time, which replaces `VerifySingleTableUpdate`.
- Files written by a definitely failed attempt are removed only if `delete_written_files` holds.
- The test-only fault injection `iceberg_test_metastore_commit_fault` stays.

### 5.4 Statement-time checks

Today's checks in `IcebergTransaction` become capability checks with the same messages. The text comes from
`Name()`, and REST's `Name()` is `"REST Catalog"`, so the 14 assertions in 5 test files that match
`"Iceberg REST Catalog cannot commit this transaction atomically …"` keep passing:

| Check | Today | Proposed |
|---|---|---|
| Table updates mixed with drop/rename | always refused (`GetOrCreateAlter`) | refused unless `atomic_catalog_commit` |
| More than one table | `VerifyAlterUpdateAtomicity` → `CanUseMultiTableCommit` (`supported_urls`, `has_assert_create`) | `atomic_multi_table_commit` and (no create or `atomic_commit_with_creates`) |
| View changes mixed with other changes | refused in `Commit()` | refused unless `atomic_catalog_commit` |

Lifting the first and third restrictions for SQL is possible but out of scope at first (Q5). The
`IcebergTransactionUpdate` variant stays.

## 6. The metastore backend

### 6.1 The minimal metastore surface

```cpp
class IcebergMetastore {            // one per attached catalog, thread-safe
public:
	virtual ~IcebergMetastore();
	virtual string Name() const = 0;
	virtual IcebergBackendCapabilities Capabilities() const = 0;

	virtual bool NamespaceExists(ClientContext &, const IcebergNamespace &) = 0;
	virtual vector<IcebergNamespace> ListNamespaces(ClientContext &, const IcebergNamespace &parent) = 0;
	virtual case_insensitive_map_t<string> LoadNamespaceProperties(ClientContext &, const IcebergNamespace &);
	virtual vector<IcebergListedEntry> ListEntries(ClientContext &, const IcebergNamespace &, IcebergEntryKind) = 0;
	//! "" if there is no such entry
	virtual string LookupMetadataLocation(ClientContext &, const IcebergEntryId &, IcebergEntryKind) = 0;
	virtual string DefaultTableLocation(ClientContext &, const IcebergEntryId &) = 0;
	virtual unique_ptr<IcebergMetastoreCommit> BeginCommit(ClientContext &) = 0;

	//! Default: '<location>/metadata/<N>-<uuid>.metadata.json'. Filesystem: create 'v<N>.metadata.json' exclusively
	virtual string WriteMetadata(ClientContext &, IcebergMetastoreCommit &, const IcebergEntryId &,
	                             IcebergTableMetadata &metadata, optional_ptr<const IcebergTableMetadata> base,
	                             const string &base_location);
	//! The directory to remove after DROP ... PURGE, "" to keep the files
	virtual string PurgeLocation(const IcebergTable &) const;
	virtual void OnDetach(ClientContext &) {}
};
```

`IcebergMetastoreCommit` is phase 3/5's class, kept verbatim: `IsAtomic`, `NamespaceExists`, `CreateNamespace`,
`UpdateNamespaceProperties`, `DropNamespace`, `LoadMetadataLocation`, `CreateEntry`, `SwapMetadataLocation`,
`DropEntry`, `RenameEntry`, `Finish`, `Abort`, `HasPublished`.

`IcebergMetastoreBackend` (final) turns this into an `IcebergCatalogBackend`:

- `LoadTable`: look up the location, or take it from `listed`. If it equals `cached->version_token`, return
  `cached`; otherwise parse the file. Not found → `NOT_FOUND`.
- `CreateTable`: build the initial metadata locally at the requested location or at `DefaultTableLocation`.
  The result has `needs_assert_create = true` and `exists_in_catalog = false`.
- `Commit`: section 5.3. No credential vending, no scan planning.

### 6.2 Per metastore

| | SQL (JDBC) | Glue native | Filesystem (Hadoop) |
|---|---|---|---|
| Namespaces | `iceberg_namespace_properties` ∪ distinct `table_namespace`; joined with `.` | Glue databases, no nesting | directories under the warehouse |
| Properties | rows of `iceberg_namespace_properties` (excluding `exists`); `location` defaults to `<warehouse>/<ns>` | `Database.Parameters`, `LocationUri` | none (`namespace_properties = false`) |
| Entries | `iceberg_tables` (V1: `iceberg_type` TABLE/VIEW; V0: tables only) | `GetTables`, `table_type = ICEBERG` | directories with `metadata/` |
| Lookup | `metadata_location` column | `Parameters.metadata_location` + `VersionId` | `version-hint.text` or the highest `v<N>` |
| Swap | `UPDATE … WHERE metadata_location = :base` | `UpdateTable(VersionId)` | exclusive create of `v<N+1>` (temp + `link()`) |
| Atomic | yes, one SQL transaction | no; one table per transaction | no; single writer, one table per transaction |
| Views | yes (V1) | no | no |
| Rename | `UPDATE table_name` | create + delete | no |

Compared with Java, the SQL metastore is `JdbcCatalog`'s SQL (`JdbcUtil`) together with
`JdbcTableOperations.doCommit`. The shared metadata writing and refreshing of `BaseMetastoreTableOperations`
(`writeNewMetadataIfRequired`, `refreshFromMetadataLocation`) becomes `IcebergMetastoreBackend` plus
`IcebergClientCommit`, and `checkCommitStatus` becomes `ReconcileEntry`. Compared with DuckLake, our metastore
is far smaller than `DuckLakeMetadataManager`: DuckLake keeps snapshots, files and stats in the database,
whereas an Iceberg metastore keeps one pointer per table.

## 7. REST backend and vendor subclasses

> **Future work.** This section is the target design. Phase 1 defers the vendor subclasses and the backend
> registry (§7.2) to a later PR: it keeps the `ENDPOINT_TYPE` handling (`glue`, `s3_tables` and the Glue URI match)
> inside `IcebergRestBackend`, as upstream's attach did, and the attach constructs `IcebergRestBackend` directly.
> The `CATALOG_TYPE` option also comes later, with the first non-REST backend.

`IcebergRestBackend` is not final. Vendor catalogs subclass it, override behaviour through protected virtual
extension points, and add their own functionality on top. Generic code only sees `IcebergCatalogBackend`.

### 7.1 Extension points

Each extension point exists because of a concrete branch in today's code:

| Extension point (virtual on `IcebergRestBackend`) | Default | Real cases today |
|---|---|---|
| `ApplyOptionPresets(IcebergRestOptions &, const case_insensitive_set_t &set_by_user)` | none | `SetAWSCatalogOptions` (`iceberg_attach.cpp`): `stage_create_tables = false`, `remove_files_on_delete = false`, `purge_requested = true`, `identifier_case = LOWERCASE`, unless set by the user. |
| `CreateAuthorization(…)` | OAuth2 / SigV4 / none from options | `GlueAttach`/`S3TablesAttach` → `S3OrGlueAttachInternal`: SigV4 with `sigv4_service` `glue`/`s3tables`; the region comes from the URI (`GlueRegionFromURI`) or the warehouse ARN (`ParsedARN::Region`). |
| `ResolveBootstrap(…)`: base URI and `/config` warehouse | `uri` option, warehouse as given | Glue derives `glue.<region>.amazonaws.com/iceberg` and sends an empty warehouse for `':'` (`config_warehouse.clear()`); S3 Tables derives `s3tables.<region>.amazonaws.com/iceberg`. |
| `BuildPrefix(…)` / `GetBaseUrl()` | `ParsePrefix` split on `/` | `encode_entire_prefix`, ARN prefixes that must stay one encoded segment. |
| `DefaultEndpoints()`: capability detection when `/config` omits `endpoints` | `AddDefaultSupportedEndpoints` | `AddGlueEndpoints` (no rename, no `transactions/commit`, no views) and `AddS3TablesEndpoints` (rename, HEAD, no multi-table commit) → capabilities. |
| `AdjustCapabilities(IcebergBackendCapabilities &)` | from endpoints and options | Glue: `drop_namespace = CASCADE_ONLY` (`IcebergCatalog::DropSchema`); S3 Tables: `delete_written_files = false`; `disable_multi_table_commit`. |
| `MapError(const HTTPResponse &, const IcebergErrorResponse &) → IcebergCatalogStatus` | 404 → NOT_FOUND, 401/403 → FORBIDDEN, 409 → CONFLICT/ALREADY_EXISTS, 5xx → UNKNOWN_OUTCOME | Glue: 404 with `NoSuchIcebergTableException` → `NOT_ICEBERG` (`IcebergTableSet::ApplyLoadResult`). |
| `PrepareCreateTable(IcebergNewTable &, …)` / `CreateTableMode()` | stage-create | Glue: `default_table_location_from_namespace` (location from the namespace's `location` property); `stage_create_tables`; `skip_create_table_metadata_updates`. |
| `DropTableRequest(…)` | `purgeRequested=false` | `purge_requested` preset for AWS (`catalog_api.cpp`). |
| `BuildStorageSecret(IcebergTableAccess &, CreateSecretInput &)` | vended config as is | `IcebergTable::LoadCredentials`: for `glue` set `endpoint = s3.<region>.amazonaws.com`; for `s3tables` replace the options with the SigV4 storage secret's keys and the region from the ARN. |

A subclass may also add methods, settings and table functions of its own (§7.3). It must not override the
commit sequence wholesale. If a vendor needs that, it's a sign that a new extension point is missing.

### 7.2 Backend registry

```cpp
struct IcebergBackendOption {
	string name;                // e.g. "uri", "oauth2_scope", "sigv4_region"
	LogicalType type;
	string description;
};

struct IcebergBackendFactory {
	string catalog_type;        // "rest", "sql", "glue", "filesystem"
	string endpoint_type;       // "" for the plain REST backend, "glue", "s3_tables"
	//! Recognize an attach without ENDPOINT_TYPE, e.g. a glue.<region>.amazonaws.com URI
	std::function<bool(const IcebergAttachInput &)> matches;
	//! The ATTACH options this backend accepts beyond the generic ones
	vector<IcebergBackendOption> options;
	std::function<unique_ptr<IcebergCatalogBackend>(ClientContext &, IcebergAttachInput &)> create;
	uint32_t api_version = ICEBERG_BACKEND_API_VERSION;
};

class IcebergBackendRegistry {
public:
	static IcebergBackendRegistry &Get(DatabaseInstance &);
	void Register(IcebergBackendFactory factory);        // throws on duplicate (catalog_type, endpoint_type)
	const IcebergBackendFactory &Resolve(const IcebergAttachInput &) const;
};
```

`IcebergAttach::Attach` does this:

1. parse the generic options: `CATALOG_TYPE`, `ENDPOINT_TYPE`, `DEFAULT_SCHEMA`, `DEFAULT_SCHEMA_POLICY`,
   `IDENTIFIER_CASE`, `MAX_TABLE_STALENESS`, `TABLE_RESOLUTION`;
2. resolve the factory, by explicit types or by `matches`;
3. reject any option that is neither generic nor in `factory.options`, which keeps today's strict rejection;
4. call `create`;
5. apply `DEFAULT_SCHEMA_POLICY` through the backend, which makes the CREATE path neutral.

The iceberg extension registers `rest`, `rest`/`glue`, `rest`/`s3_tables`, `sql`, `glue` and `filesystem` at load.

**Registration from another extension.** A separate extension (a vendor REST catalog, or a future duckdb-aws-glue
replacement) reaches the registry through DuckDB's `ExtensionCallback` list, the same way
`IcebergFileCredentialsCallback` already works. It needs no link-time symbol from the iceberg extension. What that
implies:

- **Source API.** The headers of `IcebergCatalogBackend`, `IcebergRestBackend`, the neutral types and the
  registry become a public, documented surface: the "source SDK" option from the earlier brainstorm.
- **No binary ABI across releases.** DuckDB extensions are C++ built per DuckDB version, so an extension that
  subclasses `IcebergRestBackend` must be compiled against the same iceberg sources as the iceberg extension
  it's loaded with. `api_version` is checked in `Register` and refused on mismatch, rather than crashing on a
  vtable layout change.
- **Discipline.** Virtual methods get out-of-line definitions, new virtuals are added at the end, data members
  stay private behind accessors, and there's no RTTI dependency (string type ids, below).

### 7.3 Typed access for vendor features

Vendor features, such as a table function, a setting or a pragma specific to one catalog, reach their backend
with a typed cast from the catalog:

```cpp
auto &catalog = IcebergCatalog::GetFromName(context, catalog_name);       // checks GetCatalogType() == "iceberg"
auto &glue = catalog.GetBackend().Cast<IcebergGlueRestBackend>();
// Error: "Catalog 'lake' is a SQL catalog, not an AWS Glue REST catalog"
```

`Cast<T>()` checks `IsInstanceOf(T::TYPE)`, and each class chains to its base. A Glue REST backend is
therefore also an `IcebergRestBackend`, which `iceberg_load_table_response` relies on. The generic catalog,
table sets and transaction never name a concrete backend type. A vendor's settings are registered by the code
that registers its factory, and only its own overrides read them.

### 7.4 Sketch: the Glue REST backend

```cpp
// src/include/catalog/rest/vendor/iceberg_glue_rest_backend.hpp
class IcebergGlueRestBackend : public IcebergRestBackend {
public:
	static constexpr IcebergBackendTypeId TYPE = "iceberg.rest.glue";
	static IcebergBackendFactory Factory();      // catalog_type "rest", endpoint_type "glue", matches glue.*.amazonaws.com

	IcebergGlueRestBackend(ClientContext &, IcebergAttachInput &);

	bool IsInstanceOf(IcebergBackendTypeId id) const override {
		return StringUtil::Equals(id, TYPE) || IcebergRestBackend::IsInstanceOf(id);
	}
	string Name() const override { return "REST Catalog"; }  // keep today's messages

	//! Glue-only functionality, for Glue-specific table functions
	const string &GetCatalogId() const { return catalog_id; }   // the ':' / account id warehouse
	const string &GetRegion() const { return region; }

protected:
	void ApplyOptionPresets(IcebergRestOptions &, const case_insensitive_set_t &set_by_user) override;
	unique_ptr<IcebergAuthorization> CreateAuthorization(ClientContext &, IcebergRestOptions &) override; // SigV4 'glue'
	IcebergRestBootstrap ResolveBootstrap(const IcebergRestOptions &) const override;       // URI from region, ':' warehouse
	unordered_set<string> DefaultEndpoints() const override;                                 // AddGlueEndpoints
	void AdjustCapabilities(IcebergBackendCapabilities &) const override;                    // CASCADE_ONLY
	IcebergCatalogStatus MapError(const HTTPResponse &, const IcebergErrorResponse &) const override; // NOT_ICEBERG
	void PrepareCreateTable(ClientContext &, const IcebergEntryId &, IcebergNewTable &) override; // location from ns
	void BuildStorageSecret(ClientContext &, const IcebergTableAccess &, CreateSecretInput &) const override; // s3 endpoint

private:
	string catalog_id;
	string region;
};
```

`IcebergS3TablesRestBackend` has the same shape. Its overrides are the ARN parsing in `ResolveBootstrap`,
`AddS3TablesEndpoints`, `delete_written_files = false`, and the `s3tables` branch of `LoadCredentials` in
`BuildStorageSecret`.

## 8. Mapping of existing catalog types

| Catalog | Backend | Notes |
|---|---|---|
| Generic REST (Polaris, Lakekeeper, Nessie, Gravitino, fixture, mocks) | `IcebergRestBackend` | Fully lazy. Capabilities from `/config` `endpoints` or the default set. Catalog differences stay in `/config` and in the `skip_tests` of `test/configs/*.json`. |
| AWS Glue REST endpoint (`ENDPOINT_TYPE 'glue'` or a Glue URI) | `IcebergGlueRestBackend` | §7.4. |
| AWS S3 Tables (`ENDPOINT_TYPE 's3_tables'`) | `IcebergS3TablesRestBackend` | §7.4. |
| SQL: DuckDB file, SQLite, Postgres (`CATALOG_TYPE 'sql'`) | `IcebergMetastoreBackend` + `IcebergSqlMetastore` | URI parsing (`IcebergSqlCatalogUri`), WAL/busy timeout and conflict mapping as in phase 4. Atomic. Views. |
| AWS Glue native (`CATALOG_TYPE 'glue'`) | `IcebergMetastoreBackend` + `IcebergGlueMetastore` | Phase 4.5's `IcebergGlueClient` (AWS JSON 1.1 over DuckDB HTTP, SigV4 via `AWSInput`). Non-atomic. |
| Filesystem (`CATALOG_TYPE 'filesystem'`) | `IcebergMetastoreBackend` + `IcebergFilesystemMetastore` | Local file system only, single writer, `PurgeLocation` "" (DROP removes the directory inside the warehouse). |

## 9. Code movement (from `upstream/main`)

| From | To | Kind |
|---|---|---|
| `catalog/rest/iceberg_catalog.cpp/.hpp` | `catalog/iceberg_catalog.*` (generic) + `catalog/rest/iceberg_rest_backend.*` (`Configure`, endpoints, prefix, base URL) | split |
| `catalog/rest/iceberg_schema_set.*`, `iceberg_table_set.*`, `iceberg_view_entry.*`, `iceberg_table_prefetch.*`, `iceberg_request_executor.hpp`, `iceberg_request_task.hpp`, `iceberg_file_credentials.hpp` | `catalog/` | move, then call the backend |
| `catalog/rest/catalog_entry/**` | `catalog/catalog_entry/**` | move; `IcebergTable` drops `rest_api_objects::StorageCredential` for `IcebergTableAccess` |
| `catalog/rest/iceberg_table_metadata_cache.hpp` (`IcebergLoadTableResponse`) | `catalog/iceberg_loaded_table.hpp` | replace |
| `LoadTableResultCache` (in `iceberg_catalog.hpp`) | `catalog/iceberg_loaded_table_cache.*` | move; value type changes |
| `catalog/rest/transaction/*` | `catalog/transaction/*` + `iceberg_commit_plan.*`, `iceberg_commit_staging.*` (phase 1), `iceberg_commit_retry.*` | move/split |
| REST commit sequence (`DoSchemaCreates` … `DropCreatedTables`) | `catalog/rest/iceberg_rest_commit.*` (phase 1) | move |
| `catalog/rest/api/catalog_api.*`, `url_utils.*`, `api_utils.*`, `iceberg_scan_planning.*` | stay in `catalog/rest/api/` | REST client |
| `catalog/rest/api/iceberg_table_update.*`, `table_update.*`, `iceberg_table_requirement.hpp`, `iceberg_add_snapshot.*`, `iceberg_manifest_merge.*`, `iceberg_create_table_request.*`, `iceberg_type.*`, `iceberg_expression.*`, `iceberg_retry.*` | `catalog/commit/` (staging and update building), `core/` for type conversion | move (neutral) |
| `catalog/rest/storage/authorization/*` | stay under `rest/` | |
| `catalog/rest/storage/aws.*` | `common/aws/` | move (also used by Glue native) |
| `catalog/rest/storage/iceberg_table_secret_provider.*` | `catalog/storage/` | move; refresh through `IcebergCredentialVending` |
| `iceberg_attach.cpp` | `iceberg_attach.cpp` (generic + registry) + `catalog/rest/iceberg_rest_attach.cpp` + `catalog/rest/vendor/*` | split |
| `planning/scan_plan/iceberg_scan_plan_provider.cpp` (request building) | `catalog/rest/iceberg_rest_scan_planning.cpp` | move |
| phase 2 `catalog/transaction/iceberg_client_commit*`, `iceberg_client_commit_verifier*` | `catalog/commit/` | reuse |
| phase 3–5 `catalog/metastore/*`, `catalog/sql/*`, `catalog/glue/*`, `catalog/filesystem/*` | same paths; `Iceberg*Catalog` → `Iceberg*Metastore` | reuse, adapt |

## 10. PR plan

Every PR keeps REST behaviour identical until PR 6. Each one runs:

- the native mock REST suite: `make test_mock_native_reldebug` and `build/reldebug/test/unittest_iceberg_native`;
- the Python mock (`make mock`) with the catalog-agnostic suite;
- the CI catalog configs (fixture, Lakekeeper, Nessie, Polaris).

| # | PR | Contents | Tests | Reuses |
|---|---|---|---|---|
| 1 | **Read seam** (small, behaviour-neutral) | Neutral types (§4.2, read half). `IcebergCatalogBackend` with the read methods only. `IcebergRestBackend` implements them by calling the existing `IRCAPI`/`Iceberg*Request` code. Schema set, schema entry, table set, prefetch and `RefreshFromCatalog` call `GetBackend()`. `allow_forbidden` policy moves into the table set. No file moves; error messages unchanged. ~600–900 lines. | Standard suites; `test_load_table_cache.cpp` for cache generations. | — |
| 2 | **Write seam** | `CreateTable` → `IcebergCreatedTable`. `IcebergCommitPlan` + shared staging + `IcebergCommitRetryLoop`. The REST sequence moves into `IcebergRestCommit`; `Abort`. Capability flags replace `supported_urls`/`endpoint_type` checks in the transaction and in `DropSchema`. HTTP → status mapping at one point. | Standard suites; the atomicity-message tests; commit retry/unknown-outcome tests in the native mock. | phase 1 `iceberg_commit_staging.*`, `iceberg_rest_commit.*` |
| 3 | **REST-only capabilities** | `IcebergLoadedTable` with `version_token` (ETag). `IcebergCredentialVending` (secret provider refresh). `IcebergServerScanPlanning`. Typed access (`Cast<T>`) for `iceberg_load_table_response`/`iceberg_view_metadata`. | Native mock ETag, credential-refresh and scan-planning tests. | — |
| 4 | **Registry, attach split, vendor subclasses** | The attach constructs `IcebergRestBackend` directly; the `CATALOG_TYPE` option and `IcebergBackendRegistry` follow in a later PR, with the first non-REST backend. `IcebergAttachOptions` split into generic + `IcebergRestOptions`. `ENDPOINT_TYPE` stays inside `IcebergRestBackend`; no `ENDPOINT_TYPE` branch left in generic code. The vendor subclasses (`IcebergGlueRestBackend`, `IcebergS3TablesRestBackend`, §7) follow in a later PR. | Attach option tests; `test/configs/cloud_s3tables.json` and the Glue cloud tests run manually. | summary's "REST extension point" gap |
| 5 | **File moves** (rename-only) | §9, mechanical, no logic. Can be folded into 1–4 if preferred (Q7). | Build + standard suites. | phase 1 layout |
| 6 | **Client-side commit** | `IcebergClientCommit` + `iceberg_verify_client_commit` shadow mode + the writer fixes. Independent of the seam after PR 2. | Shadow mode against the native mock and CI catalogs. | phase 2, nearly verbatim |
| 7 | **Metastore backend + SQL (DuckDB file)** | `IcebergMetastore`, `IcebergMetastoreCommit`, `IcebergMetastoreBackend`, `IcebergSqlMetastore`; `CATALOG_TYPE 'sql'`; `test/configs/sql.json`. | `make test_sql_catalog_reldebug` (`catalog_specifc/sql/` + the catalog-agnostic suite without `.test_slow`); fault injection tests. | phase 3 (commit sequence, SQL code) |
| 8 | **SQLite and Postgres** | URI forms, WAL/busy timeout, conflict mapping, `ICEBERG_SQL_CATALOG_POSTGRES=1`, PyIceberg interop. | `test_sql_catalog_sqlite_*`, `test_sql_catalog_postgres_*`, `test/python/sql_catalog/`. | phase 4, nearly verbatim |
| 9 | **Glue native** | `IcebergGlueMetastore`, `CATALOG_TYPE 'glue'`. | `make test_glue_catalog_*` against moto. | phase 4.5 |
| 10 | **Metastore views + filesystem** | View entries in the metastore commit; `IcebergFilesystemMetastore`. | `test_filesystem_catalog_*`, SQL view tests. | phase 5 |

PRs 1–5 are refactors that reviewers can check against the existing REST suites alone. PRs 6–10 add features,
and each comes with its own config. Phases 6/7 (Glue Hive tables) would later add an entry kind and an
"other format" hook on the backend rather than on the catalog. They stay out of scope.

## 11. Risks and trade-offs

- **The spec model stays generated REST classes.** Keeping `rest_api_objects::CommitTableRequest`,
  `TableUpdate`, `TableRequirement`, `TableMetadata` and `ViewMetadata` as the neutral model avoids a translation
  layer and lets `IcebergClientCommit` stay as written. The cost is that the "neutral" headers still include
  `rest_catalog/objects/*` (Q1).
- **Behaviour drift in error mapping.** Turning HTTP statuses into categories must keep today's texts
  (`"GetTableInformation endpoint returned response code …"`). Status and type stay in `message`.
- **Cache semantics.** `LoadTableCachePublication` encodes subtle invariants: superseded loads, authoritative
  overwrite, and identity-based eviction (`EvictIfCurrent`). The value-type swap in PR 3 needs the native cache
  tests to stay green, unchanged.
- **Thread-safety of metastore backends** under `IcebergRequestExecutor`. SQL uses one connection per call;
  Glue and filesystem are stateless per call.
- **Vendor subclassing widens the API surface.** Every protected virtual is a promise to external subclasses.
  The extension-point list should stay as small as the real cases in §7.1 justify.
- **A catalog-lifetime backend, unlike DuckLake.** There's no per-transaction metadata snapshot for SQL, so
  reads within one DuckDB transaction can see other writers' commits between statements, as REST does today
  (Q4).
- **Transaction model unchanged.** The alter/drop/rename variant and the "views alone" rule remain even for SQL,
  which could do better. That's deliberate for review size.

## 12. Decisions

The open questions of the proposal are settled:

1. **Spec model.** `rest_api_objects` stay the neutral update and requirement types for now, aliased where a
   neutral name helps (e.g. `using IcebergTableChange = rest_api_objects::CommitTableRequest`). Our own types can
   replace them later without changing the backend interface's shape.
2. **Metastore composition.** `IcebergMetastoreBackend` *owns* an `IcebergMetastore`. SQL, Glue and filesystem
   are metastores, not subclasses of the backend.
3. **Vendor packaging.** Glue REST and S3 Tables stay in this repository, as subclasses of `IcebergRestBackend`.
4. **Bulk listing.** `ListAllEntries` is a follow-up, not part of PR 7.
5. **Atomic catalog commit for SQL.** Lifting the mixed table/drop/rename and the views-alone restrictions for SQL
   catalogs is a follow-up.
6. **Staleness for metastores.** Metastore catalogs always revalidate. `MAX_TABLE_STALENESS` stays a REST-only
   option, so the load-table cache and its ETags belong to the REST backend, not to the generic catalog.
7. **File moves.** The moves of §9 are their own rename-only PR, after PRs 1–4.
8. **Names.** `'jdbc'` and `'hadoop'` will be accepted as aliases of `'sql'` and `'filesystem'`, in the phases that
   add those catalog types.
