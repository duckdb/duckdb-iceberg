//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/iceberg_catalog_backend.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include "catalog/iceberg_catalog_types.hpp"

namespace duckdb {

class IcebergCommitPlan;
class IcebergScanOrder;
struct IcebergTableFilters;
class SecretEntry;
struct CreateSecretInput;
struct IcebergScanPlanContext;
struct IcebergServerSideScanPlan;

//! Credentials a catalog vended anew for a table
struct IcebergVendedCredentials {
	vector<IcebergStorageCredential> storage_credentials;
	//! Table config that replaces the config the table was loaded with; unset keeps it
	optional<case_insensitive_map_t<string>> config;
};

//! Optional backend functionality: the catalog vends storage credentials with its tables (IcebergTableAccess) and
//! refreshes them when they expire.
class IcebergCredentialVending {
public:
	virtual ~IcebergCredentialVending();

public:
	//! Whether the tables of this catalog are read with the credentials it vends
	virtual bool VendsCredentials() const = 0;
	//! The secret options every vended secret of this catalog starts from (e.g. the region of the catalog's own
	//! credentials)
	virtual case_insensitive_map_t<Value> GetSecretDefaults(ClientContext &context) = 0;
	//! Adjusts the secret built from a table's vended config before it is stored
	virtual void AdjustConfigSecret(ClientContext &context, CreateSecretInput &info) = 0;
	//! The HTTP secret (e.g. a proxy) whose options every vended secret carries, if any
	virtual unique_ptr<SecretEntry> GetHTTPSecret(ClientContext &context) = 0;
	//! Newly vended credentials for a table
	virtual IcebergCatalogResult<IcebergVendedCredentials> LoadCredentials(ClientContext &context,
	                                                                       const IcebergEntryId &id) = 0;
};

//! Optional backend functionality: the catalog plans table scans on the server.
class IcebergServerScanPlanning {
public:
	virtual ~IcebergServerScanPlanning();

public:
	//! Whether the catalog plans scans at all
	virtual bool PlansScans() const = 0;
	//! Plans the scan of 'table' at the snapshot of 'context'. Returns false if the server declines to plan it;
	//! throws if planning fails
	virtual bool PlanScan(IcebergScanPlanContext &context, IcebergTable &table, const IcebergTableFilters &filters,
	                      const IcebergScanOrder &scan_order, IcebergServerSideScanPlan &result) = 0;
};

//! How DROP SCHEMA behaves
enum class IcebergDropNamespaceMode : uint8_t {
	//! A namespace must be empty to be dropped; CASCADE is refused
	RESTRICT,
	//! Dropping a namespace drops its tables, so DROP SCHEMA requires CASCADE
	CASCADE_ONLY
};

//! What a backend supports. The shared code checks these at statement time, to fail early with the same messages
//! for every backend.
struct IcebergBackendCapabilities {
	//! Views can be listed, loaded and created
	bool list_views = false;
	bool load_views = false;
	bool create_views = false;
	IcebergDropNamespaceMode drop_namespace = IcebergDropNamespaceMode::RESTRICT;
	//! The changes of several tables are published atomically
	bool atomic_multi_table_commit = false;
	//! A table created in the transaction can be part of that atomic multi-table commit
	bool atomic_commit_with_creates = false;
	//! Table changes, drops and renames, namespaces and views are published together
	bool atomic_catalog_commit = false;
	//! DuckDB may delete the files it wrote when a commit definitely failed
	bool delete_written_files = true;
};

//! Backend type ids are strings, so another extension can add a backend type, and Cast<T>() works across shared
//! libraries without RTTI
using IcebergBackendTypeId = const char *;

//! The part of an Iceberg catalog that differs per catalog type: the protocol to the catalog service and its state.
//! An IcebergCatalog owns one backend for as long as it is attached. Backends must be thread-safe: they are called
//! concurrently by transactions and by the workers of the request executor. A backend never touches catalog entries,
//! table sets or transactions; it takes ids and returns values.
class IcebergCatalogBackend {
public:
	static constexpr IcebergBackendTypeId TYPE = "iceberg.backend";

public:
	IcebergCatalogBackend();
	virtual ~IcebergCatalogBackend();

public:
	//! The catalog type in user-facing messages, e.g. "REST Catalog"
	virtual string Name() const = 0;
	//! The product behind the catalog in user-facing messages, e.g. "AWS Glue"
	virtual string ProductName() const {
		return Name();
	}
	const IcebergBackendCapabilities &Capabilities() const {
		return capabilities;
	}

	//===--------------------------------------------------------------------===//
	// Namespaces
	//===--------------------------------------------------------------------===//
	//! The namespace a DuckDB schema name refers to
	virtual IcebergNamespace ParseNamespace(const string &schema_name) const;
	//! Throws if the existence can't be established (e.g. the request is refused)
	virtual bool NamespaceExists(ClientContext &context, const IcebergNamespace &ns) = 0;
	//! Lists the namespaces under 'parent' (all levels, if the backend lists nested namespaces). A refused listing
	//! returns the namespaces listed so far
	virtual vector<IcebergNamespace> ListNamespaces(ClientContext &context, const IcebergNamespace &parent) = 0;
	virtual IcebergCatalogResult<case_insensitive_map_t<string>>
	LoadNamespaceProperties(ClientContext &context, const IcebergNamespace &ns) = 0;

	//===--------------------------------------------------------------------===//
	// Tables and views
	//===--------------------------------------------------------------------===//
	//! The key that identifies a table or view of this catalog in caches and transactions
	virtual string GetEntryKey(const IcebergNamespace &ns, const string &name) const;
	//! Lists the tables or views of a namespace. A missing namespace has no entries; a refused listing returns
	//! FORBIDDEN, and the caller decides whether that is an error
	virtual IcebergCatalogResult<vector<IcebergListedEntry>>
	ListEntries(ClientContext &context, const IcebergNamespace &ns, IcebergEntryKind kind) = 0;
	//! A generation of the table that this transaction may use without loading it (e.g. within a staleness bound)
	virtual shared_ptr<const IcebergLoadedTable> GetCachedTable(ClientContext &context, const string &table_key);
	//! Begins a load of a table. Returns NOT_FOUND for a missing table and NOT_ICEBERG for a table that isn't an
	//! Iceberg table when executed
	virtual unique_ptr<IcebergTableLoad> BeginLoadTable(ClientContext &context, IcebergEntryId id,
	                                                    const string &table_key) = 0;
	//! A table was created with this initial generation
	virtual void OnTableCreated(const string &table_key, shared_ptr<const IcebergLoadedTable> table);
	//! A table that was initialized from 'table' changed or was dropped, or may have
	virtual void OnTableChanged(const string &table_key, optional_ptr<const IcebergLoadedTable> table, bool dropped);
	virtual IcebergCatalogResult<shared_ptr<const IcebergLoadedView>> LoadView(ClientContext &context,
	                                                                           const IcebergEntryId &id);
	//! Table properties the catalog applies to all of its tables, below a table's own configuration
	virtual const case_insensitive_map_t<string> &GetCatalogProperties() const;

	//===--------------------------------------------------------------------===//
	// Statement time
	//===--------------------------------------------------------------------===//
	//! Prepares the creation of a table (e.g. to obtain its location); the table is committed with the transaction
	virtual IcebergCreatedTable CreateTable(ClientContext &context, const IcebergEntryId &id,
	                                        IcebergNewTable &table) = 0;

	//===--------------------------------------------------------------------===//
	// Commit
	//===--------------------------------------------------------------------===//
	//! Publishes the changes of a transaction. On failure, calls MarkOutcomeUnknown on the plan if the changes may
	//! have been published, and throws
	virtual void Commit(ClientContext &context, IcebergCommitPlan &plan) = 0;
	//! Undoes what was published ahead of the commit (CreatedTables of the plan). Must not throw
	virtual void Abort(ClientContext &context, IcebergCommitPlan &plan);

	//===--------------------------------------------------------------------===//
	// Optional functionality
	//===--------------------------------------------------------------------===//
	virtual optional_ptr<IcebergCredentialVending> GetCredentialVending() {
		return nullptr;
	}
	virtual optional_ptr<IcebergServerScanPlanning> GetServerScanPlanning() {
		return nullptr;
	}

	//===--------------------------------------------------------------------===//
	// Typed access, for the functionality of one backend type
	//===--------------------------------------------------------------------===//
	virtual bool IsInstanceOf(IcebergBackendTypeId id) const {
		return StringUtil::Equals(id, TYPE);
	}
	template <class TARGET>
	TARGET &Cast() {
		if (!IsInstanceOf(TARGET::TYPE)) {
			throw InvalidInputException("This Iceberg catalog is a %s, not a %s", Name(), TARGET::DESCRIPTION);
		}
		return static_cast<TARGET &>(*this);
	}
	template <class TARGET>
	optional_ptr<TARGET> TryCast() {
		if (!IsInstanceOf(TARGET::TYPE)) {
			return nullptr;
		}
		return static_cast<TARGET *>(this);
	}

protected:
	IcebergBackendCapabilities capabilities;
};

//===--------------------------------------------------------------------===//
// Backend calls the IcebergRequestExecutor runs on its workers. Each owns its inputs.
//===--------------------------------------------------------------------===//
struct IcebergListNamespacesOp {
	using Result = vector<IcebergNamespace>;
	IcebergNamespace parent;
	Result Execute(ClientContext &context, IcebergCatalogBackend &backend) const {
		return backend.ListNamespaces(context, parent);
	}
};

struct IcebergListEntriesOp {
	using Result = IcebergCatalogResult<vector<IcebergListedEntry>>;
	IcebergNamespace ns;
	IcebergEntryKind kind;
	Result Execute(ClientContext &context, IcebergCatalogBackend &backend) const {
		return backend.ListEntries(context, ns, kind);
	}
};

struct IcebergLoadTableOp {
	using Result = IcebergCatalogResult<shared_ptr<const IcebergLoadedTable>>;
	//! Shared with the caller, which publishes the result
	shared_ptr<IcebergTableLoad> load;
	Result Execute(ClientContext &context, IcebergCatalogBackend &backend) const {
		return load->Execute(context);
	}
};

struct IcebergLoadViewOp {
	using Result = IcebergCatalogResult<shared_ptr<const IcebergLoadedView>>;
	IcebergEntryId id;
	Result Execute(ClientContext &context, IcebergCatalogBackend &backend) const {
		return backend.LoadView(context, id);
	}
};

} // namespace duckdb
