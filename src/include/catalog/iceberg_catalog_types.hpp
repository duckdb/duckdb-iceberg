//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/iceberg_catalog_types.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

#include <functional>

#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/sort/iceberg_sort_order.hpp"
#include "rest_catalog/objects/commit_table_request.hpp"
#include "rest_catalog/objects/table_metadata.hpp"
#include "rest_catalog/objects/view_metadata.hpp"

//! The types a catalog backend exchanges with the rest of the extension. They name catalog concepts (namespaces,
//! entries, loaded tables and views, outcome categories), never protocol details: HTTP status codes and REST
//! responses stay inside the REST backend. The generated 'rest_api_objects' classes of the Iceberg spec model
//! (table and view metadata, table updates and requirements) are used as the neutral spec representation.

namespace duckdb {

class ClientContext;
struct IcebergTableMetadata;
struct IcebergTable;

enum class IcebergEntryKind : uint8_t { TABLE, VIEW };

//! A namespace, as the list of its levels
struct IcebergNamespace {
public:
	IcebergNamespace() {
	}
	explicit IcebergNamespace(vector<string> levels) : levels(std::move(levels)) {
	}

public:
	//! Splits a DuckDB schema name into its levels at every '.'
	static IcebergNamespace Parse(const string &schema_name);
	//! The levels joined with '.', as in DuckDB schema names
	string ToString() const;

public:
	vector<string> levels;
};

//! A table or view in a namespace
struct IcebergEntryId {
public:
	IcebergEntryId(IcebergNamespace ns, string name) : ns(std::move(ns)), name(std::move(name)) {
	}

public:
	IcebergNamespace ns;
	string name;
};

//! An entry reported by a listing
struct IcebergListedEntry {
	string name;
	IcebergEntryKind kind = IcebergEntryKind::TABLE;
};

//! The outcome categories of a backend operation. Protocol details (HTTP status codes, REST error types) only appear
//! in the message of the error.
enum class IcebergCatalogStatus : uint8_t {
	//! The operation succeeded. An IcebergCatalogResult without an error has this status; an IcebergCatalogError never
	//! does (the REST backend maps HTTP 200, 204 and 304 to it)
	OK,
	//! The namespace or entry doesn't exist
	NOT_FOUND,
	//! The entry exists, but isn't an Iceberg table
	NOT_ICEBERG,
	//! Not visible to this principal. A refused listing is not an empty one
	FORBIDDEN,
	//! A create of an entry or namespace that already exists
	ALREADY_EXISTS,
	//! A requirement of a commit failed and nothing was published; the commit may be retried
	CONFLICT,
	//! The change may have been published (e.g. a server error, a lost connection)
	UNKNOWN_OUTCOME,
	//! The backend can't do this
	UNSUPPORTED,
	//! Any other definite failure
	FAILED
};

//! A categorized failure. 'error' is what the caller raises when it does not handle the category itself, so the
//! messages and exception types users see are those of the backend.
struct IcebergCatalogError {
public:
	IcebergCatalogError(IcebergCatalogStatus status, ErrorData error, string error_type = string());

public:
	[[noreturn]] void Throw() const;

public:
	IcebergCatalogStatus status;
	ErrorData error;
	//! The backend's own name for the error (e.g. the type of a REST error response), for logs
	string error_type;
};

template <class T>
struct IcebergCatalogResult {
public:
	IcebergCatalogResult() {
	}
	IcebergCatalogResult(T value_p) : value(std::move(value_p)) { // NOLINT: allow implicit conversion
	}
	IcebergCatalogResult(IcebergCatalogError error_p) : error(std::move(error_p)) { // NOLINT
	}

public:
	bool Ok() const {
		return !error;
	}
	IcebergCatalogStatus Status() const {
		return error ? error->status : IcebergCatalogStatus::OK;
	}

public:
	//! May be partial when there is an error (e.g. the namespaces listed before a page was refused)
	T value;
	optional<IcebergCatalogError> error;
};

//! Storage credentials that apply to the files under 'prefix'
struct IcebergStorageCredential {
	string prefix;
	case_insensitive_map_t<string> config;
};

//! The storage configuration and credentials a catalog returns with a table. Empty for catalogs that don't vend any.
struct IcebergTableAccess {
	case_insensitive_map_t<string> config;
	vector<IcebergStorageCredential> storage_credentials;
};

//! One immutable generation of a table, as a backend loaded it. It can be shared between transactions: a revalidation
//! that finds the table unchanged returns the same object, a new version is a new object. Backends add their own
//! state (e.g. the REST response) in a subclass.
class IcebergLoadedTable {
public:
	explicit IcebergLoadedTable(IcebergTableAccess access);
	virtual ~IcebergLoadedTable();

public:
	//! The table metadata of this generation, as the catalog returned it
	virtual const rest_api_objects::TableMetadata &GetSpecMetadata() const = 0;
	//! The parsed table metadata of this generation. Parsed at most once, and shared by every table that is
	//! initialized from this generation
	virtual shared_ptr<const IcebergTableMetadata> GetMetadata(ClientContext &context,
	                                                           const IcebergTable &table) const = 0;
	const IcebergTableAccess &GetAccess() const {
		return access;
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

private:
	//! Credentials and transaction-local changes never enter this generation
	const IcebergTableAccess access;
};

//! A view, as a backend loaded it
struct IcebergLoadedView {
	//! The view metadata, in the spec model
	rest_api_objects::ViewMetadata metadata;
};

//! One load of one table. The catalog creates it on the thread that looks the table up, may execute it on a worker
//! of the request executor, and publishes the result from the creating thread. A backend keeps whatever revalidation
//! state it needs (e.g. the generation it revalidates and its validator) in its subclass.
class IcebergTableLoad {
public:
	explicit IcebergTableLoad(IcebergEntryId id);
	virtual ~IcebergTableLoad();

public:
	//! Loads the table. Called at most once, possibly on another thread, and must only use the state of this object
	//! and the (thread-safe) backend
	virtual IcebergCatalogResult<shared_ptr<const IcebergLoadedTable>> Execute(ClientContext &context) = 0;
	//! Called on the creating thread after Execute succeeded: offers 'table' as the shared generation of the table.
	//! Returns whether it is, i.e. whether a later change of the table must invalidate it
	virtual bool Publish(shared_ptr<const IcebergLoadedTable> table);

public:
	const IcebergEntryId id;
};

//! The spec's change of one table: its identifier, requirements and updates
using IcebergTableChange = rest_api_objects::CommitTableRequest;

//! A table to create; everything DuckDB decides about it
struct IcebergNewTable {
	IcebergNewTable(string name_p, shared_ptr<IcebergTableSchema> schema_p, IcebergPartitionSpec partition_spec_p,
	                IcebergSortOrder sort_order_p, idx_t format_version_p)
	    : name(std::move(name_p)), schema(std::move(schema_p)), partition_spec(std::move(partition_spec_p)),
	      sort_order(std::move(sort_order_p)), format_version(format_version_p) {
	}

	string name;
	shared_ptr<IcebergTableSchema> schema;
	IcebergPartitionSpec partition_spec;
	IcebergSortOrder sort_order;
	idx_t format_version;
	case_insensitive_map_t<string> properties;
	//! Empty: the backend chooses
	string location;
	//! The properties of the namespace, loaded on first use
	std::function<const case_insensitive_map_t<string> &()> namespace_properties;
};

//! What a backend did to create a table at statement time
struct IcebergCreatedTable {
	//! The metadata the table's first commit is staged against
	shared_ptr<const IcebergLoadedTable> initial;
	//! The table was published in the catalog already, ahead of the commit; an abort must drop it
	bool exists_in_catalog = false;
	//! The first commit asserts that the table doesn't exist yet
	bool needs_assert_create = true;
	//! The first commit carries the updates that set up the new table's metadata
	bool add_create_updates = true;
};

//! A view to create, as DuckDB defines it
struct IcebergNewView {
	IcebergEntryId id;
	string sql;
	//! The output columns, with unique names
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! The namespace unqualified references in 'sql' resolve against
	IcebergNamespace default_namespace;
};

//! The property changes of one namespace
struct IcebergNamespacePropertyUpdate {
	IcebergNamespace ns;
	case_insensitive_map_t<string> updates;
	vector<string> removals;
};

} // namespace duckdb
