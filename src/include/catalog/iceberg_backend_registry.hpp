//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/iceberg_backend_registry.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/extension_callback.hpp"

#include "catalog/iceberg_catalog_backend.hpp"
#include "iceberg_attach.hpp"

namespace duckdb {

//! Checked when a factory is registered, so a backend compiled against other iceberg sources is refused rather than
//! crashing on a changed interface
static constexpr uint32_t ICEBERG_BACKEND_API_VERSION = 1;

//! An ATTACH of an Iceberg catalog, as backend factories see it
struct IcebergAttachInput {
public:
	IcebergAttachInput(ClientContext &context, AttachedDatabase &db, const string &name, AttachInfo &info,
	                   AttachOptions &options);

public:
	//! Parses an option every catalog type accepts into 'attach_options' and 'default_schema'. Returns false for
	//! any other option
	bool ParseGenericOption(const string &option_name, const Value &value);
	//! Checks that DEFAULT_SCHEMA_POLICY comes with a DEFAULT_SCHEMA it can apply to
	void VerifyDefaultSchemaPolicy() const;

public:
	ClientContext &context;
	AttachedDatabase &db;
	//! The name the catalog is attached as
	const string name;
	AttachInfo &info;
	AttachOptions &options;
	//! The ATTACH options, normalized: lowercase names, aliases resolved
	unordered_map<string, Value> client_options;
	//! CATALOG_TYPE, lowercase ('rest' if not given)
	string catalog_type;
	//! ENDPOINT_TYPE, lowercase ('' if not given)
	string endpoint_type;
	//! The generic options, which the backend fills in (it may take some from its catalog's configuration)
	IcebergAttachOptions attach_options;
	optional<Identifier> default_schema;
};

//! Creates the backend of one catalog type, or of one vendor's flavour of it
struct IcebergBackendFactory {
	//! e.g. "rest"
	string catalog_type;
	//! "" for the plain backend of the catalog type; e.g. "glue", "s3_tables" for vendor REST endpoints
	string endpoint_type;
	//! Recognizes an attach without ENDPOINT_TYPE, e.g. a glue.<region>.amazonaws.com URI. Optional
	std::function<bool(const IcebergAttachInput &input)> matches;
	//! Creates the backend; parses and validates the options of the catalog type, and rejects unknown options
	std::function<unique_ptr<IcebergCatalogBackend>(IcebergAttachInput &input)> create;
	uint32_t api_version = ICEBERG_BACKEND_API_VERSION;
};

//! The backend factories of a database. The iceberg extension registers its own at load; another extension can
//! register more through the same instance, which it finds among the database's extension callbacks.
class IcebergBackendRegistry : public ExtensionCallback {
public:
	static IcebergBackendRegistry &Get(DatabaseInstance &db);

public:
	//! Throws on a duplicate (catalog_type, endpoint_type) or an API version mismatch
	void Register(IcebergBackendFactory factory);
	//! The factory for an attach: the one of its ENDPOINT_TYPE, else the first that matches it, else the plain
	//! backend of its catalog type (which rejects an unknown ENDPOINT_TYPE itself)
	IcebergBackendFactory Resolve(const IcebergAttachInput &input) const;
	//! The endpoint types registered for a catalog type, sorted
	vector<string> GetEndpointTypes(const string &catalog_type) const;

private:
	mutable mutex lock;
	vector<IcebergBackendFactory> factories;
};

} // namespace duckdb
