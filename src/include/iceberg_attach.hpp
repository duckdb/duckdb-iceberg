#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

enum class IcebergEndpointType : uint8_t { AWS_S3TABLES, AWS_GLUE, INVALID };

enum class IcebergAuthorizationType : uint8_t { OAUTH2, SIGV4, NONE, INVALID };

enum class IRCAccessDelegationMode : uint8_t { NONE, VENDED_CREDENTIALS };

//! Whether listing the tables of a schema also resolves their columns.
//! LAZY surfaces a placeholder entry per table and only loads a table when it is actually referenced.
//! EAGER issues a LoadTable request for every listed table, so 'SHOW ALL TABLES' and
//! 'information_schema.columns' report real columns, at the cost of one request per table.
enum class IcebergTableResolution : uint8_t { LAZY, EAGER };

//! How ATTACH treats the namespace named by DEFAULT_SCHEMA:
//!   THROW      - check it exists, fail the ATTACH if it does not
//!   CREATE     - check it exists, create it if it does not
//!   UNENFORCED - do not check at ATTACH; the first use of the default schema checks it instead
enum class IcebergDefaultSchemaPolicy : uint8_t { THROW, CREATE, UNENFORCED };

//! How namespace and table names are sent to the catalog. LOWERCASE suits catalogs that only accept
//! (S3 Tables) or only store (Glue) lowercase names; column names are never changed.
enum class IcebergIdentifierCase : uint8_t { PRESERVE_CASE, LOWERCASE };

//! The ATTACH options every Iceberg catalog type accepts. The options of a catalog type (e.g. IcebergRestOptions)
//! belong to its backend.
struct IcebergAttachOptions {
	IcebergTableResolution table_resolution = IcebergTableResolution::LAZY;
	IcebergDefaultSchemaPolicy default_schema_policy = IcebergDefaultSchemaPolicy::THROW;
	IcebergIdentifierCase identifier_case = IcebergIdentifierCase::PRESERVE_CASE;
};

//! An ATTACH of an Iceberg catalog, as the catalog backends see it
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
	//! The generic options, which the backend fills in (it may take some from its catalog's configuration)
	IcebergAttachOptions attach_options;
	optional<Identifier> default_schema;
};

unordered_map<string, Value> NormalizeIcebergAttachOptions(const unordered_map<string, Value> &options);
//! Translate supported catalog properties to the same option names used by ATTACH.
//! Unknown server properties are ignored; unknown ATTACH options are still rejected.
unordered_map<string, Value> NormalizeIcebergCatalogOptions(const case_insensitive_map_t<string> &properties);
//! Merge one normalized property layer, including individual extra HTTP headers.
void MergeIcebergOptions(unordered_map<string, Value> &target, const unordered_map<string, Value> &source);
bool IsIcebergAuthorizationOption(const string &name);
void FilterIcebergAuthorizationOptions(unordered_map<string, Value> &options, IcebergAuthorizationType type);

//! ATTACH ... (TYPE ICEBERG): creates the REST backend, and the
//! IcebergCatalog that owns it
struct IcebergAttach {
	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                  AttachedDatabase &db, const string &name, AttachInfo &info,
	                                  AttachOptions &options);
};

} // namespace duckdb
