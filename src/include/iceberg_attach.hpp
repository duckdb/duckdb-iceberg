#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/optional.hpp"
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

//! Who deletes a table's data and metadata files when it is dropped: nobody, the catalog, or this client. Exactly one
//! of them does, so the mode also fixes the purgeRequested flag of the DELETE request: only SERVER sends true.
//!
//! NONE: DROP TABLE only unregisters the table from the catalog (purgeRequested=false) and its files stay in storage,
//! which is what the REST spec and Spark's DROP TABLE without PURGE do, and the only mode whose DROP is not
//! destructive. It is the default: the files are not necessarily the table's to delete (a location shared with
//! another table, data registered from elsewhere, a table about to be re-registered under another catalog or name),
//! DuckDB has no DROP TABLE ... PURGE clause to ask for deletion per statement, and deleting by default would turn
//! every existing catalog's DROP TABLE destructive. The files can still be removed later by the catalog's own
//! maintenance or a remove-orphan-files pass.
//!
//! SERVER asks the catalog to delete the files (purgeRequested=true); catalogs that own the table's storage, such as
//! AWS S3 Tables, require this and refuse a drop without it.
//!
//! CLIENT unregisters the table (purgeRequested=false) and then deletes the files the table metadata references, the
//! way Iceberg's CatalogUtil.dropTableData does; for catalogs that only hold a pointer to storage they do not manage
//! and reject purgeRequested=true, such as the AWS Glue Iceberg REST endpoint for Glue tables.
enum class IcebergPurgeMode : uint8_t { NONE, SERVER, CLIENT };

struct IcebergAttachOptions {
	string catalog_uri;
	string warehouse;
	string secret;
	string name;
	// some catalogs do not yet support stage create
	bool stage_create_tables = true;
	// some catalogs reject the multi-table transactions/commit endpoint; opt out of it here
	bool disable_multi_table_commit = false;
	// some catalogs fully initialize metadata during non-staged CREATE TABLE and reject follow-up metadata updates
	bool skip_create_table_metadata_updates = false;
	// if the catalog allows manual cleaning up of storage files.
	bool remove_files_on_delete = true;
	bool support_nested_namespaces = false;
	bool encode_entire_prefix = false;
	// in rest api spec, purge requested defaults to false: a DROP TABLE leaves the table's files in storage.
	IcebergPurgeMode purge_mode = IcebergPurgeMode::NONE;
	// some catalogs (e.g. AWS Glue) do not assign a table location server-side; derive one from the namespace's
	// 'location' property
	bool default_table_location_from_namespace = false;
	// load the authoritative table metadata from the LoadTableResult's 'metadata-location' instead of
	// the embedded response metadata, see duckdb-iceberg#1403
	bool use_metadata_location = false;
	IcebergTableResolution table_resolution = IcebergTableResolution::LAZY;
	IRCAccessDelegationMode access_mode = IRCAccessDelegationMode::VENDED_CREDENTIALS;
	IcebergAuthorizationType authorization_type = IcebergAuthorizationType::INVALID;
	unordered_map<string, Value> options;
	// max staleness for cached table metadata in minutes (optional - if not set, always request fresh metadata)
	optional_idx max_table_staleness_micros;
};

unordered_map<string, Value> NormalizeIcebergAttachOptions(const unordered_map<string, Value> &options);

struct IcebergAttach {
	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                  AttachedDatabase &db, const string &name, AttachInfo &info,
	                                  AttachOptions &options);
};

} // namespace duckdb
