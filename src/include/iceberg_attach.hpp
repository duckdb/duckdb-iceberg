#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "catalog/rest/case_aware_identifier_container.hpp"

namespace duckdb {

enum class IcebergEndpointType : uint8_t { AWS_S3TABLES, AWS_GLUE, INVALID };

enum class IcebergAuthorizationType : uint8_t { OAUTH2, SIGV4, NONE, INVALID };

enum class IRCAccessDelegationMode : uint8_t { NONE, VENDED_CREDENTIALS };

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
	// in rest api spec, purge requested defaults to false.
	bool purge_requested = false;
	// some catalogs (e.g. AWS Glue) do not assign a table location server-side; derive one from the namespace's
	// 'location' property
	bool default_table_location_from_namespace = false;
	IRCAccessDelegationMode access_mode = IRCAccessDelegationMode::VENDED_CREDENTIALS;
	IcebergAuthorizationType authorization_type = IcebergAuthorizationType::INVALID;
	unordered_map<string, Value> options;
	// max staleness for cached table metadata in minutes (optional - if not set, always request fresh metadata)
	optional_idx max_table_staleness_micros;
	// controls case-sensitivity of catalog/schema/table identifier lookups against this catalog.
	// UNSET preserves today's (per-field) default behavior for backwards compatibility.
	CaseSensitivityMode case_sensitivity_mode = CaseSensitivityMode::UNSET;
};

unordered_map<string, Value> NormalizeIcebergAttachOptions(const unordered_map<string, Value> &options);

struct IcebergAttach {
	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                  AttachedDatabase &db, const string &name, AttachInfo &info,
	                                  AttachOptions &options);
};

} // namespace duckdb
