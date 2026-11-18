//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_rest_options.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "iceberg_attach.hpp"

namespace duckdb {

//! The ATTACH options of the REST backend, after the catalog's /v1/config was applied
struct IcebergRestOptions {
	string catalog_uri;
	string warehouse;
	string prefix;
	string namespace_separator = "\x1f";
	string default_region;
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
	// load the authoritative table metadata from the LoadTableResult's 'metadata-location' instead of
	// the embedded response metadata, see duckdb-iceberg#1403
	bool use_metadata_location = false;
	IRCAccessDelegationMode access_mode = IRCAccessDelegationMode::VENDED_CREDENTIALS;
	IcebergAuthorizationType authorization_type = IcebergAuthorizationType::INVALID;
	unordered_map<string, Value> options;
	// max staleness for cached table metadata in minutes (optional - if not set, always request fresh metadata)
	optional_idx max_table_staleness_micros;
};

} // namespace duckdb
