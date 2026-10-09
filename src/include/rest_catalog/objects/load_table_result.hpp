
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/labels.hpp"
#include "rest_catalog/objects/read_restrictions.hpp"
#include "rest_catalog/objects/remote_signing_config.hpp"
#include "rest_catalog/objects/storage_credential.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

namespace duckdb {
namespace rest_api_objects {

class LoadTableResult {
public:
	LoadTableResult();
	LoadTableResult(const LoadTableResult &) = delete;
	LoadTableResult &operator=(const LoadTableResult &) = delete;
	LoadTableResult(LoadTableResult &&) = default;
	LoadTableResult &operator=(LoadTableResult &&) = default;

public:
	// Deserialization
	static LoadTableResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	LoadTableResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	TableMetadata metadata;
	optional<string> metadata_location;
	optional<case_insensitive_map_t<string>> config;
	optional<vector<StorageCredential>> storage_credentials;
	optional<RemoteSigningConfig> remote_signing_config;
	optional<ReadRestrictions> read_restrictions;
	optional<Labels> labels;
};

} // namespace rest_api_objects
} // namespace duckdb
