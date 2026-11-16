
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class UpgradeFormatVersionUpdate {
public:
	UpgradeFormatVersionUpdate();
	UpgradeFormatVersionUpdate(const UpgradeFormatVersionUpdate &) = delete;
	UpgradeFormatVersionUpdate &operator=(const UpgradeFormatVersionUpdate &) = delete;
	UpgradeFormatVersionUpdate(UpgradeFormatVersionUpdate &&) = default;
	UpgradeFormatVersionUpdate &operator=(UpgradeFormatVersionUpdate &&) = default;

public:
	// Deserialization
	static UpgradeFormatVersionUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	UpgradeFormatVersionUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	int32_t format_version;
};

} // namespace rest_api_objects
} // namespace duckdb
