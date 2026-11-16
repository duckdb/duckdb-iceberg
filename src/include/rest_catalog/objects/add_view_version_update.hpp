
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/view_version.hpp"

namespace duckdb {
namespace rest_api_objects {

class AddViewVersionUpdate {
public:
	AddViewVersionUpdate();
	AddViewVersionUpdate(const AddViewVersionUpdate &) = delete;
	AddViewVersionUpdate &operator=(const AddViewVersionUpdate &) = delete;
	AddViewVersionUpdate(AddViewVersionUpdate &&) = default;
	AddViewVersionUpdate &operator=(AddViewVersionUpdate &&) = default;

public:
	// Deserialization
	static AddViewVersionUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AddViewVersionUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	ViewVersion view_version;
};

} // namespace rest_api_objects
} // namespace duckdb
