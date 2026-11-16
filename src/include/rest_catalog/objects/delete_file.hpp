
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/equality_delete_file.hpp"
#include "rest_catalog/objects/position_delete_file.hpp"

namespace duckdb {
namespace rest_api_objects {

class DeleteFile {
public:
	DeleteFile();
	DeleteFile(const DeleteFile &) = delete;
	DeleteFile &operator=(const DeleteFile &) = delete;
	DeleteFile(DeleteFile &&) = default;
	DeleteFile &operator=(DeleteFile &&) = default;

public:
	// Deserialization
	static DeleteFile FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	DeleteFile Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<PositionDeleteFile> position_delete_file;
	optional<EqualityDeleteFile> equality_delete_file;
};

} // namespace rest_api_objects
} // namespace duckdb
