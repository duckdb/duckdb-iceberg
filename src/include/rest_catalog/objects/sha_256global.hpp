
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Sha256Global {
public:
	Sha256Global();
	Sha256Global(const Sha256Global &) = delete;
	Sha256Global &operator=(const Sha256Global &) = delete;
	Sha256Global(Sha256Global &&) = default;
	Sha256Global &operator=(Sha256Global &&) = default;

public:
	// Deserialization
	static Sha256Global FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Sha256Global Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string action;
	int32_t field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
