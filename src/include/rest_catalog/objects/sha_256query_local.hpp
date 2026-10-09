
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Sha256QueryLocal {
public:
	Sha256QueryLocal();
	Sha256QueryLocal(const Sha256QueryLocal &) = delete;
	Sha256QueryLocal &operator=(const Sha256QueryLocal &) = delete;
	Sha256QueryLocal(Sha256QueryLocal &&) = default;
	Sha256QueryLocal &operator=(Sha256QueryLocal &&) = default;

public:
	// Deserialization
	static Sha256QueryLocal FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Sha256QueryLocal Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string action;
	int32_t field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
