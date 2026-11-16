
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Namespace {
public:
	Namespace();
	Namespace(const Namespace &) = delete;
	Namespace &operator=(const Namespace &) = delete;
	Namespace(Namespace &&) = default;
	Namespace &operator=(Namespace &&) = default;

public:
	// Deserialization
	static Namespace FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Namespace Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<string> value;
};

} // namespace rest_api_objects
} // namespace duckdb
