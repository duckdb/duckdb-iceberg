
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MultiValuedMap {
public:
	MultiValuedMap();
	MultiValuedMap(const MultiValuedMap &) = delete;
	MultiValuedMap &operator=(const MultiValuedMap &) = delete;
	MultiValuedMap(MultiValuedMap &&) = default;
	MultiValuedMap &operator=(MultiValuedMap &&) = default;

public:
	// Deserialization
	static MultiValuedMap FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	MultiValuedMap Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	case_insensitive_map_t<vector<string>> additional_properties;
};

} // namespace rest_api_objects
} // namespace duckdb
