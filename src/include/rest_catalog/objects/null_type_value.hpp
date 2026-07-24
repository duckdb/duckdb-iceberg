
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class NullTypeValue {
public:
	NullTypeValue();
	NullTypeValue(const NullTypeValue &) = delete;
	NullTypeValue &operator=(const NullTypeValue &) = delete;
	NullTypeValue(NullTypeValue &&) = default;
	NullTypeValue &operator=(NullTypeValue &&) = default;

public:
	// Deserialization
	static NullTypeValue FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	NullTypeValue Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	void *value;
};

} // namespace rest_api_objects
} // namespace duckdb
