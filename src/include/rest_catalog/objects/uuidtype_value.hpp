
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UUIDTypeValue {
public:
	UUIDTypeValue();
	UUIDTypeValue(const UUIDTypeValue &) = delete;
	UUIDTypeValue &operator=(const UUIDTypeValue &) = delete;
	UUIDTypeValue(UUIDTypeValue &&) = default;
	UUIDTypeValue &operator=(UUIDTypeValue &&) = default;

public:
	static UUIDTypeValue FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
