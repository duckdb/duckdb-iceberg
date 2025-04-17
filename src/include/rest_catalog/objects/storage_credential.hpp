
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StorageCredential {
public:
	StorageCredential();
	StorageCredential(const StorageCredential &) = delete;
	StorageCredential &operator=(const StorageCredential &) = delete;
	StorageCredential(StorageCredential &&) = default;
	StorageCredential &operator=(StorageCredential &&) = default;

public:
	static StorageCredential FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string prefix;
	case_insensitive_map_t<string> config;
};

} // namespace rest_api_objects
} // namespace duckdb
