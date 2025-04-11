
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class BlobMetadata {
public:
	BlobMetadata();
	BlobMetadata(const BlobMetadata &) = delete;
	BlobMetadata &operator=(const BlobMetadata &) = delete;
	BlobMetadata(BlobMetadata &&) = default;
	BlobMetadata &operator=(BlobMetadata &&) = default;

public:
	static BlobMetadata FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string type;
	int64_t snapshot_id;
	int64_t sequence_number;
	vector<int64_t> fields;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
};

} // namespace rest_api_objects
} // namespace duckdb
