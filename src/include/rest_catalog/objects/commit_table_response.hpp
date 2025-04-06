
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTableResponse {
public:
	CommitTableResponse() {
	}

public:
	static CommitTableResponse FromJSON(yyjson_val *obj) {
		CommitTableResponse res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto metadata_location_val = yyjson_obj_get(obj, "metadata_location");
		if (!metadata_location_val) {
			return "CommitTableResponse required property 'metadata_location' is missing";
		} else {
			metadata_location = yyjson_get_str(metadata_location_val);
		}
		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (!metadata_val) {
			return "CommitTableResponse required property 'metadata' is missing";
		} else {
			error = metadata.TryFromJSON(metadata_val);
			if (!error.empty()) {
				return error;
			}
		}
		return string();
	}

public:
	string metadata_location;
	TableMetadata metadata;
};

} // namespace rest_api_objects
} // namespace duckdb
