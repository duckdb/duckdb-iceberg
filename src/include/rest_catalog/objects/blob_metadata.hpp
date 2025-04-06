
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
	BlobMetadata::BlobMetadata() {
	}

public:
	static BlobMetadata FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
		return "BlobMetadata required property 'type' is missing");
		}
		type = yyjson_get_str(type_val);

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (!snapshot_id_val) {
		return "BlobMetadata required property 'snapshot_id' is missing");
		}
		snapshot_id = yyjson_get_sint(snapshot_id_val);

		auto sequence_number_val = yyjson_obj_get(obj, "sequence_number");
		if (!sequence_number_val) {
		return "BlobMetadata required property 'sequence_number' is missing");
		}
		sequence_number = yyjson_get_sint(sequence_number_val);

		auto fields_val = yyjson_obj_get(obj, "fields");
		if (!fields_val) {
		return "BlobMetadata required property 'fields' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(fields_val, idx, max, val) {

			auto tmp = yyjson_get_sint(val);
			fields.push_back(tmp);
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			properties = parse_object_of_strings(properties_val);
		}
		return string();
	}

public:
public:
	vector<int64_t> fields;
	case_insensitive_map_t<string> properties;
	int64_t sequence_number;
	int64_t snapshot_id;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
