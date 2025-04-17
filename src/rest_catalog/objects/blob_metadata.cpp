
#include "rest_catalog/objects/blob_metadata.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

BlobMetadata::BlobMetadata() {
}

BlobMetadata BlobMetadata::FromJSON(yyjson_val *obj) {
	BlobMetadata res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string BlobMetadata::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "BlobMetadata required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("BlobMetadata property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "BlobMetadata required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_int(snapshot_id_val)) {
			snapshot_id = yyjson_get_int(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "BlobMetadata property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
	if (!sequence_number_val) {
		return "BlobMetadata required property 'sequence-number' is missing";
	} else {
		if (yyjson_is_int(sequence_number_val)) {
			sequence_number = yyjson_get_int(sequence_number_val);
		} else {
			return StringUtil::Format(
			    "BlobMetadata property 'sequence_number' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(sequence_number_val));
		}
	}
	auto fields_val = yyjson_obj_get(obj, "fields");
	if (!fields_val) {
		return "BlobMetadata required property 'fields' is missing";
	} else {
		if (yyjson_is_arr(fields_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				int64_t tmp;
				if (yyjson_is_int(val)) {
					tmp = yyjson_get_int(val);
				} else {
					return StringUtil::Format(
					    "BlobMetadata property 'tmp' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				fields.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("BlobMetadata property 'fields' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(fields_val));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("BlobMetadata property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				properties.emplace(key_str, std::move(tmp));
			}
		} else {
			return "BlobMetadata property 'properties' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
