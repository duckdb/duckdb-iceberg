
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
		type = yyjson_get_str(type_val);
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "BlobMetadata required property 'snapshot-id' is missing";
	} else {
		snapshot_id = yyjson_get_sint(snapshot_id_val);
	}
	auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
	if (!sequence_number_val) {
		return "BlobMetadata required property 'sequence-number' is missing";
	} else {
		sequence_number = yyjson_get_sint(sequence_number_val);
	}
	auto fields_val = yyjson_obj_get(obj, "fields");
	if (!fields_val) {
		return "BlobMetadata required property 'fields' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(fields_val, idx, max, val) {
			auto tmp = yyjson_get_sint(val);
			fields.emplace_back(std::move(tmp));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		properties = parse_object_of_strings(properties_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
