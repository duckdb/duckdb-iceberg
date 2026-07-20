
#include "rest_catalog/objects/blob_metadata.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

BlobMetadata BlobMetadata::Copy() const {
	BlobMetadata res;
	res.type = type;
	res.snapshot_id = snapshot_id;
	res.sequence_number = sequence_number;
	res.fields.reserve(fields.size());
	for (auto &item : fields) {
		res.fields.emplace_back(item);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
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
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
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
		if (yyjson_is_sint(sequence_number_val)) {
			sequence_number = yyjson_get_sint(sequence_number_val);
		} else if (yyjson_is_uint(sequence_number_val)) {
			sequence_number = yyjson_get_uint(sequence_number_val);
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
			size_t fields_idx, fields_max;
			yyjson_val *fields_item_val;
			yyjson_arr_foreach(fields_val, fields_idx, fields_max, fields_item_val) {
				int32_t fields_item;
				if (yyjson_is_int(fields_item_val)) {
					fields_item = yyjson_get_int(fields_item_val);
				} else {
					return StringUtil::Format(
					    "BlobMetadata property 'fields_item' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(fields_item_val));
				}
				fields.emplace_back(std::move(fields_item));
			}
		} else {
			return StringUtil::Format("BlobMetadata property 'fields' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(fields_val));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		case_insensitive_map_t<string> properties_tmp;
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
				properties_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "BlobMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void BlobMetadata::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: snapshot-id
	yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id);

	// Serialize: sequence-number
	yyjson_mut_obj_add_sint(doc, obj, "sequence-number", sequence_number);

	// Serialize: fields
	yyjson_mut_val *fields_arr = yyjson_mut_arr(doc);
	for (const auto &item : fields) {
		yyjson_mut_val *item_val = yyjson_mut_int(doc, item);
		yyjson_mut_arr_append(fields_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "fields", fields_arr);

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		yyjson_mut_val *properties_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : properties_value) {
			auto &key = it.first;
			auto &value = it.second;
			auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
			yyjson_mut_obj_add_strcpy(doc, properties_value_obj, key_ptr, value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "properties", properties_value_obj);
	}
}

yyjson_mut_val *BlobMetadata::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
