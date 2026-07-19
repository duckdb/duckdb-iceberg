
#include "rest_catalog/objects/load_table_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

LoadTableResult::LoadTableResult() {
}

LoadTableResult LoadTableResult::FromJSON(yyjson_val *obj) {
	LoadTableResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LoadTableResult LoadTableResult::Copy() const {
	LoadTableResult res;
	res.metadata = metadata.Copy();
	if (metadata_location.has_value()) {
		res.metadata_location.emplace();
		(*res.metadata_location) = (*metadata_location);
	}
	if (config.has_value()) {
		res.config.emplace();
		for (auto &entry : (*config)) {
			(*res.config).emplace(entry.first, entry.second);
		}
	}
	if (storage_credentials.has_value()) {
		res.storage_credentials.emplace();
		(*res.storage_credentials).reserve((*storage_credentials).size());
		for (auto &item : (*storage_credentials)) {
			(*res.storage_credentials).emplace_back(item.Copy());
		}
	}
	return res;
}

string LoadTableResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (!metadata_val) {
		return "LoadTableResult required property 'metadata' is missing";
	} else {
		error = metadata.TryFromJSON(metadata_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
	if (metadata_location_val) {
		if (yyjson_is_null(metadata_location_val)) {
			//! do nothing, property is explicitly nullable
		} else {
			string metadata_location_tmp;
			if (yyjson_is_str(metadata_location_val)) {
				metadata_location_tmp = yyjson_get_str(metadata_location_val);
			} else {
				return StringUtil::Format(
				    "LoadTableResult property 'metadata_location_tmp' is not of type 'string', found '%s' instead",
				    yyjson_get_type_desc(metadata_location_val));
			}
			metadata_location = std::move(metadata_location_tmp);
		}
	}
	auto config_val = yyjson_obj_get(obj, "config");
	if (config_val) {
		case_insensitive_map_t<string> config_tmp;
		if (yyjson_is_obj(config_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(config_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "LoadTableResult property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				config_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "LoadTableResult property 'config_tmp' is not of type 'object'";
		}
		config = std::move(config_tmp);
	}
	auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
	if (storage_credentials_val) {
		vector<StorageCredential> storage_credentials_tmp;
		if (yyjson_is_arr(storage_credentials_val)) {
			size_t storage_credentials_tmp_idx, storage_credentials_tmp_max;
			yyjson_val *storage_credentials_tmp_item_val;
			yyjson_arr_foreach(storage_credentials_val, storage_credentials_tmp_idx, storage_credentials_tmp_max,
			                   storage_credentials_tmp_item_val) {
				StorageCredential storage_credentials_tmp_item;
				error = storage_credentials_tmp_item.TryFromJSON(storage_credentials_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				storage_credentials_tmp.emplace_back(std::move(storage_credentials_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "LoadTableResult property 'storage_credentials_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(storage_credentials_val));
		}
		storage_credentials = std::move(storage_credentials_tmp);
	}
	return "";
}

void LoadTableResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: metadata
	yyjson_mut_val *metadata_val = metadata.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "metadata", metadata_val);

	// Serialize: metadata-location
	if (metadata_location.has_value()) {
		auto &metadata_location_value = *metadata_location;
		yyjson_mut_obj_add_strcpy(doc, obj, "metadata-location", metadata_location_value.c_str());
	}

	// Serialize: config
	if (config.has_value()) {
		auto &config_value = *config;
		yyjson_mut_val *config_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : config_value) {
			auto &key = it.first;
			auto &value = it.second;
			auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
			yyjson_mut_obj_add_strcpy(doc, config_value_obj, key_ptr, value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "config", config_value_obj);
	}

	// Serialize: storage-credentials
	if (storage_credentials.has_value()) {
		auto &storage_credentials_value = *storage_credentials;
		yyjson_mut_val *storage_credentials_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : storage_credentials_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(storage_credentials_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "storage-credentials", storage_credentials_value_arr);
	}
}

yyjson_mut_val *LoadTableResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
