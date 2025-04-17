
#include "rest_catalog/objects/storage_credential.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

StorageCredential::StorageCredential() {
}

StorageCredential StorageCredential::FromJSON(yyjson_val *obj) {
	StorageCredential res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string StorageCredential::TryFromJSON(yyjson_val *obj) {
	string error;
	auto prefix_val = yyjson_obj_get(obj, "prefix");
	if (!prefix_val) {
		return "StorageCredential required property 'prefix' is missing";
	} else {
		if (yyjson_is_str(prefix_val)) {
			prefix = yyjson_get_str(prefix_val);
		} else {
			return StringUtil::Format("StorageCredential property 'prefix' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(prefix_val));
		}
	}
	auto config_val = yyjson_obj_get(obj, "config");
	if (!config_val) {
		return "StorageCredential required property 'config' is missing";
	} else {
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
					    "StorageCredential property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				config.emplace(key_str, std::move(tmp));
			}
		} else {
			return "StorageCredential property 'config' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
