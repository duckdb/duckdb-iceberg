
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
		prefix = yyjson_get_str(prefix_val);
	}
	auto config_val = yyjson_obj_get(obj, "config");
	if (!config_val) {
		return "StorageCredential required property 'config' is missing";
	} else {
		config = parse_object_of_strings(config_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
