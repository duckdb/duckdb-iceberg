
#include "rest_catalog/objects/catalog_config.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CatalogConfig::CatalogConfig() {
}

CatalogConfig CatalogConfig::FromJSON(yyjson_val *obj) {
	CatalogConfig res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CatalogConfig::TryFromJSON(yyjson_val *obj) {
	string error;
	auto defaults_val = yyjson_obj_get(obj, "defaults");
	if (!defaults_val) {
		return "CatalogConfig required property 'defaults' is missing";
	} else {
		defaults = parse_object_of_strings(defaults_val);
	}
	auto overrides_val = yyjson_obj_get(obj, "overrides");
	if (!overrides_val) {
		return "CatalogConfig required property 'overrides' is missing";
	} else {
		overrides = parse_object_of_strings(overrides_val);
	}
	auto endpoints_val = yyjson_obj_get(obj, "endpoints");
	if (endpoints_val) {
		has_endpoints = true;
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(endpoints_val, idx, max, val) {
			auto tmp = yyjson_get_str(val);
			endpoints.emplace_back(std::move(tmp));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
