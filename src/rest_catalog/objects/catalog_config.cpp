
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
		if (yyjson_is_obj(defaults_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(defaults_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CatalogConfig property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				defaults.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CatalogConfig property 'defaults' is not of type 'object'";
		}
	}
	auto overrides_val = yyjson_obj_get(obj, "overrides");
	if (!overrides_val) {
		return "CatalogConfig required property 'overrides' is missing";
	} else {
		if (yyjson_is_obj(overrides_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(overrides_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CatalogConfig property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				overrides.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CatalogConfig property 'overrides' is not of type 'object'";
		}
	}
	auto endpoints_val = yyjson_obj_get(obj, "endpoints");
	if (endpoints_val) {
		has_endpoints = true;
		if (yyjson_is_arr(endpoints_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(endpoints_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CatalogConfig property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				endpoints.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("CatalogConfig property 'endpoints' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(endpoints_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
