
#include "rest_catalog/objects/catalog_config.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

CatalogConfig CatalogConfig::Copy() const {
	CatalogConfig res;
	for (auto &entry : defaults) {
		res.defaults.emplace(entry.first, entry.second);
	}
	for (auto &entry : overrides) {
		res.overrides.emplace(entry.first, entry.second);
	}
	if (endpoints.has_value()) {
		res.endpoints.emplace();
		(*res.endpoints).reserve((*endpoints).size());
		for (auto &item : (*endpoints)) {
			(*res.endpoints).emplace_back(item);
		}
	}
	if (idempotency_key_lifetime.has_value()) {
		res.idempotency_key_lifetime.emplace();
		(*res.idempotency_key_lifetime) = (*idempotency_key_lifetime);
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
		vector<string> endpoints_tmp;
		if (yyjson_is_arr(endpoints_val)) {
			size_t endpoints_tmp_idx, endpoints_tmp_max;
			yyjson_val *endpoints_tmp_item_val;
			yyjson_arr_foreach(endpoints_val, endpoints_tmp_idx, endpoints_tmp_max, endpoints_tmp_item_val) {
				string endpoints_tmp_item;
				if (yyjson_is_str(endpoints_tmp_item_val)) {
					endpoints_tmp_item = yyjson_get_str(endpoints_tmp_item_val);
				} else {
					return StringUtil::Format(
					    "CatalogConfig property 'endpoints_tmp_item' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(endpoints_tmp_item_val));
				}
				endpoints_tmp.emplace_back(std::move(endpoints_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "CatalogConfig property 'endpoints_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(endpoints_val));
		}
		endpoints = std::move(endpoints_tmp);
	}
	auto idempotency_key_lifetime_val = yyjson_obj_get(obj, "idempotency-key-lifetime");
	if (idempotency_key_lifetime_val) {
		string idempotency_key_lifetime_tmp;
		if (yyjson_is_str(idempotency_key_lifetime_val)) {
			idempotency_key_lifetime_tmp = yyjson_get_str(idempotency_key_lifetime_val);
		} else {
			return StringUtil::Format(
			    "CatalogConfig property 'idempotency_key_lifetime_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(idempotency_key_lifetime_val));
		}
		idempotency_key_lifetime = std::move(idempotency_key_lifetime_tmp);
	}
	return "";
}

void CatalogConfig::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: defaults
	yyjson_mut_val *defaults_obj = yyjson_mut_obj(doc);
	for (const auto &it : defaults) {
		auto &key = it.first;
		auto &value = it.second;
		auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
		yyjson_mut_obj_add_strcpy(doc, defaults_obj, key_ptr, value.c_str());
	}
	yyjson_mut_obj_add_val(doc, obj, "defaults", defaults_obj);

	// Serialize: overrides
	yyjson_mut_val *overrides_obj = yyjson_mut_obj(doc);
	for (const auto &it : overrides) {
		auto &key = it.first;
		auto &value = it.second;
		auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
		yyjson_mut_obj_add_strcpy(doc, overrides_obj, key_ptr, value.c_str());
	}
	yyjson_mut_obj_add_val(doc, obj, "overrides", overrides_obj);

	// Serialize: endpoints
	if (endpoints.has_value()) {
		auto &endpoints_value = *endpoints;
		yyjson_mut_val *endpoints_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : endpoints_value) {
			yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
			yyjson_mut_arr_append(endpoints_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "endpoints", endpoints_value_arr);
	}

	// Serialize: idempotency-key-lifetime
	if (idempotency_key_lifetime.has_value()) {
		auto &idempotency_key_lifetime_value = *idempotency_key_lifetime;
		yyjson_mut_obj_add_strcpy(doc, obj, "idempotency-key-lifetime", idempotency_key_lifetime_value.c_str());
	}
}

yyjson_mut_val *CatalogConfig::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
