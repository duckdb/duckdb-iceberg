
#include "rest_catalog/objects/list_namespaces_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ListNamespacesResponse::ListNamespacesResponse() {
}

ListNamespacesResponse ListNamespacesResponse::FromJSON(yyjson_val *obj) {
	ListNamespacesResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ListNamespacesResponse ListNamespacesResponse::Copy() const {
	ListNamespacesResponse res;
	if (next_page_token.has_value()) {
		res.next_page_token.emplace();
		(*res.next_page_token) = (*next_page_token).Copy();
	}
	if (namespaces.has_value()) {
		res.namespaces.emplace();
		(*res.namespaces).reserve((*namespaces).size());
		for (auto &item : (*namespaces)) {
			(*res.namespaces).emplace_back(item.Copy());
		}
	}
	return res;
}

string ListNamespacesResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto next_page_token_val = yyjson_obj_get(obj, "next-page-token");
	if (next_page_token_val) {
		if (yyjson_is_null(next_page_token_val)) {
			//! do nothing, property is explicitly nullable
		} else {
			PageToken next_page_token_tmp;
			error = next_page_token_tmp.TryFromJSON(next_page_token_val);
			if (!error.empty()) {
				return error;
			}
			next_page_token = std::move(next_page_token_tmp);
		}
	}
	auto namespaces_val = yyjson_obj_get(obj, "namespaces");
	if (namespaces_val) {
		vector<Namespace> namespaces_tmp;
		if (yyjson_is_arr(namespaces_val)) {
			size_t namespaces_tmp_idx, namespaces_tmp_max;
			yyjson_val *namespaces_tmp_item_val;
			yyjson_arr_foreach(namespaces_val, namespaces_tmp_idx, namespaces_tmp_max, namespaces_tmp_item_val) {
				Namespace namespaces_tmp_item;
				error = namespaces_tmp_item.TryFromJSON(namespaces_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				namespaces_tmp.emplace_back(std::move(namespaces_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "ListNamespacesResponse property 'namespaces_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(namespaces_val));
		}
		namespaces = std::move(namespaces_tmp);
	}
	return "";
}

void ListNamespacesResponse::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: next-page-token
	if (next_page_token.has_value()) {
		auto &next_page_token_value = *next_page_token;
		yyjson_mut_val *next_page_token_value_val = next_page_token_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "next-page-token", next_page_token_value_val);
	}

	// Serialize: namespaces
	if (namespaces.has_value()) {
		auto &namespaces_value = *namespaces;
		yyjson_mut_val *namespaces_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : namespaces_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(namespaces_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "namespaces", namespaces_value_arr);
	}
}

yyjson_mut_val *ListNamespacesResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
