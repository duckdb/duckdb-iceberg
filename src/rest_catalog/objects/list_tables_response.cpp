
#include "rest_catalog/objects/list_tables_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ListTablesResponse::ListTablesResponse() {
}

ListTablesResponse ListTablesResponse::FromJSON(yyjson_val *obj) {
	ListTablesResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ListTablesResponse ListTablesResponse::Copy() const {
	ListTablesResponse res;
	if (next_page_token.has_value()) {
		res.next_page_token.emplace();
		(*res.next_page_token) = (*next_page_token).Copy();
	}
	if (identifiers.has_value()) {
		res.identifiers.emplace();
		(*res.identifiers).reserve((*identifiers).size());
		for (auto &item : (*identifiers)) {
			(*res.identifiers).emplace_back(item.Copy());
		}
	}
	return res;
}

string ListTablesResponse::TryFromJSON(yyjson_val *obj) {
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
	auto identifiers_val = yyjson_obj_get(obj, "identifiers");
	if (identifiers_val) {
		vector<TableIdentifier> identifiers_tmp;
		if (yyjson_is_arr(identifiers_val)) {
			size_t identifiers_tmp_idx, identifiers_tmp_max;
			yyjson_val *identifiers_tmp_item_val;
			yyjson_arr_foreach(identifiers_val, identifiers_tmp_idx, identifiers_tmp_max, identifiers_tmp_item_val) {
				TableIdentifier identifiers_tmp_item;
				error = identifiers_tmp_item.TryFromJSON(identifiers_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				identifiers_tmp.emplace_back(std::move(identifiers_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "ListTablesResponse property 'identifiers_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(identifiers_val));
		}
		identifiers = std::move(identifiers_tmp);
	}
	return "";
}

void ListTablesResponse::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: next-page-token
	if (next_page_token.has_value()) {
		auto &next_page_token_value = *next_page_token;
		yyjson_mut_val *next_page_token_value_val = next_page_token_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "next-page-token", next_page_token_value_val);
	}

	// Serialize: identifiers
	if (identifiers.has_value()) {
		auto &identifiers_value = *identifiers;
		yyjson_mut_val *identifiers_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : identifiers_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(identifiers_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "identifiers", identifiers_value_arr);
	}
}

yyjson_mut_val *ListTablesResponse::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
