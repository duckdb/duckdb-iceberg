
#include "rest_catalog/objects/list_tables_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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

string ListTablesResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto next_page_token_val = yyjson_obj_get(obj, "next-page-token");
	if (next_page_token_val) {
		has_next_page_token = true;
		error = next_page_token.TryFromJSON(next_page_token_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto identifiers_val = yyjson_obj_get(obj, "identifiers");
	if (identifiers_val) {
		has_identifiers = true;
		if (yyjson_is_arr(identifiers_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(identifiers_val, idx, max, val) {
				TableIdentifier tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				identifiers.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "ListTablesResponse property 'identifiers' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(identifiers_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
