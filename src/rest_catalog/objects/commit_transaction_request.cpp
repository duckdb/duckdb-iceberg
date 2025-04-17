
#include "rest_catalog/objects/commit_transaction_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CommitTransactionRequest::CommitTransactionRequest() {
}

CommitTransactionRequest CommitTransactionRequest::FromJSON(yyjson_val *obj) {
	CommitTransactionRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CommitTransactionRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto table_changes_val = yyjson_obj_get(obj, "table-changes");
	if (!table_changes_val) {
		return "CommitTransactionRequest required property 'table-changes' is missing";
	} else {
		if (yyjson_is_arr(table_changes_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(table_changes_val, idx, max, val) {
				CommitTableRequest tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				table_changes.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "CommitTransactionRequest property 'table_changes' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(table_changes_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
