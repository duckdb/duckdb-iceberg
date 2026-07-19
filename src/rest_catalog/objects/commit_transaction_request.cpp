
#include "rest_catalog/objects/commit_transaction_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

CommitTransactionRequest CommitTransactionRequest::Copy() const {
	CommitTransactionRequest res;
	res.table_changes.reserve(table_changes.size());
	for (auto &item : table_changes) {
		res.table_changes.emplace_back(item.Copy());
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
			size_t table_changes_idx, table_changes_max;
			yyjson_val *table_changes_item_val;
			yyjson_arr_foreach(table_changes_val, table_changes_idx, table_changes_max, table_changes_item_val) {
				CommitTableRequest table_changes_item;
				error = table_changes_item.TryFromJSON(table_changes_item_val);
				if (!error.empty()) {
					return error;
				}
				table_changes.emplace_back(std::move(table_changes_item));
			}
		} else {
			return StringUtil::Format(
			    "CommitTransactionRequest property 'table_changes' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(table_changes_val));
		}
	}
	return "";
}

void CommitTransactionRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: table-changes
	yyjson_mut_val *table_changes_arr = yyjson_mut_arr(doc);
	for (const auto &item : table_changes) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(table_changes_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "table-changes", table_changes_arr);
}

yyjson_mut_val *CommitTransactionRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
