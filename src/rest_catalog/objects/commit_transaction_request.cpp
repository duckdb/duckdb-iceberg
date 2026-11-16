
#include "rest_catalog/objects/commit_transaction_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CommitTransactionRequest::CommitTransactionRequest() {
}

CommitTransactionRequest CommitTransactionRequest::FromJSON(JSONValue obj) {
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

string CommitTransactionRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto table_changes_val = obj.GetMember("table-changes");
	if (!table_changes_val.IsValid()) {
		return "CommitTransactionRequest required property 'table-changes' is missing";
	} else {
		if (table_changes_val.IsArray()) {
			table_changes_val.IterateArray([&](JSONValue table_changes_item_val) {
				if (!error.empty()) {
					return;
				}
				CommitTableRequest table_changes_item;
				error = table_changes_item.TryFromJSON(table_changes_item_val);
				if (!error.empty()) {
					return;
				}
				table_changes.emplace_back(std::move(table_changes_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "CommitTransactionRequest property 'table_changes' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(table_changes_val).c_str());
		}
	}
	return "";
}

void CommitTransactionRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: table-changes
	auto table_changes_json = writer.CreateArray();
	for (const auto &table_changes_json_item : table_changes) {
		auto table_changes_json_item_json = table_changes_json_item.ToJSON(writer);
		table_changes_json.Append(table_changes_json_item_json);
	}
	obj.Add("table-changes", table_changes_json);
}

JSONMutableValue CommitTransactionRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
