
#include "rest_catalog/objects/list_tables_response.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ListTablesResponse::ListTablesResponse() {
}

ListTablesResponse ListTablesResponse::FromJSON(JSONValue obj) {
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

string ListTablesResponse::TryFromJSON(JSONValue obj) {
	string error;
	auto next_page_token_val = obj.GetMember("next-page-token");
	if (next_page_token_val.IsValid()) {
		if (next_page_token_val.IsNull()) {
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
	auto identifiers_val = obj.GetMember("identifiers");
	if (identifiers_val.IsValid()) {
		vector<TableIdentifier> identifiers_tmp;
		if (identifiers_val.IsArray()) {
			identifiers_val.IterateArray([&](JSONValue identifiers_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				TableIdentifier identifiers_tmp_item;
				error = identifiers_tmp_item.TryFromJSON(identifiers_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				identifiers_tmp.emplace_back(std::move(identifiers_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ListTablesResponse property 'identifiers_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(identifiers_val).c_str());
		}
		identifiers = std::move(identifiers_tmp);
	}
	return "";
}

void ListTablesResponse::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: next-page-token
	if (next_page_token.has_value()) {
		auto &next_page_token_value = *next_page_token;
		auto next_page_token_json = next_page_token_value.ToJSON(writer);
		obj.Add("next-page-token", next_page_token_json);
	}

	// Serialize: identifiers
	if (identifiers.has_value()) {
		auto &identifiers_value = *identifiers;
		auto identifiers_json = writer.CreateArray();
		for (const auto &identifiers_json_item : identifiers_value) {
			auto identifiers_json_item_json = identifiers_json_item.ToJSON(writer);
			identifiers_json.Append(identifiers_json_item_json);
		}
		obj.Add("identifiers", identifiers_json);
	}
}

JSONMutableValue ListTablesResponse::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
