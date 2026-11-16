
#include "rest_catalog/objects/list_namespaces_response.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ListNamespacesResponse::ListNamespacesResponse() {
}

ListNamespacesResponse ListNamespacesResponse::FromJSON(JSONValue obj) {
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

string ListNamespacesResponse::TryFromJSON(JSONValue obj) {
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
	auto namespaces_val = obj.GetMember("namespaces");
	if (namespaces_val.IsValid()) {
		vector<Namespace> namespaces_tmp;
		if (namespaces_val.IsArray()) {
			namespaces_val.IterateArray([&](JSONValue namespaces_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				Namespace namespaces_tmp_item;
				error = namespaces_tmp_item.TryFromJSON(namespaces_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				namespaces_tmp.emplace_back(std::move(namespaces_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ListNamespacesResponse property 'namespaces_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(namespaces_val).c_str());
		}
		namespaces = std::move(namespaces_tmp);
	}
	return "";
}

void ListNamespacesResponse::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: next-page-token
	if (next_page_token.has_value()) {
		auto &next_page_token_value = *next_page_token;
		auto next_page_token_json = next_page_token_value.ToJSON(writer);
		obj.Add("next-page-token", next_page_token_json);
	}

	// Serialize: namespaces
	if (namespaces.has_value()) {
		auto &namespaces_value = *namespaces;
		auto namespaces_json = writer.CreateArray();
		for (const auto &namespaces_json_item : namespaces_value) {
			auto namespaces_json_item_json = namespaces_json_item.ToJSON(writer);
			namespaces_json.Append(namespaces_json_item_json);
		}
		obj.Add("namespaces", namespaces_json);
	}
}

JSONMutableValue ListNamespacesResponse::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
