
#include "rest_catalog/objects/sort_order.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SortOrder::SortOrder() {
}

SortOrder SortOrder::FromJSON(JSONValue obj) {
	SortOrder res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SortOrder SortOrder::Copy() const {
	SortOrder res;
	res.order_id = order_id;
	res.fields.reserve(fields.size());
	for (auto &item : fields) {
		res.fields.emplace_back(item.Copy());
	}
	return res;
}

string SortOrder::TryFromJSON(JSONValue obj) {
	string error;
	auto order_id_val = obj.GetMember("order-id");
	if (!order_id_val.IsValid()) {
		return "SortOrder required property 'order-id' is missing";
	} else {
		if (json_utils::IsInteger(order_id_val)) {
			order_id = json_utils::GetSignedInteger(order_id_val);
		} else {
			return StringUtil::Format("SortOrder property 'order_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(order_id_val).c_str());
		}
	}
	auto fields_val = obj.GetMember("fields");
	if (!fields_val.IsValid()) {
		return "SortOrder required property 'fields' is missing";
	} else {
		if (fields_val.IsArray()) {
			fields_val.IterateArray([&](JSONValue fields_item_val) {
				if (!error.empty()) {
					return;
				}
				SortField fields_item;
				error = fields_item.TryFromJSON(fields_item_val);
				if (!error.empty()) {
					return;
				}
				fields.emplace_back(std::move(fields_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("SortOrder property 'fields' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(fields_val).c_str());
		}
	}
	return "";
}

void SortOrder::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: order-id
	auto order_id_json = writer.CreateSignedInteger(order_id);
	obj.Add("order-id", order_id_json);

	// Serialize: fields
	auto fields_json = writer.CreateArray();
	for (const auto &fields_json_item : fields) {
		auto fields_json_item_json = fields_json_item.ToJSON(writer);
		fields_json.Append(fields_json_item_json);
	}
	obj.Add("fields", fields_json);
}

JSONMutableValue SortOrder::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
