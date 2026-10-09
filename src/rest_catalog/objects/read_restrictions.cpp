
#include "rest_catalog/objects/read_restrictions.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ReadRestrictions::ReadRestrictions() {
}

ReadRestrictions ReadRestrictions::FromJSON(JSONValue obj) {
	ReadRestrictions res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ReadRestrictions ReadRestrictions::Copy() const {
	ReadRestrictions res;
	if (required_column_projections.has_value()) {
		res.required_column_projections.emplace();
		(*res.required_column_projections).reserve((*required_column_projections).size());
		for (auto &item : (*required_column_projections)) {
			(*res.required_column_projections).emplace_back(item.Copy());
		}
	}
	if (required_row_filter != nullptr) {
		res.required_row_filter = required_row_filter ? make_uniq<Predicate>(required_row_filter->Copy()) : nullptr;
	}
	return res;
}

string ReadRestrictions::TryFromJSON(JSONValue obj) {
	string error;
	auto required_column_projections_val = obj.GetMember("required-column-projections");
	if (required_column_projections_val.IsValid()) {
		vector<Action> required_column_projections_tmp;
		if (required_column_projections_val.IsArray()) {
			required_column_projections_val.IterateArray([&](JSONValue required_column_projections_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				Action required_column_projections_tmp_item;
				error = required_column_projections_tmp_item.TryFromJSON(required_column_projections_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				required_column_projections_tmp.emplace_back(std::move(required_column_projections_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ReadRestrictions property 'required_column_projections_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(required_column_projections_val).c_str());
		}
		required_column_projections = std::move(required_column_projections_tmp);
	}
	auto required_row_filter_val = obj.GetMember("required-row-filter");
	if (required_row_filter_val.IsValid()) {
		required_row_filter = make_uniq<Predicate>();
		error = required_row_filter->TryFromJSON(required_row_filter_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void ReadRestrictions::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: required-column-projections
	if (required_column_projections.has_value()) {
		auto &required_column_projections_value = *required_column_projections;
		auto required_column_projections_json = writer.CreateArray();
		for (const auto &required_column_projections_json_item : required_column_projections_value) {
			auto required_column_projections_json_item_json = required_column_projections_json_item.ToJSON(writer);
			required_column_projections_json.Append(required_column_projections_json_item_json);
		}
		obj.Add("required-column-projections", required_column_projections_json);
	}

	// Serialize: required-row-filter
	if (required_row_filter != nullptr) {
		auto required_row_filter_json = required_row_filter->ToJSON(writer);
		obj.Add("required-row-filter", required_row_filter_json);
	}
}

JSONMutableValue ReadRestrictions::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
