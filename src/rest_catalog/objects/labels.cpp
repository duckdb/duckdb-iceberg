
#include "rest_catalog/objects/labels.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Labels::Labels() {
}

Labels Labels::FromJSON(JSONValue obj) {
	Labels res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Labels Labels::Copy() const {
	Labels res;
	if (object_labels.has_value()) {
		res.object_labels.emplace();
		(*res.object_labels) = (*object_labels).Copy();
	}
	if (fields.has_value()) {
		res.fields.emplace();
		(*res.fields).reserve((*fields).size());
		for (auto &item : (*fields)) {
			(*res.fields).emplace_back(item.Copy());
		}
	}
	return res;
}

string Labels::TryFromJSON(JSONValue obj) {
	string error;
	auto object_labels_val = obj.GetMember("object-labels");
	if (object_labels_val.IsValid()) {
		CatalogObjectLabels object_labels_tmp;
		error = object_labels_tmp.TryFromJSON(object_labels_val);
		if (!error.empty()) {
			return error;
		}
		object_labels = std::move(object_labels_tmp);
	}
	auto fields_val = obj.GetMember("fields");
	if (fields_val.IsValid()) {
		vector<FieldLabels> fields_tmp;
		if (fields_val.IsArray()) {
			fields_val.IterateArray([&](JSONValue fields_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				FieldLabels fields_tmp_item;
				error = fields_tmp_item.TryFromJSON(fields_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				fields_tmp.emplace_back(std::move(fields_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("Labels property 'fields_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(fields_val).c_str());
		}
		fields = std::move(fields_tmp);
	}
	return "";
}

void Labels::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: object-labels
	if (object_labels.has_value()) {
		auto &object_labels_value = *object_labels;
		auto object_labels_json = object_labels_value.ToJSON(writer);
		obj.Add("object-labels", object_labels_json);
	}

	// Serialize: fields
	if (fields.has_value()) {
		auto &fields_value = *fields;
		auto fields_json = writer.CreateArray();
		for (const auto &fields_json_item : fields_value) {
			auto fields_json_item_json = fields_json_item.ToJSON(writer);
			fields_json.Append(fields_json_item_json);
		}
		obj.Add("fields", fields_json);
	}
}

JSONMutableValue Labels::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
