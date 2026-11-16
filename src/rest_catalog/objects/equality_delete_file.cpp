
#include "rest_catalog/objects/equality_delete_file.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

EqualityDeleteFile::EqualityDeleteFile() {
}

EqualityDeleteFile EqualityDeleteFile::FromJSON(JSONValue obj) {
	EqualityDeleteFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

EqualityDeleteFile EqualityDeleteFile::Copy() const {
	EqualityDeleteFile res;
	res.content_file = content_file.Copy();
	if (equality_ids.has_value()) {
		res.equality_ids.emplace();
		(*res.equality_ids).reserve((*equality_ids).size());
		for (auto &item : (*equality_ids)) {
			(*res.equality_ids).emplace_back(item);
		}
	}
	return res;
}

string EqualityDeleteFile::TryFromJSON(JSONValue obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_refinement_val = obj.GetMember("content");
	if (content_refinement_val.IsValid()) {
		string content_refinement;
		if (json_utils::IsString(content_refinement_val)) {
			content_refinement = json_utils::GetString(content_refinement_val);
		} else {
			return StringUtil::Format(
			    "EqualityDeleteFile property 'content_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(content_refinement_val).c_str());
		}
		if (!content_refinement_val.IsNull() && content_refinement != "equality-deletes") {
			return "EqualityDeleteFile property 'content_refinement' does not match its required const value";
		}
	} else {
		return "EqualityDeleteFile required property 'content' is missing";
	}
	auto equality_ids_val = obj.GetMember("equality-ids");
	if (equality_ids_val.IsValid()) {
		vector<int32_t> equality_ids_tmp;
		if (equality_ids_val.IsArray()) {
			equality_ids_val.IterateArray([&](JSONValue equality_ids_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				int32_t equality_ids_tmp_item;
				if (json_utils::IsInteger(equality_ids_tmp_item_val)) {
					equality_ids_tmp_item = json_utils::GetSignedInteger(equality_ids_tmp_item_val);
				} else {
					error = StringUtil::Format("EqualityDeleteFile property 'equality_ids_tmp_item' is not of type "
					                           "'integer', found %s instead",
					                           json_utils::GetTypeDescription(equality_ids_tmp_item_val).c_str());
					return;
				}
				equality_ids_tmp.emplace_back(std::move(equality_ids_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "EqualityDeleteFile property 'equality_ids_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(equality_ids_val).c_str());
		}
		equality_ids = std::move(equality_ids_tmp);
	}
	return "";
}

void EqualityDeleteFile::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: ContentFile
	content_file.PopulateJSON(writer, obj);

	// Serialize: equality-ids
	if (equality_ids.has_value()) {
		auto &equality_ids_value = *equality_ids;
		auto equality_ids_json = writer.CreateArray();
		for (const auto &equality_ids_json_item : equality_ids_value) {
			auto equality_ids_json_item_json = writer.CreateSignedInteger(equality_ids_json_item);
			equality_ids_json.Append(equality_ids_json_item_json);
		}
		obj.Add("equality-ids", equality_ids_json);
	}
}

JSONMutableValue EqualityDeleteFile::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
