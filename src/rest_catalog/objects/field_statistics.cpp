
#include "rest_catalog/objects/field_statistics.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FieldStatistics::FieldStatistics() {
}

FieldStatistics FieldStatistics::FromJSON(JSONValue obj) {
	FieldStatistics res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FieldStatistics FieldStatistics::Copy() const {
	FieldStatistics res;
	if (avg_value_size_in_bytes.has_value()) {
		res.avg_value_size_in_bytes.emplace();
		(*res.avg_value_size_in_bytes) = (*avg_value_size_in_bytes);
	}
	return res;
}

string FieldStatistics::TryFromJSON(JSONValue obj) {
	string error;
	auto avg_value_size_in_bytes_val = obj.GetMember("avg-value-size-in-bytes");
	if (avg_value_size_in_bytes_val.IsValid()) {
		int32_t avg_value_size_in_bytes_tmp;
		if (json_utils::IsInteger(avg_value_size_in_bytes_val)) {
			avg_value_size_in_bytes_tmp = json_utils::GetSignedInteger(avg_value_size_in_bytes_val);
		} else {
			return StringUtil::Format(
			    "FieldStatistics property 'avg_value_size_in_bytes_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(avg_value_size_in_bytes_val).c_str());
		}
		avg_value_size_in_bytes = std::move(avg_value_size_in_bytes_tmp);
	}
	return "";
}

void FieldStatistics::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: avg-value-size-in-bytes
	if (avg_value_size_in_bytes.has_value()) {
		auto &avg_value_size_in_bytes_value = *avg_value_size_in_bytes;
		auto avg_value_size_in_bytes_json = writer.CreateSignedInteger(avg_value_size_in_bytes_value);
		obj.Add("avg-value-size-in-bytes", avg_value_size_in_bytes_json);
	}
}

JSONMutableValue FieldStatistics::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
