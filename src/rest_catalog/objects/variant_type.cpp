
#include "rest_catalog/objects/variant_type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

VariantType::VariantType() {
}

VariantType VariantType::FromJSON(JSONValue obj) {
	VariantType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

VariantType VariantType::Copy() const {
	VariantType res;
	res.value = value;
	return res;
}

string VariantType::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsString(obj)) {
		value = json_utils::GetString(obj);
	} else {
		return StringUtil::Format("VariantType property 'value' is not of type 'string', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	if (!obj.IsNull() && value != "variant") {
		return "VariantType property 'value' does not match its required const value";
	}
	return "";
}

JSONMutableValue VariantType::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateString(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
