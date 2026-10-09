
#include "rest_catalog/objects/term_reference.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

TermReference::TermReference() {
}

TermReference TermReference::FromJSON(JSONValue obj) {
	TermReference res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

TermReference TermReference::Copy() const {
	TermReference res;
	res.value = value;
	return res;
}

string TermReference::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsString(obj)) {
		value = json_utils::GetString(obj);
	} else {
		return StringUtil::Format("TermReference property 'value' is not of type 'string', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue TermReference::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateString(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
