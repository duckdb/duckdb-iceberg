
#include "rest_catalog/objects/page_token.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

PageToken::PageToken() {
}

PageToken PageToken::FromJSON(JSONValue obj) {
	PageToken res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PageToken PageToken::Copy() const {
	PageToken res;
	res.value = value;
	return res;
}

string PageToken::TryFromJSON(JSONValue obj) {
	string error;
	if (obj.IsNull()) {
		//! do nothing, property is explicitly nullable
	} else if (json_utils::IsString(obj)) {
		value = json_utils::GetString(obj);
	} else {
		return StringUtil::Format("PageToken property 'value' is not of type 'string', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue PageToken::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateString(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
