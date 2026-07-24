
#include "rest_catalog/objects/null_type_value.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

NullTypeValue::NullTypeValue() {
}

NullTypeValue NullTypeValue::FromJSON(yyjson_val *obj) {
	NullTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

NullTypeValue NullTypeValue::Copy() const {
	NullTypeValue res;
	res.value = value;
	return res;
}

string NullTypeValue::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_null(obj)) {
		//! do nothing, property is explicitly nullable
	} else if (yyjson_is_null(obj)) {
		value = (void *)(obj);
	} else {
		return StringUtil::Format("NullTypeValue property 'value' is not of type 'None', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return "";
}

yyjson_mut_val *NullTypeValue::ToJSON(yyjson_mut_doc *doc) const {
	throw InternalException("Unsupported primitive serialization");
}

} // namespace rest_api_objects
} // namespace duckdb
