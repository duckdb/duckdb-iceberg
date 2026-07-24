
#include "rest_catalog/objects/boolean_expression.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

BooleanExpression::BooleanExpression() {
}

BooleanExpression BooleanExpression::FromJSON(yyjson_val *obj) {
	BooleanExpression res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

BooleanExpression BooleanExpression::Copy() const {
	BooleanExpression res;
	res.value = value;
	return res;
}

string BooleanExpression::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_bool(obj)) {
		value = yyjson_get_bool(obj);
	} else {
		return StringUtil::Format("BooleanExpression property 'value' is not of type 'boolean', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return "";
}

yyjson_mut_val *BooleanExpression::ToJSON(yyjson_mut_doc *doc) const {
	return yyjson_mut_bool(doc, value);
}

} // namespace rest_api_objects
} // namespace duckdb
