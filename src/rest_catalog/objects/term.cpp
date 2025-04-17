
#include "rest_catalog/objects/term.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Term::Term() {
}

Term Term::FromJSON(yyjson_val *obj) {
	Term res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Term::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = reference.TryFromJSON(obj);
		if (error.empty()) {
			has_reference = true;
			break;
		}
		error = transform_term.TryFromJSON(obj);
		if (error.empty()) {
			has_transform_term = true;
			break;
		}
		return "Term failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
