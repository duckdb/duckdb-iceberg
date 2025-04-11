
#include "rest_catalog/objects/namespace.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Namespace::Namespace() {
}

Namespace Namespace::FromJSON(yyjson_val *obj) {
	Namespace res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Namespace::TryFromJSON(yyjson_val *obj) {
	string error;
	size_t idx, max;
	yyjson_val *val;
	yyjson_arr_foreach(obj, idx, max, val) {
		auto tmp = yyjson_get_str(val);
		value.emplace_back(std::move(tmp));
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
