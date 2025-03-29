#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewRepresentation {
public:
	static ViewRepresentation FromJSON(yyjson_val *obj) {
		ViewRepresentation result;
		if (yyjson_is_obj(obj)) {
			auto type_val = yyjson_obj_get(obj, "type");
			if (type_val && strcmp(yyjson_get_str(type_val), "sqlviewrepresentation") == 0) {
				result.sqlview_representation = SQLViewRepresentation::FromJSON(obj);
				result.has_sqlview_representation = true;
			}
		}
		return result;
	}

public:
	SQLViewRepresentation sqlview_representation;
	bool has_sqlview_representation = false;
};
} // namespace rest_api_objects
} // namespace duckdb
