
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/sqlview_representation.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewRepresentation {
public:
	ViewRepresentation::ViewRepresentation() {
	}

public:
	static ViewRepresentation FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		do {
			error = base_sqlview_representation.TryFromJSON(obj);
			if (error.empty()) {
				has_sqlview_representation = true;
				break;
			}
			return "ViewRepresentation failed to parse, none of the oneOf candidates matched";
		} while (false);

		return string();
	}

public:
	SQLViewRepresentation sqlview_representation;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
