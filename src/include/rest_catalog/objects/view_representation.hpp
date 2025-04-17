
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
	ViewRepresentation();
	ViewRepresentation(const ViewRepresentation &) = delete;
	ViewRepresentation &operator=(const ViewRepresentation &) = delete;
	ViewRepresentation(ViewRepresentation &&) = default;
	ViewRepresentation &operator=(ViewRepresentation &&) = default;

public:
	static ViewRepresentation FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	SQLViewRepresentation sqlview_representation;
	bool has_sqlview_representation = false;
};

} // namespace rest_api_objects
} // namespace duckdb
