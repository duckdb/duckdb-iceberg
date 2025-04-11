
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/namespace.hpp"
#include "rest_catalog/objects/view_representation.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewVersion {
public:
	ViewVersion();
	ViewVersion(const ViewVersion &) = delete;
	ViewVersion &operator=(const ViewVersion &) = delete;
	ViewVersion(ViewVersion &&) = default;
	ViewVersion &operator=(ViewVersion &&) = default;

public:
	static ViewVersion FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t version_id;
	int64_t timestamp_ms;
	int64_t schema_id;
	case_insensitive_map_t<string> summary;
	vector<ViewRepresentation> representations;
	Namespace default_namespace;
	string default_catalog;
	bool has_default_catalog = false;
};

} // namespace rest_api_objects
} // namespace duckdb
