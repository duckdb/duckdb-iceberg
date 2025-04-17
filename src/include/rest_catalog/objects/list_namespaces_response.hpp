
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/namespace.hpp"
#include "rest_catalog/objects/page_token.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ListNamespacesResponse {
public:
	ListNamespacesResponse();
	ListNamespacesResponse(const ListNamespacesResponse &) = delete;
	ListNamespacesResponse &operator=(const ListNamespacesResponse &) = delete;
	ListNamespacesResponse(ListNamespacesResponse &&) = default;
	ListNamespacesResponse &operator=(ListNamespacesResponse &&) = default;

public:
	static ListNamespacesResponse FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	PageToken next_page_token;
	bool has_next_page_token = false;
	vector<Namespace> namespaces;
	bool has_namespaces = false;
};

} // namespace rest_api_objects
} // namespace duckdb
