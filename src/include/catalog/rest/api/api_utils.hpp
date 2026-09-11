//===----------------------------------------------------------------------===//
//                         DuckDB
//
// api_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_util.hpp"

#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/storage/aws.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

namespace duckdb {

class APIUtils {
public:
	static unique_ptr<HTTPResponse> Request(RequestType request_type, optional_ptr<AttachedDatabase> db,
	                                        ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                                        HTTPHeaders &headers, const string &data);
};

} // namespace duckdb
