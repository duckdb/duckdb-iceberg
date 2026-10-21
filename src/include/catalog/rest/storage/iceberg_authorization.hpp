#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/http_util.hpp"

#include "iceberg_attach.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/api/url_utils.hpp"

namespace duckdb {

struct IcebergAuthorization {
public:
	IcebergAuthorization(AttachedDatabase &db, IcebergAuthorizationType type) : db(db), type(type) {
	}
	virtual ~IcebergAuthorization() {
	}

public:
	static IcebergAuthorizationType TypeFromString(const string &type);

	static void ParseExtraHttpHeaders(const Value &headers_value, unordered_map<string, string> &out_headers);

public:
	virtual unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                         const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                         const string &data = "") = 0;
	//! Headers that authenticate a request to the catalog's remote signing endpoint. The signer is called by
	//! httpfs, outside of Request, so they are handed over instead of being applied to a request here.
	virtual void AddRemoteSignerHeaders(ClientContext &context, case_insensitive_map_t<string> &headers);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IcebergAuthorization to type - IcebergAuthorization type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IcebergAuthorization to type - IcebergAuthorization type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	AttachedDatabase &db;
	IcebergAuthorizationType type;
	unordered_map<string, string> extra_http_headers;
};

} // namespace duckdb
