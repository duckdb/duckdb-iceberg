#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
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
	//! Test hook ('iceberg_test_force_token_expiry'), so a test can observe a refresh without waiting it out
	static bool ForceTokenExpiry(ClientContext &context);
	//! Re-create a secret from the 'refresh_info' its provider stored (duckdb-aws and duckdb-azure with
	//! REFRESH 'auto'), which re-runs the provider's credential chain. Returns false if there is no 'refresh_info'.
	static bool ReplaySecretRefresh(ClientContext &context, const SecretEntry &secret_entry);

public:
	virtual unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context,
	                                         const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
	                                         const string &data = "") = 0;

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
