#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "iceberg_attach.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/api/url_utils.hpp"

namespace duckdb {

class IcebergHTTPClientLock {
public:
	IcebergHTTPClientLock(mutex &client_lock, unordered_map<uintptr_t, unique_ptr<HTTPClient>> &client_map,
	                      uintptr_t database_id)
	    : guard(client_lock), client(client_map.emplace(database_id, nullptr).first->second) {
	}

	unique_ptr<HTTPClient> &GetClient() {
		return client;
	}

private:
	unique_lock<mutex> guard;
	unique_ptr<HTTPClient> &client;
};

//! Hold the pre-initialized HTTPClient for a given connection
struct IcebergAuthorizationContextState : public ClientContextState {
public:
	IcebergAuthorizationContextState() {
	}

public:
	static IcebergHTTPClientLock GetHTTPClient(AttachedDatabase &db, ClientContext &context);

public:
	//! For this connection, a map of attached database -> http-client
	mutex client_lock;
	unordered_map<uintptr_t, unique_ptr<HTTPClient>> client_map;
};

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
