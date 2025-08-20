#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "catalog_utils.hpp"
#include "url_utils.hpp"
#include "duckdb/common/http_util.hpp"

namespace duckdb {

enum class IcebergEndpointType : uint8_t { AWS_S3TABLES, AWS_GLUE, INVALID };

enum class IRCAuthorizationType : uint8_t { OAUTH2, SIGV4, NONE, INVALID };

struct IcebergAttachOptions {
	string endpoint;
	string warehouse;
	string secret;
	string name;
	// some catalogs do not yet support stage create
	bool supports_stage_create = true;
	// if the catalog allows manual cleaning up of storage files.
	bool allows_deletes = true;
	bool support_nested_namespaces = false;
	// in rest api spec, purge requested defaults to false.
	bool purge_requested = false;

	IRCAuthorizationType authorization_type = IRCAuthorizationType::INVALID;
	unordered_map<string, Value> options;
};

struct IRCAuthorization {
public:
	IRCAuthorization(IRCAuthorizationType type) : type(type) {
	}
	virtual ~IRCAuthorization() {
	}

public:
	static IRCAuthorizationType TypeFromString(const string &type);

public:
	virtual unique_ptr<HTTPResponse> GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) = 0;
	virtual unique_ptr<HTTPResponse> DeleteRequest(ClientContext &context,
	                                               const IRCEndpointBuilder &endpoint_builder) = 0;
	virtual unique_ptr<HTTPResponse> PostRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                                             const string &body) = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IRCAuthorization to type - IRCAuthorization type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IRCAuthorization to type - IRCAuthorization type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	IRCAuthorizationType type;
};

} // namespace duckdb
