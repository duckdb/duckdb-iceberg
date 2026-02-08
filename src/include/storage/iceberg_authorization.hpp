#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "catalog_utils.hpp"
#include "url_utils.hpp"
#include "duckdb/common/http_util.hpp"

namespace duckdb {

enum class IcebergEndpointType : uint8_t { AWS_S3TABLES, AWS_GLUE, INVALID };

enum class IcebergAuthorizationType : uint8_t { OAUTH2, SIGV4, NONE, INVALID };

enum class IRCAccessDelegationMode : uint8_t { NONE, VENDED_CREDENTIALS };

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
	IRCAccessDelegationMode access_mode = IRCAccessDelegationMode::VENDED_CREDENTIALS;
	IcebergAuthorizationType authorization_type = IcebergAuthorizationType::INVALID;
	unordered_map<string, Value> options;
	// max staleness for cached table metadata in minutes (optional - if not set, always request fresh metadata)
	optional_idx max_table_staleness_micros;
};

struct IcebergAuthorization {
public:
	IcebergAuthorization(IcebergAuthorizationType type) : type(type) {
	}
	virtual ~IcebergAuthorization() {
	}

public:
	static IcebergAuthorizationType TypeFromString(const string &type);

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
	IcebergAuthorizationType type;
	unique_ptr<HTTPClient> client;
};

} // namespace duckdb
