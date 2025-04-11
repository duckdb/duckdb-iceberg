#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "catalog_utils.hpp"
#include "url_utils.hpp"

//! fwd declare
struct curl_slist;

namespace duckdb {

enum class IRCAuthorizationType : uint8_t { OAUTH2, SIGV4, INVALID };

struct IcebergAttachOptions {
	string endpoint;
	string warehouse;
	string secret;
	string name;
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
	virtual string GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
	                          curl_slist *extra_headers = nullptr) = 0;

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
