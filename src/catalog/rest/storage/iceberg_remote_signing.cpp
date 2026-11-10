#include "catalog/rest/storage/iceberg_remote_signing.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

#include <algorithm>

namespace duckdb {

bool IcebergRemoteSigning::IsEnabled(const case_insensitive_map_t<string> &config) {
	auto entry = config.find(ENABLED_KEY);
	return entry != config.end() && StringUtil::CIEquals(entry->second, "true");
}

//! The value of 'key', or of its deprecated alias when the catalog only sends that
static string GetConfigValue(const case_insensitive_map_t<string> &config, const char *key,
                             const char *deprecated_key) {
	for (auto candidate : {key, deprecated_key}) {
		auto entry = config.find(candidate);
		if (entry != config.end() && !entry->second.empty()) {
			return entry->second;
		}
	}
	return string();
}

//! Mirrors Iceberg's RESTUtil.resolveEndpoint: an absolute endpoint replaces the uri instead of extending it
static string ResolveSignerURL(const string &uri, const string &endpoint) {
	auto lower_endpoint = StringUtil::Lower(endpoint);
	if (StringUtil::StartsWith(lower_endpoint, "http://") || StringUtil::StartsWith(lower_endpoint, "https://")) {
		return endpoint;
	}
	auto base = uri;
	StringUtil::RTrim(base, "/");
	idx_t path_start = 0;
	while (path_start < endpoint.size() && endpoint[path_start] == '/') {
		path_start++;
	}
	return AddHttpHostIfMissing(base + "/" + endpoint.substr(path_start));
}

static Value StringMapToValue(const case_insensitive_map_t<string> &input) {
	//! Sorted, so the secret does not change between loads of the same table
	map<string, string> sorted(input.begin(), input.end());
	vector<Value> keys;
	vector<Value> values;
	for (auto &entry : sorted) {
		keys.emplace_back(entry.first);
		values.emplace_back(entry.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

string IcebergRemoteSigning::GetSignerURL(const IcebergCatalog &catalog, const vector<string> &namespace_items,
                                          const string &table_name, const case_insensitive_map_t<string> &table_config,
                                          bool has_remote_signing_config) {
	//! The deprecated properties are only consulted when the catalog does not send a 'remote-signing-config'
	if (!has_remote_signing_config) {
		auto signer_uri = GetConfigValue(table_config, SIGNER_URI_KEY, DEPRECATED_SIGNER_URI_KEY);
		auto signer_endpoint = GetConfigValue(table_config, SIGNER_ENDPOINT_KEY, DEPRECATED_SIGNER_ENDPOINT_KEY);
		if (!signer_uri.empty() || !signer_endpoint.empty()) {
			//! Catalogs that set these predate the table scoped endpoint and expect the former default
			return ResolveSignerURL(signer_uri.empty() ? catalog.base_uri : signer_uri,
			                        signer_endpoint.empty() ? DEPRECATED_DEFAULT_SIGNER_ENDPOINT : signer_endpoint);
		}
	}
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(table_name));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("sign"));
	return AddHttpHostIfMissing(url_builder.GetURLEncoded());
}

void IcebergRemoteSigning::AddSecretOptions(ClientContext &context, IcebergCatalog &catalog,
                                            const vector<string> &namespace_items, const string &table_name,
                                            const string &log_table_name,
                                            const case_insensitive_map_t<string> &table_config,
                                            optional_ptr<const rest_api_objects::RemoteSigningConfig> signing_config,
                                            case_insensitive_map_t<Value> &options) {
	//! The signer signs requests instead of local credentials, httpfs does not accept a secret with both
	for (auto key : {"key_id", "secret", "session_token", "expires_at"}) {
		options.erase(key);
	}
	case_insensitive_map_t<string> headers;
	if (signing_config && signing_config->headers) {
		for (auto &entry : signing_config->headers->additional_properties) {
			headers[entry.first] = StringUtil::Join(entry.second, ", ");
		}
	}
	//! httpfs calls the signer for us, so it has to be handed the catalog's credentials. A secret refresh after the
	//! signer rejects them reloads the table, which puts the current ones in place.
	catalog.auth_handler->AddRemoteSignerHeaders(context, headers);
	auto signer_url = GetSignerURL(catalog, namespace_items, table_name, table_config, signing_config != nullptr);
	vector<string> header_names;
	for (auto &entry : headers) {
		header_names.push_back(entry.first);
	}
	std::sort(header_names.begin(), header_names.end());
	DUCKDB_LOG_DEBUG(context, "Iceberg table '%s': storage requests are signed by '%s', sending headers [%s]",
	                 log_table_name, signer_url, StringUtil::Join(header_names, ", "));
	options["signer_url"] = std::move(signer_url);
	options["signer_headers"] = StringMapToValue(headers);
	if (signing_config && signing_config->properties) {
		options["signer_properties"] = StringMapToValue(*signing_config->properties);
	}
}

} // namespace duckdb
