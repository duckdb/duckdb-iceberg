#include "storage/remote_signing/iceberg_remote_signing.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "catalog/rest/api/url_utils.hpp"

namespace duckdb {

static string GetConfigValue(const case_insensitive_map_t<string> &config, const vector<string> &keys) {
	for (auto &key : keys) {
		auto entry = config.find(key);
		if (entry != config.end() && !entry->second.empty()) {
			return entry->second;
		}
	}
	return string();
}

//! Mirrors RESTUtil.resolveEndpoint: an absolute endpoint replaces the signer uri instead of extending it
static string ResolveSignerUrl(const string &uri, const string &endpoint) {
	auto lower_endpoint = StringUtil::Lower(endpoint);
	if (StringUtil::StartsWith(lower_endpoint, "http://") || StringUtil::StartsWith(lower_endpoint, "https://")) {
		return endpoint;
	}
	auto base = uri;
	StringUtil::RTrim(base, "/");
	if (base.empty()) {
		throw InvalidConfigurationException(
		    "Iceberg remote signing is enabled for this table, but no signer uri could be resolved");
	}
	idx_t path_start = 0;
	while (path_start < endpoint.size() && endpoint[path_start] == '/') {
		path_start++;
	}
	return AddHttpHostIfMissing(base + "/" + endpoint.substr(path_start));
}

bool IcebergRemoteSigningConfig::IsSupportedLocation(const string &location) {
	return StringUtil::StartsWith(location, "s3://") || StringUtil::StartsWith(location, "s3a://") ||
	       StringUtil::StartsWith(location, "s3n://");
}

bool IcebergRemoteSigningConfig::TryParse(const case_insensitive_map_t<string> &config, const string &catalog_uri,
                                          const string &catalog_name, IcebergRemoteSigningTarget &result) {
	auto enabled = GetConfigValue(config, {"s3.remote-signing-enabled", "remote-signing-enabled"});
	if (!StringUtil::CIEquals(enabled, "true")) {
		return false;
	}

	auto signer_uri = GetConfigValue(config, {"signer.uri", "s3.signer.uri"});
	if (signer_uri.empty()) {
		signer_uri = catalog_uri;
	}
	auto signer_endpoint = GetConfigValue(config, {"signer.endpoint", "s3.signer.endpoint"});
	if (signer_endpoint.empty()) {
		signer_endpoint = DEFAULT_SIGNER_ENDPOINT;
	}

	result.catalog_name = catalog_name;
	result.signer_url = ResolveSignerUrl(signer_uri, signer_endpoint);
	result.region = GetConfigValue(config, {"s3.region", "client.region", "region"});
	if (result.region.empty()) {
		throw InvalidConfigurationException("Iceberg remote signing is enabled for this table, but the catalog did "
		                                    "not report the 's3.region' that requests have to be signed for");
	}
	result.path_style_access = StringUtil::CIEquals(GetConfigValue(config, {"s3.path-style-access"}), "true");

	auto endpoint = GetConfigValue(config, {"s3.endpoint"});
	if (endpoint.empty()) {
		result.host = StringUtil::Format("s3.%s.amazonaws.com", result.region);
		return true;
	}

	if (StringUtil::StartsWith(StringUtil::Lower(endpoint), "http://")) {
		result.use_ssl = false;
	}
	auto authority = StripScheme(endpoint);
	StringUtil::RTrim(authority, "/");
	auto path_start = authority.find('/');
	if (path_start != string::npos) {
		result.path_prefix = authority.substr(path_start);
		authority = authority.substr(0, path_start);
	}
	result.host = authority;
	return true;
}

void IcebergRemoteSigningRegistry::RegisterTarget(const string &location, IcebergRemoteSigningTarget target) {
	auto prefix = location;
	if (!StringUtil::EndsWith(prefix, "/")) {
		prefix += "/";
	}
	lock_guard<mutex> guard(lock);
	targets[prefix] = std::move(target);
	has_targets = true;
}

bool IcebergRemoteSigningRegistry::TryGetTarget(const string &path, IcebergRemoteSigningTarget &result) {
	if (Empty()) {
		return false;
	}
	lock_guard<mutex> guard(lock);
	idx_t longest_match = 0;
	bool found = false;
	for (auto &entry : targets) {
		if (entry.first.size() <= longest_match || !StringUtil::StartsWith(path, entry.first)) {
			continue;
		}
		longest_match = entry.first.size();
		result = entry.second;
		found = true;
	}
	return found;
}

bool IcebergRemoteSigningRegistry::TryGetSignature(const string &cache_key, IcebergSignedRequest &result) {
	lock_guard<mutex> guard(lock);
	auto entry = signatures.find(cache_key);
	if (entry == signatures.end()) {
		return false;
	}
	if (Timestamp::GetCurrentTimestamp() >= entry->second.expires_at) {
		signatures.erase(entry);
		return false;
	}
	result = entry->second.signature;
	return true;
}

void IcebergRemoteSigningRegistry::PutSignature(const string &cache_key, const IcebergSignedRequest &signature) {
	auto now = Timestamp::GetCurrentTimestamp();
	CachedSignature cached;
	cached.expires_at = Timestamp::FromEpochMs(Timestamp::GetEpochMs(now) + SIGNATURE_CACHE_MS);
	cached.signature = signature;

	lock_guard<mutex> guard(lock);
	//! Every file gets its own entry and is rarely signed twice, so expired entries have to be swept
	//! instead of being evicted on lookup
	if (signatures.size() >= SIGNATURE_CACHE_SWEEP_SIZE) {
		for (auto it = signatures.begin(); it != signatures.end();) {
			it = now >= it->second.expires_at ? signatures.erase(it) : std::next(it);
		}
	}
	signatures[cache_key] = std::move(cached);
}

} // namespace duckdb
