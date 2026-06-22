#include "catalog/aws/lakeformation/lf_credential_cache.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

static bool CredentialsExpired(const LakeFormationTemporaryCredentials &credentials) {
	if (credentials.expiration.empty()) {
		return false;
	}
	timestamp_t expiration;
	auto result = Timestamp::TryConvertTimestamp(credentials.expiration.c_str(), credentials.expiration.size(),
	                                             expiration, false);
	if (result != TimestampCastResult::SUCCESS) {
		return false;
	}
	return Timestamp::GetCurrentTimestamp() >= expiration;
}

optional_ptr<const LakeFormationTemporaryCredentials> LakeFormationCredentialCache::Get(const string &cache_key) {
	lock_guard<mutex> guard(lock);
	auto entry = entries.find(cache_key);
	if (entry == entries.end()) {
		return nullptr;
	}
	if (CredentialsExpired(entry->second)) {
		entries.erase(entry);
		return nullptr;
	}
	return &entry->second;
}

void LakeFormationCredentialCache::Put(const string &cache_key, LakeFormationTemporaryCredentials credentials) {
	lock_guard<mutex> guard(lock);
	entries[cache_key] = std::move(credentials);
}

} // namespace duckdb
