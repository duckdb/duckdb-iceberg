#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct HostDecompositionResult {
	string authority;
	vector<string> path_components;
};

inline HostDecompositionResult DecomposeHost(const string &host) {
	HostDecompositionResult result;
	auto start_of_path = host.find('/');
	if (start_of_path != string::npos) {
		result.authority = host.substr(0, start_of_path);
		auto remainder = host.substr(start_of_path + 1);
		result.path_components = StringUtil::Split(remainder, '/');
	} else {
		result.authority = host;
	}
	return result;
}

inline bool IsAwsRegion(const string &token) {
	static const vector<string> prefixes = {"us-", "eu-", "ap-", "sa-", "ca-", "me-", "af-", "il-", "mx-"};
	bool has_prefix = false;
	for (auto &prefix : prefixes) {
		if (StringUtil::StartsWith(token, prefix)) {
			has_prefix = true;
			break;
		}
	}
	if (!has_prefix) {
		return false;
	}
	if (token.empty() || !StringUtil::CharacterIsDigit(token.back())) {
		return false;
	}
	return true;
}

inline string GetAwsRegion(const string &host) {
	auto parts = StringUtil::Split(host, '.');
	for (auto &part : parts) {
		if (IsAwsRegion(part)) {
			return part;
		}
	}
	throw InvalidInputException("Could not parse AWS region from host: %s", host);
}

inline string GetAwsService(const string &host) {
	auto parts = StringUtil::Split(host, '.');
	for (idx_t i = 0; i < parts.size(); i++) {
		if (IsAwsRegion(parts[i]) && i > 0) {
			return parts[i - 1];
		}
	}
	throw InvalidInputException("Could not parse AWS service from host: %s", host);
}

//! Detect storage type from a location URL.
inline string DetectStorageType(const string &location) {
	if (StringUtil::StartsWith(location, "gs://") || StringUtil::Contains(location, "storage.googleapis.com")) {
		return "gcs";
	} else if (StringUtil::StartsWith(location, "s3://") || StringUtil::StartsWith(location, "s3a://")) {
		return "s3";
	} else if (StringUtil::StartsWith(location, "abfs://") || StringUtil::StartsWith(location, "abfss://") ||
	           StringUtil::StartsWith(location, "az://")) {
		return "azure";
	}
	// Default to s3 for backward compatibility
	return "s3";
}

//! Check whether a credential prefix belongs to the given storage type.
//! This handles the mismatch between friendly storage type names (e.g. "azure", "gcs")
//! and actual URI scheme prefixes used in vended credentials (e.g. "abfs://", "gs://").
inline bool CredentialMatchesStorageType(const string &credential_prefix, const string &storage_type) {
	if (storage_type == "s3") {
		return StringUtil::StartsWith(credential_prefix, "s3://") ||
		       StringUtil::StartsWith(credential_prefix, "s3a://") ||
		       StringUtil::StartsWith(credential_prefix, "s3n://");
	} else if (storage_type == "gcs") {
		return StringUtil::StartsWith(credential_prefix, "gs://") ||
		       StringUtil::StartsWith(credential_prefix, "gcs://");
	} else if (storage_type == "azure") {
		return StringUtil::StartsWith(credential_prefix, "abfs://") ||
		       StringUtil::StartsWith(credential_prefix, "abfss://") ||
		       StringUtil::StartsWith(credential_prefix, "az://");
	}
	// Unknown storage type — accept the credential to be safe
	return StringUtil::StartsWith(credential_prefix, storage_type);
}

} // namespace duckdb
