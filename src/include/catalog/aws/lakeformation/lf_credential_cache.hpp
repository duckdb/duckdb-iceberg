#pragma once

#include "catalog/aws/lakeformation/lf_types.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class LakeFormationCredentialCache {
public:
	optional_ptr<const LakeFormationTemporaryCredentials> Get(const string &cache_key);
	void Put(const string &cache_key, LakeFormationTemporaryCredentials credentials);

private:
	mutex lock;
	unordered_map<string, LakeFormationTemporaryCredentials> entries;
};

} // namespace duckdb
