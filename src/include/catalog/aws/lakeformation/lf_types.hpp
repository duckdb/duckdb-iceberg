#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct LakeFormationCellFilter {
	string column_name;
	string row_filter_expression;
};

struct LakeFormationPartitionPolicy {
	vector<Value> partition_values;
	string row_filter_sql;
};

struct LakeFormationTablePolicy {
	string row_filter_sql;
	vector<string> authorized_columns;
	bool all_columns_authorized = true;
	vector<LakeFormationCellFilter> cell_filters;
	string query_authorization_id;
	vector<LakeFormationPartitionPolicy> partition_policies;
	bool is_partitioned = false;
	bool loaded = false;
};

struct LakeFormationTemporaryCredentials {
	string access_key_id;
	string secret_access_key;
	string session_token;
	string expiration;
};

} // namespace duckdb
