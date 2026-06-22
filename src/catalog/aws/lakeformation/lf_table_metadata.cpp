#include "catalog/aws/lakeformation/lf_table_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#ifndef EMSCRIPTEN

#include <aws/glue/model/GetUnfilteredTableMetadataResult.h>
#include <aws/glue/model/UnfilteredPartition.h>

namespace duckdb {

static bool IsAllRowsWildcard(const string &filter) {
	string trimmed = filter;
	StringUtil::Trim(trimmed);
	trimmed = StringUtil::Lower(trimmed);
	return trimmed.empty() || trimmed == "all" || trimmed == "allrows" || trimmed == "all_rows_wildcard";
}

LakeFormationTablePolicy
ParseUnfilteredTableMetadata(const ::Aws::Glue::Model::GetUnfilteredTableMetadataResult &result) {
	// Glue evaluates Lake Formation grants and returns the effective filter policy for
	// the caller. We never talk to LF for policy discovery; only for temporary creds.
	LakeFormationTablePolicy policy;
	if (!result.GetIsRegisteredWithLakeFormation()) {
		throw InvalidConfigurationException(
		    "Table is not registered with Lake Formation; cannot use LAKE_FORMATION_DATA_FILTERS mode");
	}

	if (!result.GetQueryAuthorizationId().empty()) {
		// Must be passed back to GetTemporaryGlueTableCredentials for the same scan.
		policy.query_authorization_id = result.GetQueryAuthorizationId();
	}

	for (auto &column : result.GetAuthorizedColumns()) {
		policy.authorized_columns.push_back(column);
	}
	policy.all_columns_authorized = policy.authorized_columns.empty();

	for (auto &cell_filter : result.GetCellFilters()) {
		// Cell filters (column-scoped row filters) are returned separately from the
		// table-level RowFilter string; v1 only supports plain row filters.
		LakeFormationCellFilter filter;
		if (cell_filter.ColumnNameHasBeenSet()) {
			filter.column_name = cell_filter.GetColumnName();
		}
		if (cell_filter.RowFilterExpressionHasBeenSet()) {
			filter.row_filter_expression = cell_filter.GetRowFilterExpression();
		}
		policy.cell_filters.push_back(std::move(filter));
	}

	if (!result.GetRowFilter().empty()) {
		auto expression = result.GetRowFilter();
		if (!IsAllRowsWildcard(expression)) {
			policy.row_filter_sql = expression;
		}
	}

	policy.loaded = true;
	return policy;
}

LakeFormationPartitionPolicy
ParseUnfilteredPartitionMetadata(const ::Aws::Glue::Model::UnfilteredPartition &unfiltered) {
	LakeFormationPartitionPolicy policy;
	auto &partition = unfiltered.GetPartition();
	for (auto &value : partition.GetValues()) {
		policy.partition_values.emplace_back(value);
	}
	return policy;
}

} // namespace duckdb

#endif

namespace duckdb {

void ValidateLakeFormationPolicyV1(const LakeFormationTablePolicy &policy, const IcebergTableSchema &schema) {
	// Fail closed on grant shapes Glue can express but we do not enforce yet.
	if (!policy.cell_filters.empty()) {
		throw NotImplementedException(
		    "Lake Formation cell-level data filters are not supported in v1. "
		    "Only row-level filters are supported when using LAKE_FORMATION_DATA_FILTERS.");
	}
	if (!policy.all_columns_authorized) {
		case_insensitive_set_t authorized;
		for (auto &col : policy.authorized_columns) {
			authorized.insert(col);
		}
		for (auto &col : schema.columns) {
			if (authorized.find(col->name) == authorized.end()) {
				throw NotImplementedException(
				    "Lake Formation column-level permissions are not supported in v1. "
				    "Grant access to all columns or use row-level filters only.");
			}
		}
	}
}

} // namespace duckdb
