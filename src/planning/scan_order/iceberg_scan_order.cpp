#include "planning/scan_order/iceberg_scan_order.hpp"

#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "iceberg_logging.hpp"

#include <algorithm>

namespace duckdb {

namespace {

static bool ScanOrderCompare(const Value &v1, const Value &v2, OrderByStatistics stat_type) {
	return (stat_type == OrderByStatistics::MAX && v1 < v2) || (stat_type == OrderByStatistics::MIN && v1 > v2);
}

struct IcebergOrderEntry {
	idx_t entry_idx;
	Value lower;
	Value upper;
	idx_t count;
};

} // namespace

IcebergScanOrder::IcebergScanOrder() {
}

IcebergScanOrder::~IcebergScanOrder() {
}

void IcebergScanOrder::Set(unique_ptr<RowGroupOrderOptions> new_options) {
	options = std::move(new_options);
	applied = false;
}

unique_ptr<RowGroupOrderOptions> IcebergScanOrder::CopyOptions() const {
	return options ? make_uniq<RowGroupOrderOptions>(*options) : nullptr;
}

optional_ptr<const RowGroupOrderOptions> IcebergScanOrder::GetOptions() const {
	return options.get();
}

bool IcebergScanOrder::IsPending() const {
	return options && !applied;
}

void IcebergScanOrder::Apply(ClientContext &context, const IcebergTableSchema &schema,
                             bool has_matching_delete_manifests, vector<BoundIcebergManifestEntry> &manifest_entries) {
	if (!options || applied) {
		return;
	}
	applied = true;

	auto &opts = *options;
	if (opts.column_type != OrderByColumnType::NUMERIC || opts.column_idx.HasChildren()) {
		return;
	}
	if (manifest_entries.size() <= 1) {
		return;
	}

	auto &schema_columns = schema.columns;
	auto schema_idx = opts.column_idx.GetPrimaryIndex();
	if (schema_idx >= schema_columns.size()) {
		return;
	}
	auto &order_column = *schema_columns[schema_idx];
	auto field_id = order_column.id;

	bool can_prune = opts.row_limit.IsValid();
	vector<IcebergOrderEntry> order_entries;
	order_entries.reserve(manifest_entries.size());
	for (idx_t i = 0; i < manifest_entries.size(); i++) {
		auto &data_file = manifest_entries[i].entry.data_file;
		auto lower_it = data_file.lower_bounds.find(field_id);
		auto upper_it = data_file.upper_bounds.find(field_id);
		if (lower_it == data_file.lower_bounds.end() || upper_it == data_file.upper_bounds.end()) {
			return;
		}
		auto stats = IcebergPredicateStats::DeserializeBounds(context, lower_it->second, upper_it->second,
		                                                      order_column.name, order_column.type);
		if (!stats.lower_bound || !stats.upper_bound || stats.lower_bound->IsNull() || stats.upper_bound->IsNull()) {
			return;
		}
		auto null_it = data_file.null_value_counts.find(field_id);
		if (null_it == data_file.null_value_counts.end() || null_it->second > 0) {
			can_prune = false;
		}
		order_entries.push_back(
		    {i, *stats.lower_bound, *stats.upper_bound, NumericCast<idx_t>(data_file.record_count)});
	}

	if (has_matching_delete_manifests) {
		can_prune = false;
	}

	const auto stat_type = opts.order_by;
	const bool ascending = opts.order_type == OrderType::ASCENDING;
	auto primary = [&](const IcebergOrderEntry &entry) -> const Value & {
		return stat_type == OrderByStatistics::MAX ? entry.upper : entry.lower;
	};
	auto opposite = [&](const IcebergOrderEntry &entry) -> const Value & {
		return stat_type == OrderByStatistics::MAX ? entry.lower : entry.upper;
	};

	std::stable_sort(order_entries.begin(), order_entries.end(),
	                 [&](const IcebergOrderEntry &left, const IcebergOrderEntry &right) {
		                 return ascending ? primary(left) < primary(right) : primary(right) < primary(left);
	                 });

	idx_t keep = order_entries.size();
	if (can_prune && opts.row_limit.GetIndex() > 0) {
		const auto row_limit = opts.row_limit.GetIndex();
		keep = 0;
		for (idx_t k = 0; k < order_entries.size(); k++) {
			const auto &frontier = primary(order_entries[k]);
			idx_t guaranteed = 0;
			for (idx_t j = 0; j < k; j++) {
				if (!ScanOrderCompare(opposite(order_entries[j]), frontier, stat_type)) {
					guaranteed += order_entries[j].count;
				}
				if (guaranteed >= row_limit) {
					break;
				}
			}
			if (guaranteed >= row_limit) {
				break;
			}
			keep = k + 1;
		}
	}

	if (keep < order_entries.size()) {
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg Scan Order Pushdown, kept %llu of %llu 'data_file's for ORDER BY LIMIT %llu", keep,
		           order_entries.size(), opts.row_limit.GetIndex());
	}

	vector<BoundIcebergManifestEntry> reordered;
	reordered.reserve(keep);
	for (idx_t i = 0; i < keep; i++) {
		reordered.push_back(manifest_entries[order_entries[i].entry_idx]);
	}
	manifest_entries = std::move(reordered);
}

} // namespace duckdb
