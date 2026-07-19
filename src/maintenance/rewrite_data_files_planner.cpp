#include "maintenance/rewrite_data_files_planner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"
#include "maintenance/maintenance_table_loader.hpp"

#include <algorithm>
#include <map>

namespace duckdb {

namespace {

constexpr int64_t DEFAULT_TARGET_FILE_SIZE_BYTES = 134217728;
constexpr int64_t MIN_TARGET_FILE_SIZE_BYTES = 100;

static int64_t ParseTargetFileSizeProperty(const string &value, const string &property) {
	idx_t parsed_value;
	if (!TryCast::Operation<string_t, idx_t>(string_t(value), parsed_value)) {
		auto error = StringUtil::TryParseFormattedBytes(value, parsed_value);
		if (!error.empty()) {
			throw InvalidInputException("iceberg_rewrite_data_files: invalid '%s': %s", property, error);
		}
	}
	if (parsed_value < static_cast<idx_t>(MIN_TARGET_FILE_SIZE_BYTES)) {
		throw InvalidInputException("iceberg_rewrite_data_files: '%s' must be >= %lld bytes, got %llu", property,
		                            MIN_TARGET_FILE_SIZE_BYTES, static_cast<unsigned long long>(parsed_value));
	}
	if (parsed_value > static_cast<idx_t>(NumericLimits<int64_t>::Maximum())) {
		throw InvalidInputException("iceberg_rewrite_data_files: '%s' is too large", property);
	}
	return static_cast<int64_t>(parsed_value);
}

static int64_t ResolveTargetFileSizeBytes(const RewriteDataFilesPlanInput &input,
                                          const IcebergTableMetadata &metadata) {
	if (input.target_file_size_bytes) {
		return input.target_file_size_bytes.value();
	}
	auto &properties = metadata.GetTableProperties();
	auto it = properties.find("write.parquet.target-file-size-bytes");
	if (it != properties.end()) {
		return ParseTargetFileSizeProperty(it->second, "write.parquet.target-file-size-bytes");
	}
	it = properties.find("write.target-file-size-bytes");
	if (it != properties.end()) {
		return ParseTargetFileSizeProperty(it->second, "write.target-file-size-bytes");
	}
	return DEFAULT_TARGET_FILE_SIZE_BYTES;
}

void GroupCandidates(RewritePlan &plan, int64_t target_file_size_bytes, int64_t min_input_files, bool rewrite_all) {
	if (plan.candidates.empty()) {
		return;
	}

	std::map<string, vector<RewriteCandidate>> per_partition;
	for (auto &cand : plan.candidates) {
		if (!rewrite_all && cand.file_size_in_bytes >= target_file_size_bytes) {
			continue;
		}
		per_partition[rewrite_planner_internal::PartitionBucketKey(cand.partition_info)].push_back(cand);
	}

	for (auto &kv : per_partition) {
		if (!rewrite_all && static_cast<int64_t>(kv.second.size()) < min_input_files) {
			continue;
		}
		plan.file_groups.push_back(std::move(kv.second));
	}
}

} // namespace

namespace rewrite_planner_internal {

string PartitionBucketKey(const vector<IcebergPartitionInfo> &partition_info) {
	if (partition_info.empty()) {
		return "";
	}
	vector<IcebergPartitionInfo> sorted = partition_info;
	std::sort(sorted.begin(), sorted.end(),
	          [](const IcebergPartitionInfo &a, const IcebergPartitionInfo &b) { return a.field_id < b.field_id; });
	//! Length-prefix values so partition keys cannot collide on delimiters.
	string out;
	for (auto &p : sorted) {
		out += std::to_string(p.field_id);
		out += ":";
		if (p.value.IsNull()) {
			out += "NULL";
		} else {
			auto val_str = p.value.ToString();
			out += std::to_string(val_str.size());
			out += ":";
			out += val_str;
		}
		out += ";";
	}
	return out;
}

} // namespace rewrite_planner_internal

RewritePlan PlanRewrite(ClientContext &context, const RewriteDataFilesPlanInput &input) {
	RewritePlan plan;
	plan.table_name = input.table_name;

	auto table_info_ptr = ReloadIcebergTableShared(context, input.table_name, "iceberg_rewrite_data_files");
	auto &table_info = *table_info_ptr;
	auto &table_metadata = table_info.table_metadata;
	plan.target_file_size_bytes = ResolveTargetFileSizeBytes(input, table_metadata);
	plan.table_info = std::move(table_info_ptr);

	if (table_metadata.iceberg_version >= 3) {
		throw NotImplementedException("iceberg_rewrite_data_files does not yet support V3 tables");
	}

	auto latest_snapshot = table_metadata.GetLatestSnapshot();
	if (!latest_snapshot) {
		//! Brand-new table — no snapshot to rewrite against. Caller treats this
		//! as a no-op success; executor and commit step must not run on this plan.
		return plan;
	}

	if (!latest_snapshot->snapshot_id) {
		throw InvalidConfigurationException("snapshot.snapshot_id is not set");
	}
	if (!latest_snapshot->sequence_number) {
		throw InvalidConfigurationException("snapshot.sequence_number is not set");
	}
	plan.starting_snapshot_id = *latest_snapshot->snapshot_id;
	//! Rewritten files use the sequence number of the snapshot being compacted.
	plan.starting_sequence_number = *latest_snapshot->sequence_number;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = latest_snapshot;
	snapshot_info.schema_id = table_metadata.GetCurrentSchemaId();

	IcebergOptions options;
	auto manifest_list =
	    IcebergManifestList::Load(table_metadata.GetLocation(), table_metadata, snapshot_info, context, options);

	const auto &manifest_files = manifest_list->GetManifestFilesConst();
	if (manifest_files.empty()) {
		return plan;
	}

	auto default_spec_id = table_metadata.default_spec_id;

	for (const auto &list_entry : manifest_files) {
		if (list_entry.file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		//! Guard against partition spec evolution: if a manifest was written
		//! under a different partition spec, its partition tuples may not match
		//! the current default spec. Mixing specs in one rewrite group would
		//! produce incorrect manifest metadata. Reject until multi-spec support
		//! is implemented.
		if (list_entry.file.partition_spec_id != default_spec_id) {
			throw NotImplementedException(
			    "iceberg_rewrite_data_files: table has data files written under partition spec %d "
			    "but current default spec is %d; partition spec evolution is not yet supported",
			    list_entry.file.partition_spec_id, default_spec_id);
		}

		for (const auto &entry : list_entry.GetManifestEntries()) {
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			if (entry.data_file.content != IcebergManifestEntryContentType::DATA) {
				continue;
			}

			RewriteCandidate cand;
			cand.file_path = entry.data_file.file_path;
			cand.file_size_in_bytes = entry.data_file.file_size_in_bytes;
			cand.record_count = entry.data_file.record_count;
			cand.partition_info = entry.data_file.partition_info;
			plan.candidates.push_back(std::move(cand));
		}
	}

	if (plan.candidates.empty()) {
		return plan;
	}

	GroupCandidates(plan, plan.target_file_size_bytes, input.min_input_files, input.rewrite_all);

	return plan;
}

} // namespace duckdb
