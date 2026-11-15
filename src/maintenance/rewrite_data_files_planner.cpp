#include "maintenance/rewrite_data_files_planner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"
#include "maintenance/maintenance_table_loader.hpp"
#include "maintenance/rewrite_data_files_executor.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"

#include <algorithm>
#include <map>

namespace duckdb {

namespace {

//! Iceberg spec default for write.target-file-size-bytes (same as IcebergCopyOptions::file_size_bytes).
constexpr int64_t DEFAULT_TARGET_FILE_SIZE_BYTES = 512LL * 1024 * 1024;
constexpr int64_t MIN_TARGET_FILE_SIZE_BYTES = 100;

int64_t ParseTargetFileSizeProperty(const string &value, const string &property) {
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

int64_t ResolveTargetFileSizeBytes(const RewriteDataFilesPlanInput &input, const IcebergTableMetadata &metadata) {
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

//! Spark SizeBasedFileRewriter defaults: min = 75% of target, max = 180% of target.
int64_t ResolveMinFileSizeBytes(const RewriteDataFilesPlanInput &input, int64_t target_file_size_bytes) {
	if (input.min_file_size_bytes) {
		return input.min_file_size_bytes.value();
	}
	return (target_file_size_bytes * 3) / 4;
}

int64_t ResolveMaxFileSizeBytes(const RewriteDataFilesPlanInput &input, int64_t target_file_size_bytes) {
	if (input.max_file_size_bytes) {
		return input.max_file_size_bytes.value();
	}
	return (target_file_size_bytes * 9) / 5;
}

bool BucketQualifies(const RewriteBucket &bucket, const RewriteDataFilesPlanInput &input,
                     int64_t target_file_size_bytes) {
	if (input.rewrite_all) {
		return true;
	}
	//! Spark SizeBasedFileRewriter.tooMuchContent: one file above max-file-size-bytes
	//! is rewritten on its own so it can be split. A lone undersized file stays put.
	if (bucket.eligible_count == 1 && bucket.eligible_bytes > input.max_file_size_bytes.value()) {
		return true;
	}
	//! Spark SizeBasedFileRewriter: rewrite when the group has enough files,
	//! or at least two files whose total size already meets the target.
	if (bucket.eligible_count >= input.min_input_files) {
		return true;
	}
	//! Spark enoughContent uses a strict greater-than comparison against the target.
	return bucket.eligible_count >= 2 && bucket.eligible_bytes > target_file_size_bytes;
}

void ConsiderCandidate(RewriteBucket &bucket, RewriteCandidate cand, const RewriteDataFilesPlanInput &input) {
	//! Match Spark: rewrite undersized or oversized files; leave the band alone.
	if (!input.rewrite_all && cand.file_size_in_bytes >= input.min_file_size_bytes.value() &&
	    cand.file_size_in_bytes <= input.max_file_size_bytes.value()) {
		return;
	}
	bucket.eligible_count++;
	bucket.eligible_bytes += cand.file_size_in_bytes;
	if (input.max_files_to_rewrite &&
	    bucket.retained.size() >= static_cast<idx_t>(input.max_files_to_rewrite.value())) {
		//! Partition already has a Spark-style subList prefix; keep counting so gating stays cap-independent.
		return;
	}
	bucket.retained.push_back(std::move(cand));
}

bool UnpartitionedCollectionComplete(const RewriteBucket &bucket, const RewriteDataFilesPlanInput &input,
                                     int64_t target_file_size_bytes) {
	if (!input.max_files_to_rewrite) {
		return false;
	}
	if (bucket.retained.size() < static_cast<idx_t>(input.max_files_to_rewrite.value())) {
		return false;
	}
	return BucketQualifies(bucket, input, target_file_size_bytes);
}

void SelectFromBucket(RewritePlan &plan, RewriteBucket &bucket, const RewriteDataFilesPlanInput &input,
                      idx_t &remaining, bool capped) {
	if (!BucketQualifies(bucket, input, plan.target_file_size_bytes)) {
		return;
	}
	for (auto &cand : bucket.retained) {
		if (capped && remaining == 0) {
			return;
		}
		plan.selected_candidates.push_back(std::move(cand));
		if (capped) {
			remaining--;
		}
	}
}

idx_t RemainingRewriteCap(const RewriteDataFilesPlanInput &input, bool &capped) {
	capped = input.max_files_to_rewrite.has_value();
	if (capped) {
		return NumericCast<idx_t>(input.max_files_to_rewrite.value());
	}
	return NumericLimits<idx_t>::Maximum();
}

void SelectCandidates(RewritePlan &plan, const RewriteDataFilesPlanInput &input, RewriteBucket &bucket) {
	bool capped;
	idx_t remaining = RemainingRewriteCap(input, capped);
	SelectFromBucket(plan, bucket, input, remaining, capped);
}

void SelectCandidates(RewritePlan &plan, const RewriteDataFilesPlanInput &input,
                      std::map<string, RewriteBucket> &per_partition, RewriteBucket &other_specs) {
	bool capped;
	idx_t remaining = RemainingRewriteCap(input, capped);
	for (auto &kv : per_partition) {
		if (capped && remaining == 0) {
			return;
		}
		SelectFromBucket(plan, kv.second, input, remaining, capped);
	}
	if (capped && remaining == 0) {
		return;
	}
	SelectFromBucket(plan, other_specs, input, remaining, capped);
}

//! output_spec_id defaults to the current spec. A missing id is rejected before manifests are read.
//! The copy writer only emits the current default spec, so a historical output spec is rejected too.
int32_t ResolveOutputSpecId(const RewriteDataFilesPlanInput &input, const IcebergTableMetadata &metadata) {
	auto output_spec_id = metadata.default_spec_id;
	if (input.output_spec_id) {
		output_spec_id = input.output_spec_id.value();
	}
	if (metadata.partition_specs.find(output_spec_id) == metadata.partition_specs.end()) {
		throw InvalidInputException(
		    "iceberg_rewrite_data_files: 'output_spec_id' %d does not exist in the table partition specs",
		    output_spec_id);
	}
	auto spec = metadata.FindPartitionSpecById(output_spec_id);
	if (!spec) {
		throw InvalidInputException(
		    "iceberg_rewrite_data_files: 'output_spec_id' %d does not exist in the table partition specs",
		    output_spec_id);
	}
	if (spec->spec_id != metadata.default_spec_id) {
		throw InvalidInputException(
		    "iceberg_rewrite_data_files: 'output_spec_id' %d is not the table's current default partition spec %d; "
		    "rewriting data files into a historical partition spec is not supported",
		    spec->spec_id, metadata.default_spec_id);
	}
	return spec->spec_id;
}

optional<RewriteCandidate> TryMakeRewriteCandidate(const IcebergManifestEntry &entry) {
	if (entry.status == IcebergManifestEntryStatusType::DELETED) {
		return nullopt;
	}
	if (entry.data_file.content != IcebergManifestEntryContentType::DATA) {
		return nullopt;
	}
	RewriteCandidate cand;
	cand.file_path = entry.data_file.file_path;
	cand.file_size_in_bytes = entry.data_file.file_size_in_bytes;
	cand.record_count = entry.data_file.record_count;
	cand.partition_info = entry.data_file.partition_info;
	return cand;
}

void CollectUnpartitionedCandidates(RewriteBucket &bucket, const vector<IcebergManifestListEntry> &manifest_files,
                                    const RewriteDataFilesPlanInput &input, int64_t target_file_size_bytes) {
	for (const auto &list_entry : manifest_files) {
		if (list_entry.GetFile().content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (UnpartitionedCollectionComplete(bucket, input, target_file_size_bytes)) {
			continue;
		}
		for (const auto &entry : list_entry.GetManifestEntries()) {
			auto cand = TryMakeRewriteCandidate(entry);
			if (!cand) {
				continue;
			}
			ConsiderCandidate(bucket, std::move(*cand), input);
			if (UnpartitionedCollectionComplete(bucket, input, target_file_size_bytes)) {
				break;
			}
		}
	}
}

//! Canonical partition key used by the bin-packer.
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

void CollectPartitionedCandidates(std::map<string, RewriteBucket> &per_partition, RewriteBucket &other_specs,
                                  const vector<IcebergManifestListEntry> &manifest_files,
                                  const RewriteDataFilesPlanInput &input, int32_t output_spec_id) {
	for (const auto &list_entry : manifest_files) {
		if (list_entry.GetFile().content != IcebergManifestContentType::DATA) {
			continue;
		}
		//! Spark keeps output-spec files in per-partition buckets and groups every other spec together.
		const auto on_output_spec = list_entry.GetFile().partition_spec_id == output_spec_id;
		for (const auto &entry : list_entry.GetManifestEntries()) {
			auto cand = TryMakeRewriteCandidate(entry);
			if (!cand) {
				continue;
			}
			if (!on_output_spec) {
				ConsiderCandidate(other_specs, std::move(*cand), input);
				continue;
			}
			auto &bucket = per_partition[PartitionBucketKey(cand->partition_info)];
			ConsiderCandidate(bucket, std::move(*cand), input);
		}
	}
}

const Value *PartitionFieldValue(const vector<IcebergPartitionInfo> &partition_info, uint64_t field_id) {
	for (auto &partition : partition_info) {
		if (partition.field_id == field_id) {
			return &partition.value;
		}
	}
	return nullptr;
}

enum class DeleteApplyMatch { NO, YES, AMBIGUOUS };

using LiveDataFiles = unordered_map<string, std::pair<const IcebergManifest *, const IcebergManifestEntry *>>;

//! Apply the partition and sequence rules from IcebergFilePruner::DeleteFileMatchesDataFile.
//! A missing spec or partition value is ambiguous, so the caller leaves the delete file in place.
DeleteApplyMatch PositionDeleteApplies(const IcebergTableMetadata &metadata, const IcebergManifest &delete_manifest,
                                       const IcebergManifestEntry &delete_entry, const IcebergManifest &data_manifest,
                                       const IcebergManifestEntry &data_entry) {
	if (delete_entry.GetSequenceNumber(delete_manifest) < data_entry.GetSequenceNumber(data_manifest)) {
		return DeleteApplyMatch::NO;
	}
	auto spec_it = metadata.GetPartitionSpecs().find(delete_manifest.partition_spec_id);
	if (spec_it == metadata.GetPartitionSpecs().end()) {
		return DeleteApplyMatch::AMBIGUOUS;
	}
	if (spec_it->second.IsUnpartitioned()) {
		return DeleteApplyMatch::YES;
	}
	if (delete_manifest.partition_spec_id != data_manifest.partition_spec_id) {
		return DeleteApplyMatch::NO;
	}
	for (auto &field : spec_it->second.GetFields()) {
		auto delete_value = PartitionFieldValue(delete_entry.data_file.partition_info, field.partition_field_id);
		auto data_value = PartitionFieldValue(data_entry.data_file.partition_info, field.partition_field_id);
		if (!delete_value || !data_value) {
			return DeleteApplyMatch::AMBIGUOUS;
		}
		if (!Value::NotDistinctFrom(*delete_value, *data_value)) {
			return DeleteApplyMatch::NO;
		}
	}
	return DeleteApplyMatch::YES;
}

LiveDataFiles CollectLiveDataFiles(const vector<IcebergManifestListEntry> &manifest_files) {
	LiveDataFiles live_data;
	for (const auto &list_entry : manifest_files) {
		auto &manifest = list_entry.GetManifest();
		for (const auto &entry : list_entry.GetManifestEntries()) {
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			if (entry.data_file.content != IcebergManifestEntryContentType::DATA) {
				continue;
			}
			live_data.emplace(entry.data_file.file_path, std::make_pair(&manifest, &entry));
		}
	}
	return live_data;
}

//! Drop a delete file only when every data file it references is in the rewritten set.
//! referenced_data_file names that file directly. Otherwise a position delete is removed only when every
//! live data file it can match was rewritten. Dangling deletes stay (Spark remove-dangling-deletes defaults
//! to false). Equality deletes without referenced_data_file stay.
bool DeleteFileFullyApplied(const IcebergTableMetadata &metadata, const IcebergManifest &delete_manifest,
                            const IcebergManifestEntry &delete_entry, const LiveDataFiles &live_data,
                            const unordered_set<string> &rewritten_paths) {
	auto &delete_file = delete_entry.data_file;
	//! InvalidateFile(path) addresses an ordinary file. An offset identifies a deletion-vector blob.
	if (delete_file.content_offset || delete_file.file_path.empty()) {
		return false;
	}
	const bool has_reference = delete_file.referenced_data_file && !delete_file.referenced_data_file->empty();
	if (delete_file.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
		return has_reference && rewritten_paths.count(*delete_file.referenced_data_file) > 0;
	}
	if (delete_file.content != IcebergManifestEntryContentType::POSITION_DELETES) {
		return false;
	}
	if (has_reference) {
		if (rewritten_paths.count(*delete_file.referenced_data_file) == 0) {
			return false;
		}
		auto live = live_data.find(*delete_file.referenced_data_file);
		//! A reference identifies the file, but a stale delete may no longer apply to it.
		return live != live_data.end() &&
		       PositionDeleteApplies(metadata, delete_manifest, delete_entry, *live->second.first,
		                             *live->second.second) == DeleteApplyMatch::YES;
	}

	bool matched_live_file = false;
	for (auto &live : live_data) {
		auto match =
		    PositionDeleteApplies(metadata, delete_manifest, delete_entry, *live.second.first, *live.second.second);
		if (match == DeleteApplyMatch::AMBIGUOUS) {
			return false;
		}
		if (match == DeleteApplyMatch::NO) {
			continue;
		}
		matched_live_file = true;
		if (rewritten_paths.count(live.first) == 0) {
			return false;
		}
	}
	return matched_live_file;
}

void CollectFullyAppliedDeleteFiles(RewritePlan &plan, const IcebergTableMetadata &metadata,
                                    const vector<IcebergManifestListEntry> &manifest_files) {
	if (plan.selected_candidates.empty()) {
		return;
	}
	unordered_set<string> rewritten_paths;
	for (auto &candidate : plan.selected_candidates) {
		rewritten_paths.insert(candidate.file_path);
	}

	auto live_data = CollectLiveDataFiles(manifest_files);

	unordered_set<string> blocked_paths;
	vector<string> removable_paths;
	unordered_set<string> seen_removable;
	for (const auto &list_entry : manifest_files) {
		auto &manifest = list_entry.GetManifest();
		for (const auto &entry : list_entry.GetManifestEntries()) {
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			auto &delete_file = entry.data_file;
			if (delete_file.content != IcebergManifestEntryContentType::POSITION_DELETES &&
			    delete_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES) {
				continue;
			}
			if (!DeleteFileFullyApplied(metadata, manifest, entry, live_data, rewritten_paths)) {
				blocked_paths.insert(delete_file.file_path);
				continue;
			}
			if (seen_removable.insert(delete_file.file_path).second) {
				removable_paths.push_back(delete_file.file_path);
			}
		}
	}
	for (auto &path : removable_paths) {
		if (blocked_paths.count(path)) {
			continue;
		}
		plan.delete_files_to_remove.push_back(path);
	}
}

//! Bounds the rows the rewrite must write, which the commit checks. Deletes are matched to the selected files with
//! the scan's own rule, and a position delete file counts all its rows, so the bound only errs towards allowing
//! more deleted rows.
void BoundRewrittenRows(ClientContext &context, RewritePlan &plan, const IcebergTableMetadata &metadata,
                        const vector<IcebergManifestListEntry> &manifest_files) {
	plan.selected_record_count = 0;
	plan.max_deleted_rows = 0;
	for (auto &candidate : plan.selected_candidates) {
		int64_t total;
		if (!TryAddOperator::Operation(plan.selected_record_count, candidate.record_count, total)) {
			throw InvalidConfigurationException(
			    "iceberg_rewrite_data_files: selected data-file record counts exceed the supported BIGINT range");
		}
		plan.selected_record_count = total;
	}
	if (plan.selected_candidates.empty()) {
		return;
	}
	auto live_data = CollectLiveDataFiles(manifest_files);
	IcebergTableFilters no_filters;
	IcebergFilePruner pruner(context, metadata, metadata.GetLatestSchema(), no_filters);
	vector<std::pair<const IcebergManifestEntry *, partition_value_map_t>> selected;
	for (auto &candidate : plan.selected_candidates) {
		auto live = live_data.find(candidate.file_path);
		if (live == live_data.end()) {
			throw InternalException("iceberg_rewrite_data_files: selected file '%s' is not live", candidate.file_path);
		}
		selected.emplace_back(live->second.second,
		                      IcebergFilePruner::PartitionValueMap(live->second.second->data_file));
	}
	unordered_set<string> counted;
	for (const auto &list_entry : manifest_files) {
		auto &manifest = list_entry.GetManifest();
		for (const auto &entry : list_entry.GetManifestEntries()) {
			auto &delete_file = entry.data_file;
			if (entry.status == IcebergManifestEntryStatusType::DELETED ||
			    (delete_file.content != IcebergManifestEntryContentType::POSITION_DELETES &&
			     delete_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES)) {
				continue;
			}
			bool applies = false;
			IcebergEqualityDeleteBounds delete_bounds;
			for (auto &data : selected) {
				auto &data_manifest = *live_data.at(data.first->data_file.file_path).first;
				if (pruner.DeleteFileMatchesDataFile(manifest, entry, data_manifest, *data.first, data.second,
				                                     &delete_bounds)) {
					applies = true;
					break;
				}
			}
			if (!applies) {
				continue;
			}
			if (delete_file.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
				// an equality delete can remove any number of rows
				plan.max_deleted_rows.reset();
				return;
			}
			// a deletion vector shares its Puffin file with others, so it's identified by its offset too
			auto key = delete_file.file_path + "@" +
			           (delete_file.content_offset ? std::to_string(*delete_file.content_offset) : string());
			if (counted.insert(key).second) {
				// More deletes cannot lower the bound below zero, even when their counts overlap or overflow.
				*plan.max_deleted_rows +=
				    MinValue(delete_file.record_count, plan.selected_record_count - *plan.max_deleted_rows);
			}
		}
	}
}

} // namespace

//! The rewrite plans from the committed table: the files it selects and the schema and partition spec it writes
//! with are the committed ones. Files this transaction already added, replaced or deleted would be rewritten
//! again or lost, and a pending schema or spec change would not reach the files it writes, so the rewrite is
//! refused after any change to the table. The one exception is a format-version upgrade, which the rewrite
//! follows: its snapshot uses the new version.
static void ThrowIfTableHasPendingChanges(ClientContext &context, const IcebergTable &table_info) {
	auto &transaction = IcebergTransaction::Get(context, table_info.catalog);
	auto state = transaction.GetLatestTableState(table_info.GetTableKey());
	if (!state || state->IsMissing() || !state->GetInfo().transaction_data) {
		return;
	}
	auto &transaction_data = *state->GetInfo().transaction_data;
	ValidateRewriteTransaction(transaction_data);
}

RewritePlan PlanRewrite(ClientContext &context, RewriteDataFilesPlanInput input) {
	RewritePlan plan;
	plan.table_name = input.table_name;

	auto table_info_ptr = ReloadIcebergTableShared(context, input.table_name, "iceberg_rewrite_data_files");
	auto &table_info = *table_info_ptr;
	ThrowIfTableHasPendingChanges(context, table_info);
	auto &table_metadata = table_info.GetMetadata();
	if (table_metadata.iceberg_version < 2) {
		throw NotImplementedException("iceberg_rewrite_data_files on Iceberg V%d tables",
		                              table_metadata.iceberg_version);
	}
	plan.target_file_size_bytes = ResolveTargetFileSizeBytes(input, table_metadata);
	input.min_file_size_bytes = ResolveMinFileSizeBytes(input, plan.target_file_size_bytes);
	input.max_file_size_bytes = ResolveMaxFileSizeBytes(input, plan.target_file_size_bytes);
	if (input.min_file_size_bytes.value() > input.max_file_size_bytes.value()) {
		throw InvalidInputException("iceberg_rewrite_data_files: resolved 'min_file_size_bytes' (%lld) must be <= "
		                            "'max_file_size_bytes' (%lld)",
		                            input.min_file_size_bytes.value(), input.max_file_size_bytes.value());
	}
	//! Parse only compares explicit arguments. These checks run after table properties and the
	//! 75% / 180% defaults are filled in.
	if (plan.target_file_size_bytes <= input.min_file_size_bytes.value()) {
		throw InvalidInputException("iceberg_rewrite_data_files: resolved 'target_file_size_bytes' (%lld) must be > "
		                            "'min_file_size_bytes' (%lld)",
		                            plan.target_file_size_bytes, input.min_file_size_bytes.value());
	}
	if (plan.target_file_size_bytes >= input.max_file_size_bytes.value()) {
		throw InvalidInputException("iceberg_rewrite_data_files: resolved 'target_file_size_bytes' (%lld) must be < "
		                            "'max_file_size_bytes' (%lld)",
		                            plan.target_file_size_bytes, input.max_file_size_bytes.value());
	}
	plan.table_info = std::move(table_info_ptr);
	table_info.VerifyWriteEncryptionSupported();

	if (table_metadata.iceberg_version >= 3) {
		throw NotImplementedException("iceberg_rewrite_data_files does not yet support V3 tables");
	}

	//! Reject an unknown or historical output spec before reading manifests.
	auto output_spec_id = ResolveOutputSpecId(input, table_metadata);

	auto latest_snapshot = table_metadata.GetLatestSnapshot();
	if (!latest_snapshot) {
		//! Brand-new table — no snapshot to rewrite against. Caller treats this
		//! as a no-op success; executor and commit step must not run on this plan.
		return plan;
	}

	if (!latest_snapshot->sequence_number) {
		throw InvalidConfigurationException("snapshot.sequence_number is not set");
	}
	plan.starting_snapshot_id = latest_snapshot->snapshot_id;
	//! Rewritten files use the sequence number of the snapshot being compacted.
	plan.starting_sequence_number = *latest_snapshot->sequence_number;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = latest_snapshot;
	snapshot_info.schema_id = table_metadata.GetCurrentSchemaId();

	//! Install vended storage secrets before reading manifest lists from object storage.
	table_info.LoadCredentials(context);

	IcebergOptions options;
	auto manifest_list =
	    IcebergManifestList::Load(table_metadata.GetLocation(), table_metadata, snapshot_info, context, options);

	const auto &manifest_files = manifest_list->GetManifestFilesConst();
	if (manifest_files.empty()) {
		return plan;
	}

	const bool unpartitioned = !table_metadata.HasPartitionSpec();
	if (unpartitioned) {
		//! All files share the same output partition, including files written before partitioning was removed.
		//! Apply size/count gating and the rewrite cap to that single group.
		RewriteBucket bucket;
		CollectUnpartitionedCandidates(bucket, manifest_files, input, plan.target_file_size_bytes);
		SelectCandidates(plan, input, bucket);
	} else {
		//! Files already on the output spec keep per-partition gating. Every other spec shares one group,
		//! then the existing COPY rewrites both with the current spec (which partitions output files).
		RewriteBucket other_specs;
		std::map<string, RewriteBucket> per_partition;
		CollectPartitionedCandidates(per_partition, other_specs, manifest_files, input, output_spec_id);
		SelectCandidates(plan, input, per_partition, other_specs);
	}

	CollectFullyAppliedDeleteFiles(plan, table_metadata, manifest_files);
	BoundRewrittenRows(context, plan, table_metadata, manifest_files);
	return plan;
}

} // namespace duckdb
