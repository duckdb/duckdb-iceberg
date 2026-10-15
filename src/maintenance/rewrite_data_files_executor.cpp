#include "maintenance/rewrite_data_files_executor.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

void AccountSelectedCandidates(const RewritePlan &plan, RewriteExecutionResult &result) {
	result.rewritten_data_files = static_cast<int64_t>(plan.selected_candidates.size());
	result.rewritten_bytes = 0;
	result.rewritten_candidates.clear();
	result.rewritten_candidates.reserve(plan.selected_candidates.size());
	for (auto &candidate : plan.selected_candidates) {
		result.rewritten_bytes += candidate.file_size_in_bytes;
		result.rewritten_candidates.push_back(candidate);
	}
}

void PinRewriteSequenceNumbers(vector<IcebergManifestEntry> &entries, int64_t starting_sequence_number) {
	for (auto &entry : entries) {
		//! Preserve equality-delete applicability after compaction.
		entry.SetSequenceNumber(starting_sequence_number);
	}
}

void ValidateRewriteSnapshot(const RewritePlan &plan, const IcebergTable &table_info, const string &phase) {
	auto snapshot = table_info.table_metadata.GetLatestSnapshot();
	if (plan.starting_snapshot_id < 0) {
		if (snapshot) {
			throw CatalogException(
			    "iceberg_rewrite_data_files: table snapshot changed after planning an empty rewrite during %s", phase);
		}
		return;
	}
	if (!snapshot || snapshot->snapshot_id != plan.starting_snapshot_id) {
		throw CatalogException("iceberg_rewrite_data_files: table snapshot changed between planning (%lld) and %s (%s)",
		                       plan.starting_snapshot_id, phase,
		                       snapshot ? std::to_string(snapshot->snapshot_id) : "none");
	}
}

void CleanupRewriteFiles(ClientContext &context, const IcebergTable &table_info, const vector<string> &produced_paths) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &path : produced_paths) {
		try {
			fs.TryRemoveFile(path);
		} catch (...) {
			DUCKDB_LOG_DEBUG(context, "Failed to clean up rewrite output file '%s'", path);
		}
	}
}

void CommitRewrite(ClientContext &context, const RewritePlan &plan, RewriteExecutionResult &result) {
	if (result.new_entries.empty()) {
		return;
	}
	Value fail_next;
	if (context.TryGetCurrentSetting("iceberg_rewrite_fail_next_commit", fail_next) && !fail_next.IsNull() &&
	    BooleanValue::Get(fail_next)) {
		throw IOException("Injected rewrite commit failure");
	}
	Value fail_index;
	if (context.TryGetCurrentSetting("iceberg_rewrite_fail_commit_index", fail_index) && !fail_index.IsNull() &&
	    fail_index.GetValue<int64_t>() == 1 && !plan.partial_progress_enabled) {
		throw IOException("Injected rewrite commit failure");
	}
	if (!plan.table_info) {
		throw InternalException("iceberg_rewrite_data_files: rewrite plan has no table information");
	}
	auto &table_info = *plan.table_info;
	ValidateRewriteSnapshot(plan, table_info, "commit");

	auto &iceberg_transaction = IcebergTransaction::Get(context, table_info.catalog);
	IcebergManifestDeletes deletes;
	for (auto &cand : result.rewritten_candidates) {
		deletes.InvalidateFile(cand.file_path);
	}

	ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTable &tbl) {
		ValidateRewriteSnapshot(plan, tbl, "transaction commit");
		auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
		transaction_data.AddSnapshot(IcebergSnapshotOperationType::REPLACE, std::move(result.new_entries),
		                             std::move(deletes));
	});
}

namespace {

string SqlQuote(const string &text) {
	return "'" + StringUtil::Replace(text, "'", "''") + "'";
}

string TableIdentifier(const QualifiedName &name) {
	return name.ToString();
}

} // namespace

RewriteExecutionResult ExecutePartialRewrite(ClientContext &context, const RewritePlan &plan) {
	RewriteExecutionResult total;
	int64_t fail_index = 0;
	Value fail_index_setting;
	if (context.TryGetCurrentSetting("iceberg_rewrite_fail_commit_index", fail_index_setting) &&
	    !fail_index_setting.IsNull()) {
		fail_index = fail_index_setting.GetValue<int64_t>();
	}

	idx_t index = 0;
	int64_t commits_used = 0;
	int64_t failed_commits = 0;
	int64_t attempt = 0;
	auto max_commits = plan.partial_progress_max_commits;
	while (index < plan.file_groups.size() && commits_used < max_commits) {
		auto remaining = plan.file_groups.size() - index;
		auto commits_left = NumericCast<idx_t>(max_commits - commits_used);
		auto batch = (remaining + commits_left - 1) / commits_left;
		if (plan.max_concurrent_file_group_rewrites > 0) {
			auto concurrent = NumericCast<idx_t>(plan.max_concurrent_file_group_rewrites);
			//! Keep enough groups in this commit to stay within max_commits, even if that exceeds concurrency.
			auto min_batch = batch;
			if (concurrent < min_batch) {
				batch = min_batch;
			} else {
				batch = std::min(concurrent, remaining);
				auto needed = (remaining + batch - 1) / batch;
				if (needed > commits_left) {
					batch = min_batch;
				}
			}
		}
		attempt++;

		string paths;
		int64_t batch_files = 0;
		for (idx_t group_index = 0; group_index < batch; group_index++) {
			for (auto &file : plan.file_groups[index + group_index].files) {
				if (!paths.empty()) {
					paths.push_back('\n');
				}
				paths += file.file_path;
				batch_files++;
			}
		}

		Connection worker(DatabaseInstance::GetDatabase(context));
		auto set_paths = worker.Query("SET iceberg_rewrite_only_paths = " + SqlQuote(paths));
		if (set_paths->HasError()) {
			throw IOException(set_paths->GetError());
		}
		if (fail_index != 0 && attempt == fail_index) {
			auto set_fail = worker.Query("SET iceberg_rewrite_fail_next_commit = true");
			if (set_fail->HasError()) {
				throw IOException(set_fail->GetError());
			}
		}
		auto call = "CALL iceberg_rewrite_data_files(" + SqlQuote(TableIdentifier(plan.table_name)) +
		            ", partial_progress_enabled => false, rewrite_all => true, min_input_files => 1" +
		            ", target_file_size_bytes => " + std::to_string(plan.target_file_size_bytes) + ")";
		auto result = worker.Query(call);
		if (result->HasError()) {
			auto error = result->GetError();
			if (!StringUtil::Contains(error, "Injected rewrite commit failure")) {
				throw IOException(error);
			}
			failed_commits++;
			total.failed_data_files += batch_files;
			if (failed_commits > plan.partial_progress_max_failed_commits) {
				throw IOException(error);
			}
		} else {
			auto chunk = result->Fetch();
			if (chunk && chunk->size() > 0) {
				total.rewritten_data_files += chunk->GetValue(0, 0).GetValue<int64_t>();
				total.added_data_files += chunk->GetValue(1, 0).GetValue<int64_t>();
				total.rewritten_bytes += chunk->GetValue(2, 0).GetValue<int64_t>();
			}
			commits_used++;
		}
		index += batch;
	}
	return total;
}

} // namespace duckdb
