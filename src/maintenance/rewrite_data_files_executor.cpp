#include "maintenance/rewrite_data_files_executor.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"

namespace duckdb {

namespace rewrite_executor_internal {

//! Wrap a string in single quotes and double any embedded `'`. We never expose
//! this to untrusted input today (file paths come from the manifest, schema
//! names come from the iceberg catalog), but the escaping is still required so
//! a column name like `it's` doesn't blow up the SQL parser.
static string QuoteSqlString(const string &s) {
	string out;
	out.reserve(s.size() + 2);
	out.push_back('\'');
	for (char c : s) {
		if (c == '\'') {
			out.push_back('\'');
		}
		out.push_back(c);
	}
	out.push_back('\'');
	return out;
}

//! Recursive walk over an iceberg column definition emitting the FIELD_IDS
//! literal. For primitives the literal is the field_id as an integer; for
//! nested types it's a struct with a sentinel `__duckdb_field_id` slot plus
//! one slot per child. Mirrors the Value tree built by
//! `iceberg_insert.cpp::GetFieldIdValue` so APPEND and REWRITE produce the
//! same on-disk parquet metadata for the same logical schema.
static string RenderFieldIdValue(const IcebergColumnDefinition &col) {
	if (col.children.empty()) {
		return std::to_string(col.id);
	}
	string out = "{'__duckdb_field_id': ";
	out += std::to_string(col.id);
	for (auto &child : col.children) {
		out += ", ";
		out += QuoteSqlString(child->name);
		out += ": ";
		out += RenderFieldIdValue(*child);
	}
	out += "}";
	return out;
}

string BuildFieldIdsLiteral(const IcebergTableSchema &schema) {
	string out = "{";
	for (idx_t i = 0; i < schema.columns.size(); ++i) {
		if (i > 0) {
			out += ", ";
		}
		out += QuoteSqlString(schema.columns[i]->name);
		out += ": ";
		out += RenderFieldIdValue(*schema.columns[i]);
	}
	out += "}";
	return out;
}

static string BuildSimpleTableReference(const string &catalog, const string &schema, const string &table) {
	D_ASSERT(!catalog.empty() && !schema.empty() && !table.empty());
	return catalog + "." + schema + "." + table;
}

string BuildRewriteCopySql(const string &catalog, const string &schema, const string &table,
                           const vector<string> &file_paths, const string &target_dir,
                           const string &field_ids_literal) {
	string file_list = "[";
	for (idx_t i = 0; i < file_paths.size(); ++i) {
		if (i > 0) {
			file_list += ", ";
		}
		file_list += QuoteSqlString(file_paths[i]);
	}
	file_list += "]";

	//! Read via the catalog-attached Iceberg table so the scan layer applies
	//! MoR position/equality deletes during read. `WHERE filename IN (...)`
	//! uses the iceberg `filename` virtual column to scope the scan to this
	//! rewrite group's files.
	string sql = "COPY (SELECT * FROM ";
	sql += BuildSimpleTableReference(catalog, schema, table);
	sql += " WHERE filename IN ";
	sql += file_list;
	sql += ") TO ";
	sql += QuoteSqlString(target_dir);
	sql += " (FORMAT PARQUET, FIELD_IDS ";
	sql += field_ids_literal;
	sql += ", FILENAME_PATTERN '{uuidv7}', RETURN_FILES TRUE, PER_THREAD_OUTPUT FALSE";
	sql += ", OVERWRITE_OR_IGNORE TRUE)";
	return sql;
}

} // namespace rewrite_executor_internal

IcebergManifestEntry ExecuteOneGroup(ClientContext &context, const IcebergTableInformation &table_info,
                                     const MaintenanceTableKey &table_key,
                                     const vector<RewriteCandidate> &group, int64_t starting_sequence_number) {
	if (group.empty()) {
		throw InternalException("iceberg_rewrite_data_files: ExecuteOneGroup called with an empty group");
	}
	auto &metadata = table_info.table_metadata;
	auto &fs = FileSystem::GetFileSystem(context);
	auto data_path = metadata.GetDataPath(fs);

	auto schema_id = metadata.GetCurrentSchemaId();
	auto schema_it = metadata.GetSchemas().find(schema_id);
	if (schema_it == metadata.GetSchemas().end()) {
		throw InternalException("iceberg_rewrite_data_files: current schema id %d not found in metadata", schema_id);
	}
	auto &schema = *schema_it->second;

	vector<string> file_paths;
	file_paths.reserve(group.size());
	for (auto &cand : group) {
		file_paths.push_back(cand.file_path);
	}

	auto field_ids = rewrite_executor_internal::BuildFieldIdsLiteral(schema);
	auto sql = rewrite_executor_internal::BuildRewriteCopySql(table_key.catalog, table_key.schema, table_key.table,
	                                                          file_paths, data_path, field_ids);

	//! Fresh Connection on the same DatabaseInstance — see
	//! `iceberg_to_ducklake.cpp:953` for the precedent. Secret manager state
	//! is at the DB level so OSS/S3 credentials carry over without explicit
	//! plumbing. We avoid running on `context` directly because COPY would
	//! re-enter the table function pipeline.
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection conn(db);
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->ThrowError("iceberg_rewrite_data_files: failed to write rewrite group: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw InternalException(
		    "iceberg_rewrite_data_files: COPY returned no rows from RETURN_FILES — expected exactly one");
	}
	if (chunk->size() > 1) {
		//! PER_THREAD_OUTPUT=false + no partition_output should yield exactly
		//! one summary row. More than one means our assumptions about the COPY
		//! shape drifted — fail loud rather than silently dropping rows.
		throw InternalException(
		    "iceberg_rewrite_data_files: COPY returned %llu summary rows (expected 1)",
		    (unsigned long long)chunk->size());
	}

	//! CHANGED_ROWS_AND_FILE_LIST layout (DuckDB v1.4):
	//!   0 Count BIGINT          — total rows written across this COPY
	//!   1 Files LIST(VARCHAR)   — produced output file paths
	auto count_val = chunk->GetValue(0, 0);
	auto files_val = chunk->GetValue(1, 0);
	auto &produced_files = ListValue::GetChildren(files_val);
	if (produced_files.size() != 1) {
		//! PER_THREAD_OUTPUT=false guarantees a single file; anything else means
		//! we somehow hit partitioned output or threaded output by accident.
		throw InternalException(
		    "iceberg_rewrite_data_files: COPY produced %llu files for a single rewrite group (expected 1)",
		    (unsigned long long)produced_files.size());
	}

	IcebergManifestEntry entry;
	entry.status = IcebergManifestEntryStatusType::ADDED;
	//! Pin data_sequence_number to the starting snapshot's seq so concurrently-
	//! written equality deletes keep applying (Iceberg V2 spec §4.2.4).
	//! Existing position deletes are materialized during the scan read above.
	entry.SetSequenceNumber(starting_sequence_number);
	//! Do NOT pin file_sequence_number: the spec says it "is always assigned at
	//! commit and cannot be provided explicitly" — it records the snapshot in
	//! which the file was *physically* added, which for a compaction output is
	//! the new REPLACE snapshot, NOT the starting one. Leaving it NULL on this
	//! ADDED entry lets it inherit the new manifest's seq on read (and the
	//! commit-time rewrite-forward in iceberg_add_snapshot resolves it then).
	//! Pinning it would falsely age the file and break incremental readers that
	//! key off file_sequence_number. Parity: Iceberg GenericManifestEntry.
	//! wrapAppend sets fileSequenceNumber=null; Trino finishOptimize sets only
	//! rewriteFiles.dataSequenceNumber(...).
	entry.data_file.content = IcebergManifestEntryContentType::DATA;
	entry.data_file.file_format = "parquet";
	entry.data_file.file_path = produced_files[0].GetValue<string>();
	entry.data_file.record_count = count_val.GetValue<int64_t>();
	//! SQL COPY doesn't surface file_size_bytes in this return shape; one HEAD
	//! request per produced file (cheap relative to the COPY itself) keeps the
	//! manifest entry honest about on-disk size for downstream readers.
	auto file_handle = fs.OpenFile(entry.data_file.file_path, FileFlags::FILE_FLAGS_READ);
	entry.data_file.file_size_in_bytes = static_cast<int64_t>(file_handle->GetFileSize());
	//! The planner buckets candidates so every file in one group shares the same
	//! partition values. Copy from candidate 0 instead of re-deriving from
	//! `partition_keys` — for unpartitioned tables COPY emits NULL there
	//! anyway, and for partitioned tables we'd just round-trip the same
	//! values that already live on the candidate.
	entry.data_file.partition_info = group.front().partition_info;
	//! NOTE: lower_bounds / upper_bounds / null_value_counts / column_sizes
	//! are intentionally left empty. Iceberg spec marks them optional
	//! and readers degrade gracefully (skip min/max pushdown for this file).
	//! Stats parsing is deferred to a follow-up — it requires the same
	//! ParseColumnStats + IcebergValue::SerializeValue machinery as
	//! IcebergInsertGlobalState::AddFiles and would double the size of this
	//! commit without changing commit semantics.
	return entry;
}

RewriteExecutionResult ExecuteRewrite(ClientContext &context, const RewritePlan &plan) {
	RewriteExecutionResult result;
	if (plan.table_is_empty || plan.file_groups.empty()) {
		return result;
	}
	if (!plan.table_info) {
		throw InternalException(
		    "iceberg_rewrite_data_files: RewritePlan.table_info is null but file_groups is non-empty");
	}
	auto &table_info = *plan.table_info;

	for (auto &group : plan.file_groups) {
		try {
			auto new_entry =
			    ExecuteOneGroup(context, table_info, plan.table_key, group, plan.starting_sequence_number);
			int64_t group_bytes = 0;
			for (auto &cand : group) {
				group_bytes += cand.file_size_in_bytes;
			}
			result.rewritten_data_files += static_cast<int64_t>(group.size());
			result.rewritten_bytes += group_bytes;
			result.added_data_files += 1;
			result.new_entries.push_back(std::move(new_entry));
			for (auto &cand : group) {
				result.rewritten_candidates.push_back(cand);
			}
		} catch (...) {
			//! Best-effort cleanup of prior successful groups' output files.
			//! The current failing group's file (if COPY produced one before the
			//! error) is NOT cleaned here — it becomes an orphan reclaimable by
			//! remove_orphan_files. This only covers files already in
			//! result.new_entries. Respects allows_deletes for catalogs (like
			//! S3 Tables) that reject client-side deletions.
			if (table_info.catalog.attach_options.allows_deletes) {
				auto &fs = FileSystem::GetFileSystem(context);
				for (auto &entry : result.new_entries) {
					fs.TryRemoveFile(entry.data_file.file_path);
				}
			}
			throw;
		}
	}
	return result;
}

} // namespace duckdb
