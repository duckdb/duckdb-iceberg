#include "execution/operator/copy/iceberg_copy.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/set.hpp"

#include "function/copy/iceberg_copy_function.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "common/iceberg_utils.hpp"
#include "core/expression/iceberg_value.hpp"

namespace duckdb {

void IcebergLogicalCopy::ResolveTypes() {
	types = {LogicalType::BIGINT};
}

//! Adds every file below 'directory' to 'result', recursively. A directory that does not exist has no files.
static void ListFilesRecursive(FileSystem &fs, const string &directory, set<string> &result) {
	if (!fs.DirectoryExists(directory)) {
		return;
	}
	vector<string> directories {directory};
	for (idx_t i = 0; i < directories.size(); i++) {
		auto current = directories[i];
		fs.ListFiles(current, [&](const string &name, bool is_directory) {
			auto full_path = fs.JoinPath(current, name);
			if (is_directory) {
				directories.push_back(std::move(full_path));
			} else {
				result.insert(std::move(full_path));
			}
		});
	}
}

static void ThrowTableExists(const CopyIcebergBindData &bind_data) {
	throw IOException("Cannot COPY to \"%s\": an Iceberg table already exists at this location, and COPY ... "
	                  "(FORMAT ICEBERG) only creates new tables",
	                  bind_data.file_path);
}

//! COPY creates a new table, so any file in the metadata directory means there is already a table at this
//! location, and committing would silently replace it.
static void CheckNoExistingTable(FileSystem &fs, const CopyIcebergBindData &bind_data) {
	set<string> existing_files;
	ListFilesRecursive(fs, bind_data.table_metadata->GetMetadataPath(fs), existing_files);
	if (!existing_files.empty()) {
		ThrowTableExists(bind_data);
	}
}

//! Best effort cleanup of the files of a COPY that could not commit
static void RemoveFilesBestEffort(FileSystem &fs, const vector<string> &files) {
	for (auto &file : files) {
		try {
			fs.RemoveFile(file);
		} catch (...) {
		}
	}
}

PhysicalOperator &IcebergLogicalCopy::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	// Plan the child (the SELECT query)
	auto &child_plan = planner.CreatePlan(*children[0]);

	auto &copy_bind_data = bind_data->Cast<CopyIcebergBindData>();

	auto &fs = FileSystem::GetFileSystem(context);
	// Fail before any data is written; WriteIcebergMetadata checks again before it commits
	CheckNoExistingTable(fs, copy_bind_data);

	// Create IcebergCopyInput with the metadata from bind data
	IcebergCopyInput copy_input(context, *copy_bind_data.table_metadata, *copy_bind_data.table_schema);
	copy_input.table_name = "local_iceberg_table";

	if (!fs.IsRemoteFile(copy_input.data_path)) {
		// create data path if it does not yet exist
		try {
			fs.CreateDirectoriesRecursive(copy_input.data_path);
		} catch (...) {
		}
	}

	// Create a parquet copy operator as the child
	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, &child_plan);

	// Create the IcebergPhysicalCopy operator and move bind_data to keep metadata alive
	auto &op = planner.Make<IcebergPhysicalCopy>(types, estimated_cardinality);
	auto &iceberg_copy = op.Cast<IcebergPhysicalCopy>();
	iceberg_copy.bind_data = std::move(bind_data);
	iceberg_copy.children.push_back(physical_copy);

	return op;
}

CopyIcebergLocalState::CopyIcebergLocalState(ClientContext &context) : LocalSinkState() {
}

unique_ptr<GlobalSinkState> IcebergPhysicalCopy::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergInsertGlobalState>(context);
}

unique_ptr<LocalSinkState> IcebergPhysicalCopy::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CopyIcebergLocalState>(context.client);
}

SinkResultType IcebergPhysicalCopy::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<IcebergInsertGlobalState>();
	auto &copy_bind_data = bind_data->Cast<CopyIcebergBindData>();

	gstate.AddFiles(chunk, "local_iceberg_table", *copy_bind_data.table_metadata);
	return SinkResultType::NEED_MORE_INPUT;
}

static void WriteIcebergMetadata(ClientContext &context, CopyIcebergBindData &bind_data,
                                 vector<IcebergManifestEntry> &written_files) {
	auto last_updated_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());

	auto &table_metadata = *bind_data.table_metadata;
	table_metadata.last_sequence_number = 0;
	table_metadata.last_updated_ms = last_updated_ms;

	auto &fs = FileSystem::GetFileSystem(context);
	auto metadata_path = table_metadata.GetMetadataPath(fs);

	// Everything this COPY writes, so it can be removed again if another COPY commits a table here first
	vector<string> files_written;
	for (auto &entry : written_files) {
		files_written.push_back(entry.data_file.file_path);
	}
	// A table may have been created at this location while the data files were being written
	try {
		CheckNoExistingTable(fs, bind_data);
	} catch (...) {
		RemoveFilesBestEffort(fs, files_written);
		throw;
	}

	if (!fs.IsRemoteFile(metadata_path)) {
		// create data path if it does not yet exist
		try {
			fs.CreateDirectoriesRecursive(metadata_path);
		} catch (...) {
		}
	}

	int64_t next_row_id = 0;
	if (!written_files.empty()) {
		// Get the avro copy function for writing manifest files
		auto &db = DatabaseInstance::GetDatabase(context);
		auto &copy_fun = IcebergUtils::GetCopyFunction(context, "avro");

		auto snapshot_id = IcebergSnapshot::NewSnapshotId();
		const auto sequence_number = table_metadata.last_sequence_number + 1;
		const auto first_row_id = next_row_id;

		//! Construct the manifest list
		auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
		auto manifest_list_path =
		    fs.JoinPath(metadata_path, "snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro");

		auto manifest_file = IcebergManifestListEntry::CreateFromEntries(
		    fs, sequence_number, table_metadata,
		    IcebergManifestMetadata::FromTableMetadata(table_metadata, IcebergManifestContentType::DATA),
		    std::move(written_files), next_row_id);

		// Create a snapshot from the written files
		IcebergSnapshot snapshot(0);
		snapshot.operation = IcebergSnapshotOperationType::APPEND;
		snapshot.snapshot_id = snapshot_id;
		snapshot.sequence_number = sequence_number;
		snapshot.manifest_list = manifest_list_path;
		snapshot.timestamp_ms = last_updated_ms;

		snapshot.metrics.AddManifestListEntry(manifest_file);

		if (table_metadata.iceberg_version >= 3) {
			snapshot.first_row_id = first_row_id;

			if (manifest_file.file.content == IcebergManifestContentType::DATA) {
				D_ASSERT(manifest_file.file.counts && manifest_file.file.counts->added_rows_count);
				snapshot.added_rows = *manifest_file.file.counts->added_rows_count;
			} else {
				snapshot.added_rows = 0;
			}
		}

		// Write manifest file(s)
		files_written.push_back(manifest_file.file.manifest_path);
		manifest_file.file.manifest_length =
		    manifest_file::WriteToFile(table_metadata, manifest_file, copy_fun.function, db, context);

		IcebergManifestList manifest_list(snapshot_id, sequence_number, manifest_list_path);
		manifest_list.AddNewManifestFile(std::move(manifest_file));
		files_written.push_back(manifest_list_path);
		manifest_list::WriteToFile(table_metadata, manifest_list, copy_fun.function, db, context);

		// Update table metadata with snapshot
		table_metadata.current_snapshot_id = snapshot.snapshot_id;
		table_metadata.last_sequence_number = sequence_number;
		table_metadata.snapshots.emplace(0, std::move(snapshot));
	}
	if (table_metadata.iceberg_version >= 3) {
		// Required since v3: higher than every row id assigned so far, [0, next_row_id) went to this snapshot
		table_metadata.next_row_id = next_row_id;
	}
	auto version_hint = UUID::ToString(UUID::GenerateRandomUUID());

	// Write metadata.json
	auto metadata_file_path = fs.JoinPath(metadata_path, version_hint + ".metadata.json");
	files_written.push_back(metadata_file_path);
	table_metadata.WriteMetadata(context, metadata_file_path);

	// Write version-hint.text pointing to the latest metadata. This commits the table: the hint must not exist yet,
	// so if another COPY to this location committed in the meantime, this one fails.
	auto version_hint_path = fs.JoinPath(metadata_path, "version-hint.text");
	if (!table_metadata.WriteVersionHint(context, version_hint_path, version_hint)) {
		RemoveFilesBestEffort(fs, files_written);
		ThrowTableExists(bind_data);
	}
}

SinkFinalizeType IcebergPhysicalCopy::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<IcebergInsertGlobalState>();
	auto &copy_bind_data = bind_data->Cast<CopyIcebergBindData>();

	vector<IcebergManifestEntry> written_files;
	{
		lock_guard<mutex> guard(gstate.lock);
		written_files = std::move(gstate.written_files);
	}

	// Write manifest files, manifest list, and metadata.json
	// This is where we differ from IcebergInsert - we write a complete metadata.json
	// instead of updating a catalog entry
	WriteIcebergMetadata(context, copy_bind_data, written_files);

	return SinkFinalizeType::READY;
}

SourceResultType IcebergPhysicalCopy::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<IcebergInsertGlobalState>();
	auto value = Value::BIGINT(gstate.insert_count);
	chunk.data[0].Append(value);
	return SourceResultType::FINISHED;
}

string IcebergPhysicalCopy::GetName() const {
	return "ICEBERG_COPY";
}

InsertionOrderPreservingMap<string> IcebergPhysicalCopy::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	auto &copy_bind_data = bind_data->Cast<CopyIcebergBindData>();
	result["File Path"] = copy_bind_data.file_path;
	return result;
}

} // namespace duckdb
