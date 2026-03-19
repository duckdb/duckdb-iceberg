#include "execution/operator/copy/iceberg_copy.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"

#include "function/copy/iceberg_copy_function.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "common/iceberg_utils.hpp"
#include "core/expression/iceberg_value.hpp"

namespace duckdb {

void IcebergLogicalCopy::ResolveTypes() {
	types = {LogicalType::BIGINT};
}

PhysicalOperator &IcebergLogicalCopy::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);

	// Plan the child (the SELECT query)
	auto &child_plan = planner.CreatePlan(*children[0]);

	auto &copy_bind_data = bind_data->Cast<CopyIcebergBindData>();

	// Create IcebergCopyInput with the metadata from bind data
	IcebergCopyInput copy_input(context, *copy_bind_data.table_metadata, *copy_bind_data.table_schema);

	auto &fs = FileSystem::GetFileSystem(context);
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
	auto &table_metadata = *bind_data.table_metadata;

	// Get the avro copy function for writing manifest files
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &copy_fun = IcebergUtils::GetCopyFunction(context, "avro");

	int64_t next_row_id = 0;
	auto snapshot_id = IcebergSnapshot::NewSnapshotId();
	const auto sequence_number = 0;
	const auto first_row_id = next_row_id;

	auto &fs = FileSystem::GetFileSystem(context);
	auto metadata_path = table_metadata.GetMetadataPath(fs);
	if (!fs.IsRemoteFile(metadata_path)) {
		// create data path if it does not yet exist
		try {
			fs.CreateDirectoriesRecursive(metadata_path);
		} catch (...) {
		}
	}

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path =
	    fs.JoinPath(metadata_path, "snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro");

	auto manifest_file = IcebergManifestListEntry::CreateFromEntries(fs, snapshot_id, sequence_number, table_metadata,
	                                                                 IcebergManifestContentType::DATA,
	                                                                 std::move(written_files), next_row_id);

	// Create a snapshot from the written files
	IcebergSnapshot snapshot;
	snapshot.operation = IcebergSnapshotOperationType::APPEND;
	snapshot.snapshot_id = snapshot_id;
	snapshot.sequence_number = sequence_number;
	snapshot.schema_id = 0;
	snapshot.manifest_list = manifest_list_path;
	snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
	snapshot.has_parent_snapshot = false;

	snapshot.metrics.AddManifestFile(manifest_file.file);

	if (table_metadata.iceberg_version >= 3) {
		snapshot.has_first_row_id = true;
		snapshot.first_row_id = first_row_id;

		snapshot.has_added_rows = true;
		if (manifest_file.file.content == IcebergManifestContentType::DATA) {
			snapshot.added_rows = manifest_file.file.added_rows_count;
		} else {
			snapshot.added_rows = 0;
		}
	}

	// Write manifest file(s)
	manifest_file.file.manifest_length =
	    manifest_file::WriteToFile(table_metadata, manifest_file.file.manifest_path, manifest_file.manifest_entries,
	                               copy_fun.function, db, context);

	IcebergManifestList manifest_list(manifest_list_path);
	manifest_list.AddManifestFile(std::move(manifest_file));
	manifest_list::WriteToFile(table_metadata, manifest_list, copy_fun.function, db, context);

	// Update table metadata with snapshot
	table_metadata.current_snapshot_id = snapshot.snapshot_id;
	table_metadata.last_sequence_number = snapshot.sequence_number;
	table_metadata.last_updated_ms = snapshot.timestamp_ms;
	table_metadata.snapshots[0] = std::move(snapshot);

	auto version_hint = UUID::ToString(UUID::GenerateRandomUUID());

	// Write metadata.json
	auto metadata_file_path = fs.JoinPath(metadata_path, version_hint + ".metadata.json");
	table_metadata.WriteMetadata(context, metadata_file_path);

	// Write version-hint.text pointing to the latest metadata
	auto version_hint_path = fs.JoinPath(metadata_path, "version-hint.text");
	table_metadata.WriteVersionHint(context, version_hint_path, version_hint);
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

	if (!written_files.empty()) {
		// Write manifest files, manifest list, and metadata.json
		// This is where we differ from IcebergInsert - we write a complete metadata.json
		// instead of updating a catalog entry
		WriteIcebergMetadata(context, copy_bind_data, written_files);
	}

	return SinkFinalizeType::READY;
}

SourceResultType IcebergPhysicalCopy::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<IcebergInsertGlobalState>();
	auto value = Value::BIGINT(gstate.insert_count);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
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
