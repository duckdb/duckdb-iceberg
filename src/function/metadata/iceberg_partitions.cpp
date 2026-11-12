#include "function/iceberg_functions.hpp"
#include "core/metadata/partition/iceberg_partition_struct.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {

//! The totals of one partition
struct IcebergPartitionsRow {
	Value partition;
	int32_t spec_id = 0;
	int64_t record_count = 0;
	int32_t file_count = 0;
	int64_t total_data_file_size_in_bytes = 0;
	int64_t position_delete_record_count = 0;
	int32_t position_delete_file_count = 0;
	int64_t equality_delete_record_count = 0;
	int32_t equality_delete_file_count = 0;
	optional_ptr<const IcebergSnapshot> last_updated_snapshot;
};

struct IcebergPartitionsBindData : public TableFunctionData {
	IcebergPartitionsBindData() {};
	IcebergTableMetadata metadata {IcebergTableMetadataSchemas {}};

	IcebergOptions options;
	string table_location;
	IcebergSnapshotScanInfo snapshot_to_scan;
	vector<IcebergPartitionStructField> partition_fields;
};

//! Number of fields of the spec that are in the partition struct
static idx_t ProjectedFieldCount(const IcebergPartitionSpec &spec,
                                 const vector<IcebergPartitionStructField> &partition_fields) {
	idx_t count = 0;
	for (auto &spec_field : spec.fields) {
		for (auto &field : partition_fields) {
			if (field.field.get().partition_field_id == spec_field.partition_field_id) {
				count++;
				break;
			}
		}
	}
	return count;
}

static void UpdatePartition(IcebergPartitionsRow &row, const IcebergManifestFile &manifest_file,
                            const IcebergManifestEntry &entry, optional_ptr<const IcebergSnapshot> snapshot) {
	//! The spec and snapshot of the newest file
	if (snapshot && (!row.last_updated_snapshot || snapshot->timestamp_ms > row.last_updated_snapshot->timestamp_ms)) {
		row.spec_id = manifest_file.partition_spec_id;
		row.last_updated_snapshot = snapshot;
	}
	auto &data_file = entry.data_file;
	switch (data_file.content) {
	case IcebergManifestEntryContentType::DATA:
		row.record_count += data_file.record_count;
		row.file_count++;
		row.total_data_file_size_in_bytes += data_file.file_size_in_bytes;
		break;
	case IcebergManifestEntryContentType::POSITION_DELETES:
		row.position_delete_record_count += data_file.record_count;
		row.position_delete_file_count++;
		break;
	case IcebergManifestEntryContentType::EQUALITY_DELETES:
		row.equality_delete_record_count += data_file.record_count;
		row.equality_delete_file_count++;
		break;
	}
}

struct IcebergPartitionsGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<IcebergPartitionsBindData>();
		auto global_state = make_uniq<IcebergPartitionsGlobalState>();
		if (!bind_data.snapshot_to_scan.snapshot) {
			//! The table has no snapshot
			return std::move(global_state);
		}
		auto &metadata = bind_data.metadata;
		auto manifest_list = IcebergManifestList::Load(bind_data.table_location, metadata, bind_data.snapshot_to_scan,
		                                               context, bind_data.options);

		unordered_map<int32_t, idx_t> projected_fields;
		for (auto &spec : metadata.partition_specs) {
			projected_fields[spec.first] = ProjectedFieldCount(spec.second, bind_data.partition_fields);
		}

		//! Files of different specs are different partitions, even when their values are the same
		map<pair<idx_t, string>, idx_t> partition_index;
		for (auto &manifest : manifest_list->GetManifestFilesConst()) {
			auto &manifest_file = manifest.GetFile();
			auto field_count = projected_fields.at(manifest_file.partition_spec_id);
			for (auto &entry : manifest.GetManifestEntries()) {
				//! Only files that are live in this snapshot
				if (entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				auto partition = IcebergPartitionStruct::GetValue(bind_data.partition_fields, entry.data_file);
				auto key = make_pair(field_count, partition.ToSQLString());
				auto it = partition_index.find(key);
				if (it == partition_index.end()) {
					it = partition_index.emplace(std::move(key), global_state->rows.size()).first;
					global_state->rows.emplace_back();
					global_state->rows.back().partition = std::move(partition);
				}
				//! The snapshot can be missing if it has been expired
				auto snapshot = metadata.FindSnapshotByIdInternal(entry.GetSnapshotId(manifest_file));
				UpdatePartition(global_state->rows[it->second], manifest_file, entry, snapshot);
			}
		}
		return std::move(global_state);
	}

	vector<IcebergPartitionsRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> IcebergPartitionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<IcebergPartitionsBindData>();

	bind_data->options = IcebergOptions(input.named_parameters);
	auto input_string = input.inputs[0].ToString();
	auto resolved_metadata = IcebergUtils::ResolveTableMetadata(context, input_string, bind_data->options);
	bind_data->table_location = std::move(resolved_metadata.table_location);
	bind_data->metadata = std::move(resolved_metadata.metadata);
	bind_data->snapshot_to_scan = bind_data->metadata.GetSnapshot(*bind_data->options.snapshot_lookup);
	bind_data->partition_fields = IcebergPartitionStruct::GetFields(bind_data->metadata);

	//! Unpartitioned tables have no 'partition' and 'spec_id', as Iceberg does
	if (!bind_data->partition_fields.empty()) {
		names.emplace_back("partition");
		return_types.emplace_back(IcebergPartitionStruct::GetType(bind_data->partition_fields));

		names.emplace_back("spec_id");
		return_types.emplace_back(LogicalType::INTEGER);
	}

	names.emplace_back("record_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("file_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("total_data_file_size_in_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("position_delete_record_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("position_delete_file_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("equality_delete_record_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("equality_delete_file_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("last_updated_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_MS);

	names.emplace_back("last_updated_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(bind_data);
}

static void IcebergPartitionsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergPartitionsBindData>();
	auto &global_state = data.global_state->Cast<IcebergPartitionsGlobalState>();

	idx_t out = 0;
	while (global_state.offset < global_state.rows.size() && out < STANDARD_VECTOR_SIZE) {
		auto &row = global_state.rows[global_state.offset++];
		auto &snapshot = row.last_updated_snapshot;

		idx_t col = 0;
		if (!bind_data.partition_fields.empty()) {
			output.data[col++].SetValue(out, row.partition);
			output.data[col++].SetValue(out, Value::INTEGER(row.spec_id));
		}
		output.data[col++].SetValue(out, Value::BIGINT(row.record_count));
		output.data[col++].SetValue(out, Value::INTEGER(row.file_count));
		output.data[col++].SetValue(out, Value::BIGINT(row.total_data_file_size_in_bytes));
		output.data[col++].SetValue(out, Value::BIGINT(row.position_delete_record_count));
		output.data[col++].SetValue(out, Value::INTEGER(row.position_delete_file_count));
		output.data[col++].SetValue(out, Value::BIGINT(row.equality_delete_record_count));
		output.data[col++].SetValue(out, Value::INTEGER(row.equality_delete_file_count));
		output.data[col++].SetValue(out, snapshot ? Value::TIMESTAMPMS(snapshot->timestamp_ms)
		                                          : Value(LogicalType::TIMESTAMP_MS));
		output.data[col++].SetValue(out, snapshot ? Value::BIGINT(snapshot->snapshot_id) : Value(LogicalType::BIGINT));
		out++;
	}
	output.SetChildCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergPartitionsFunction() {
	TableFunctionSet function_set("iceberg_partitions");
	TableFunction table_function(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                             IcebergPartitionsFunction, IcebergPartitionsBind, IcebergPartitionsGlobalState::Init);

	table_function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("allow_moved_paths", LogicalType::BOOLEAN)
		    .Add("metadata_compression_codec", LogicalType::VARCHAR)
		    .Add("version", LogicalType::ANY)
		    .Add("version_name_format", LogicalType::VARCHAR)
		    .Add("snapshot_from_timestamp", LogicalType::ANY)
		    .Add("snapshot_from_id", LogicalType::UBIGINT);
	});
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
