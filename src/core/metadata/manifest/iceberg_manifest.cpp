#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/manifest/iceberg_avro_codec.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "catalog/rest/iceberg_table_set.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "core/expression/iceberg_value.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"

#include <optional>

namespace duckdb {

namespace {

using std::optional;

using IntIntMapWriter = VectorWriter<VectorListType<VectorStructType<int32_t, int64_t>>>;
using IntStringMapWriter = VectorWriter<VectorListType<VectorStructType<int32_t, string_t>>>;
using Int32ListWriter = VectorWriter<VectorListType<int32_t>>;
using Int64ListWriter = VectorWriter<VectorListType<int64_t>>;

template <class MAP>
static void WriteIntIntMap(IntIntMapWriter &writer, const MAP &map);
static void WriteBoundsMap(IntStringMapWriter &writer, const unordered_map<int32_t, Value> &bounds);
static void WriteInt32List(Int32ListWriter &writer, const vector<int32_t> &values);
static void WriteInt64List(Int64ListWriter &writer, const vector<int64_t> &values);
static void WritePartitionStructRow(Vector &partition_vector, idx_t row_idx, const IcebergDataFile &data_file,
                                    const IcebergTableMetadata &table_metadata,
                                    const vector<IcebergExtendedPartitionInfo> &schema_partition_info);
static void PopulateSourceIdToTypeMap(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                      unordered_map<uint64_t, const LogicalType *> &source_id_to_type) {
	for (auto &col : columns) {
		source_id_to_type.emplace(static_cast<uint64_t>(col->id), &col->type);
		PopulateSourceIdToTypeMap(col->GetChildren(), source_id_to_type);
	}
}

struct DataFileVectorWriters {
	explicit DataFileVectorWriters(Vector &data_file_vector, idx_t row_count,
	                               const IcebergTableMetadata &table_metadata, int32_t manifest_format_version)
	    : table_metadata(table_metadata), data_file_entries(StructVector::GetEntries(data_file_vector)),
	      file_path(data_file_entries[FILE_PATH_INDEX], row_count, 0),
	      file_format(data_file_entries[FILE_FORMAT_INDEX], row_count, 0),
	      partition(data_file_entries[PARTITION_INDEX]),
	      record_count(data_file_entries[RECORD_COUNT_INDEX], row_count, 0),
	      file_size_in_bytes(data_file_entries[FILE_SIZE_IN_BYTES_INDEX], row_count, 0),
	      column_sizes(data_file_entries[COLUMN_SIZES_INDEX], row_count, 0),
	      value_counts(data_file_entries[VALUE_COUNTS_INDEX], row_count, 0),
	      null_value_counts(data_file_entries[NULL_VALUE_COUNTS_INDEX], row_count, 0),
	      nan_value_counts(data_file_entries[NAN_VALUE_COUNTS_INDEX], row_count, 0),
	      lower_bounds(data_file_entries[LOWER_BOUNDS_INDEX], row_count, 0),
	      upper_bounds(data_file_entries[UPPER_BOUNDS_INDEX], row_count, 0),
	      split_offsets(data_file_entries[SPLIT_OFFSETS_INDEX], row_count, 0),
	      equality_ids(data_file_entries[EQUALITY_IDS_INDEX], row_count, 0),
	      sort_order_id(data_file_entries[SORT_ORDER_ID_INDEX], row_count, 0) {
		if (manifest_format_version >= 2) {
			content.emplace(data_file_entries[CONTENT_INDEX], row_count, 0);
			referenced_data_file.emplace(data_file_entries[REFERENCED_DATA_FILE_INDEX], row_count, 0);
		}
		if (manifest_format_version >= 3) {
			first_row_id.emplace(data_file_entries[FIRST_ROW_ID_INDEX], row_count, 0);
			content_offset.emplace(data_file_entries[CONTENT_OFFSET_INDEX], row_count, 0);
			content_size_in_bytes.emplace(data_file_entries[CONTENT_SIZE_IN_BYTES_INDEX], row_count, 0);
		}
	}

	void WriteRow(idx_t row_idx, const IcebergDataFile &data_file,
	              const vector<IcebergExtendedPartitionInfo> &schema_partition_info) {
		if (content) {
			content->WriteValue(static_cast<int32_t>(data_file.content));
		}

		file_path.WriteValue(string_t(data_file.file_path));
		file_format.WriteValue(string_t(data_file.file_format));

		WritePartitionStructRow(partition, row_idx, data_file, table_metadata, schema_partition_info);

		record_count.WriteValue(data_file.record_count);
		file_size_in_bytes.WriteValue(data_file.file_size_in_bytes);

		WriteIntIntMap(column_sizes, data_file.column_sizes);
		WriteIntIntMap(value_counts, data_file.value_counts);
		WriteIntIntMap(null_value_counts, data_file.null_value_counts);
		WriteIntIntMap(nan_value_counts, data_file.nan_value_counts);

		WriteBoundsMap(lower_bounds, data_file.lower_bounds);
		WriteBoundsMap(upper_bounds, data_file.upper_bounds);

		if (data_file.split_offsets.empty()) {
			split_offsets.WriteNull();
		} else {
			WriteInt64List(split_offsets, data_file.split_offsets);
		}

		if (data_file.equality_ids.empty()) {
			equality_ids.WriteNull();
		} else {
			WriteInt32List(equality_ids, data_file.equality_ids);
		}

		if (data_file.sort_order_id) {
			sort_order_id.WriteValue(*data_file.sort_order_id);
		} else {
			sort_order_id.WriteNull();
		}

		if (first_row_id) {
			if (data_file.HasFirstRowId()) {
				first_row_id->WriteValue(data_file.GetFirstRowId());
			} else {
				first_row_id->WriteNull();
			}
		}

		if (referenced_data_file) {
			if (!data_file.referenced_data_file) {
				referenced_data_file->WriteNull();
			} else {
				referenced_data_file->WriteValue(string_t(*data_file.referenced_data_file));
			}
		}

		if (content_offset) {
			if (!data_file.content_offset) {
				content_offset->WriteNull();
			} else {
				content_offset->WriteValue(*data_file.content_offset);
			}
		}

		if (content_size_in_bytes) {
			if (!data_file.content_size_in_bytes) {
				content_size_in_bytes->WriteNull();
			} else {
				content_size_in_bytes->WriteValue(*data_file.content_size_in_bytes);
			}
		}
	}

private:
	static constexpr idx_t FILE_PATH_INDEX = 0;
	static constexpr idx_t FILE_FORMAT_INDEX = 1;
	static constexpr idx_t PARTITION_INDEX = 2;
	static constexpr idx_t RECORD_COUNT_INDEX = 3;
	static constexpr idx_t FILE_SIZE_IN_BYTES_INDEX = 4;
	static constexpr idx_t COLUMN_SIZES_INDEX = 5;
	static constexpr idx_t VALUE_COUNTS_INDEX = 6;
	static constexpr idx_t NULL_VALUE_COUNTS_INDEX = 7;
	static constexpr idx_t NAN_VALUE_COUNTS_INDEX = 8;
	static constexpr idx_t LOWER_BOUNDS_INDEX = 9;
	static constexpr idx_t UPPER_BOUNDS_INDEX = 10;
	static constexpr idx_t SPLIT_OFFSETS_INDEX = 11;
	static constexpr idx_t EQUALITY_IDS_INDEX = 12;
	static constexpr idx_t SORT_ORDER_ID_INDEX = 13;
	static constexpr idx_t CONTENT_INDEX = 14;
	static constexpr idx_t REFERENCED_DATA_FILE_INDEX = 15;
	static constexpr idx_t FIRST_ROW_ID_INDEX = 16;
	static constexpr idx_t CONTENT_OFFSET_INDEX = 17;
	static constexpr idx_t CONTENT_SIZE_IN_BYTES_INDEX = 18;

public:
	const IcebergTableMetadata &table_metadata;
	vector<Vector> &data_file_entries;
	VectorWriter<string_t> file_path;
	VectorWriter<string_t> file_format;
	Vector &partition;
	VectorWriter<int64_t> record_count;
	VectorWriter<int64_t> file_size_in_bytes;
	IntIntMapWriter column_sizes;
	IntIntMapWriter value_counts;
	IntIntMapWriter null_value_counts;
	IntIntMapWriter nan_value_counts;
	IntStringMapWriter lower_bounds;
	IntStringMapWriter upper_bounds;
	Int64ListWriter split_offsets;
	Int32ListWriter equality_ids;
	VectorWriter<int32_t> sort_order_id;
	optional<VectorWriter<int32_t>> content;
	optional<VectorWriter<string_t>> referenced_data_file;
	optional<VectorWriter<int64_t>> first_row_id;
	optional<VectorWriter<int64_t>> content_offset;
	optional<VectorWriter<int64_t>> content_size_in_bytes;
};

} // namespace

string IcebergManifestEntryContentTypeToString(IcebergManifestEntryContentType type) {
	switch (type) {
	case IcebergManifestEntryContentType::DATA:
		return "DATA";
	case IcebergManifestEntryContentType::POSITION_DELETES:
		return "POSITION_DELETES";
	case IcebergManifestEntryContentType::EQUALITY_DELETES:
		return "EQUALITY_DELETES";
	default:
		throw InvalidConfigurationException("Invalid Manifest Entry Content Type");
	}
}

string IcebergManifestEntryStatusTypeToString(IcebergManifestEntryStatusType type) {
	switch (type) {
	case IcebergManifestEntryStatusType::EXISTING:
		return "EXISTING";
	case IcebergManifestEntryStatusType::ADDED:
		return "ADDED";
	case IcebergManifestEntryStatusType::DELETED:
		return "DELETED";
	default:
		throw InvalidConfigurationException("Invalid matifest entry type");
	}
}

map<idx_t, LogicalType> IcebergDataFile::GetFieldIdToTypeMapping(const IcebergSnapshotScanInfo &snapshot_info,
                                                                 const IcebergTableMetadata &metadata,
                                                                 const unordered_set<int32_t> &partition_spec_ids) {
	D_ASSERT(!partition_spec_ids.empty());
	auto &partition_specs = metadata.GetPartitionSpecs();
	auto &schema = *metadata.GetSchemaFromId(snapshot_info.schema_id);

	auto &source_to_column_id = schema.GetSourceIdMap();
	map<idx_t, LogicalType> partition_field_id_to_type;
	for (auto &spec_id : partition_spec_ids) {
		auto &partition_spec = partition_specs.at(spec_id);
		auto &fields = partition_spec.GetFields();

		for (auto &field : fields) {
			auto it = source_to_column_id.find(field.source_id);
			if (it == source_to_column_id.end()) {
				//! FIXME: is this correct?
				//! The column doesn't exist (anymore) in the schema we're scanning
				//! So this essentially excludes these partition values from the scan
				continue;
			}
			auto &column_id = it->second;
			auto &column = IcebergTableSchema::GetFromColumnIndex(schema.columns, column_id, 0);
			partition_field_id_to_type.emplace(field.partition_field_id, field.transform.GetBoundsType(column.type));
		}
	}
	return partition_field_id_to_type;
}

LogicalType IcebergDataFile::PartitionStructType(const map<idx_t, LogicalType> &partition_field_id_to_type) {
	child_list_t<LogicalType> children;
	if (partition_field_id_to_type.empty()) {
		return LogicalType::SQLNULL;
	} else {
		for (auto &it : partition_field_id_to_type) {
			children.emplace_back(StringUtil::Format("r%d", it.first), it.second);
		}
	}
	return LogicalType::STRUCT(children);
}

const vector<IcebergExtendedPartitionInfo>
IcebergDataFile::GetExtendedPartitionInfo(const IcebergTableMetadata &metadata) const {
	if (partition_info.empty()) {
		return {};
	}

	// Build source_id -> LogicalType map from all schemas (schema evolution may spread columns).
	unordered_map<uint64_t, const LogicalType *> source_id_to_type;
	for (auto &schema_pair : metadata.GetSchemas()) {
		PopulateSourceIdToTypeMap(schema_pair.second->columns, source_id_to_type);
	}

	// Build field_id -> (spec field, source_type) map from all partition specs.
	// Partition field ids are globally unique across all specs per the Iceberg spec.
	struct ParitionFieldWithSourceType {
		const IcebergPartitionSpecField *field;
		const LogicalType *source_type;
	};

	unordered_map<uint64_t, ParitionFieldWithSourceType> field_id_to_partition_spec_and_source_type;
	for (auto &spec_pair : metadata.partition_specs) {
		for (auto &field : spec_pair.second.fields) {
			auto type_it = source_id_to_type.find(field.source_id);
			if (type_it == source_id_to_type.end()) {
				throw InternalException(
				    "Partition %s with field_id %llu in data_file %s with source_id %llu not found in any table schema",
				    field.GetPartitionSpecFieldName(), field.partition_field_id, file_path, field.source_id);
			}
			field_id_to_partition_spec_and_source_type.emplace(field.partition_field_id,
			                                                   ParitionFieldWithSourceType {&field, type_it->second});
		}
	}

	vector<IcebergExtendedPartitionInfo> ret;
	ret.reserve(partition_info.size());
	for (auto &info : partition_info) {
		auto it = field_id_to_partition_spec_and_source_type.find(info.field_id);
		if (it == field_id_to_partition_spec_and_source_type.end()) {
			throw InternalException("Partition field_id %llu not found in any partition spec", info.field_id);
		}
		auto &resolved = it->second;
		IcebergExtendedPartitionInfo extended;
		extended.name = resolved.field->GetPartitionSpecFieldName();
		extended.field_id = info.field_id;
		extended.value = info.value;
		extended.source_id = resolved.field->source_id;
		extended.transform = resolved.field->transform;
		D_ASSERT(resolved.source_type);
		extended.source_type = *resolved.source_type;
		ret.push_back(std::move(extended));
	}
	return ret;
}

void IcebergDataFile::SetFirstRowId(optional<int64_t> value) {
	first_row_id = value;
}

bool IcebergDataFile::HasFirstRowId() const {
	return !!first_row_id;
}

int64_t IcebergDataFile::GetFirstRowId() const {
	D_ASSERT(HasFirstRowId());
	return *first_row_id;
}

LogicalType IcebergDataFile::GetType(const IcebergTableMetadata &metadata, const LogicalType &partition_type) {
	auto &iceberg_version = metadata.iceberg_version;

	// lower/upper bounds
	child_list_t<LogicalType> bounds_fields;
	bounds_fields.emplace_back("key", LogicalType::INTEGER);
	bounds_fields.emplace_back("value", LogicalType::BLOB);

	// null_value_counts
	child_list_t<LogicalType> null_value_counts_fields;
	null_value_counts_fields.emplace_back("key", LogicalType::INTEGER);
	null_value_counts_fields.emplace_back("value", LogicalType::BIGINT);

	child_list_t<LogicalType> children;

	// file_path: string
	children.emplace_back("file_path", LogicalType::VARCHAR);
	// file_format: string
	children.emplace_back("file_format", LogicalType::VARCHAR);
	// partition: struct(...)
	children.emplace_back("partition", partition_type);
	// record_count: long
	children.emplace_back("record_count", LogicalType::BIGINT);
	// file_size_in_bytes: long
	children.emplace_back("file_size_in_bytes", LogicalType::BIGINT);
	// column_sizes: map<int, binary>
	children.emplace_back("column_sizes", LogicalType::MAP(LogicalType::STRUCT(null_value_counts_fields)));
	// value_counts: map<int, binary>
	children.emplace_back("value_counts", LogicalType::MAP(LogicalType::STRUCT(null_value_counts_fields)));
	// null_value_counts: map<int, binary>
	children.emplace_back("null_value_counts", LogicalType::MAP(LogicalType::STRUCT(null_value_counts_fields)));
	// nan_value_counts: map<int, binary>
	children.emplace_back("nan_value_counts", LogicalType::MAP(LogicalType::STRUCT(null_value_counts_fields)));
	// lower bounds: map<int, binary>
	children.emplace_back("lower_bounds", LogicalType::MAP(LogicalType::STRUCT(bounds_fields)));
	// upper bounds: map<int, binary>
	children.emplace_back("upper_bounds", LogicalType::MAP(LogicalType::STRUCT(bounds_fields)));
	// split_offsets: list<long>
	children.emplace_back("split_offsets", LogicalType::LIST(LogicalType::BIGINT));
	// equality_ids: list<int>
	children.emplace_back("equality_ids", LogicalType::LIST(LogicalType::INTEGER));
	// sort_id: int
	children.emplace_back("sort_order_id", LogicalType::INTEGER);

	// v2-only suffix
	if (iceberg_version >= 2) {
		// content: int
		children.emplace_back("content", LogicalType::INTEGER);
		// referenced_data_file: string
		children.emplace_back("referenced_data_file", LogicalType::VARCHAR);
	}

	// v3-only suffix
	if (iceberg_version >= 3) {
		// first_row_id: long
		children.emplace_back("first_row_id", LogicalType::BIGINT);
		// content_offset: long
		children.emplace_back("content_offset", LogicalType::BIGINT);
		// content_size_in_bytes: long
		children.emplace_back("content_size_in_bytes", LogicalType::BIGINT);
	}

	return LogicalType::STRUCT(std::move(children));
}

void IcebergManifestEntry::SetSequenceNumber(optional<sequence_number_t> value) {
	sequence_number = value;
}

void IcebergManifestEntry::SetFileSequenceNumber(optional<sequence_number_t> value) {
	file_sequence_number = value;
}

optional<sequence_number_t> IcebergManifestEntry::ExplicitSequenceNumber() const {
	return sequence_number;
}

optional<sequence_number_t> IcebergManifestEntry::ExplicitFileSequenceNumber() const {
	return file_sequence_number;
}

sequence_number_t IcebergManifestEntry::GetSequenceNumber(const IcebergManifestFile &manifest_file) const {
	if (!sequence_number) {
		if (status != IcebergManifestEntryStatusType::ADDED) {
			throw InvalidConfigurationException(
			    "'manifest_entry.sequence_number' is only allowed to be NULL for ADDED entries");
		}
		if (!manifest_file.sequence_number) {
			throw InvalidConfigurationException("'manifest_file.sequence_number' is not set");
		}
		return *manifest_file.sequence_number;
	}
	return *sequence_number;
}

sequence_number_t IcebergManifestEntry::GetFileSequenceNumber(const IcebergManifestFile &manifest_file) const {
	if (!file_sequence_number) {
		if (status != IcebergManifestEntryStatusType::ADDED) {
			throw InvalidConfigurationException(
			    "'manifest_entry.file_sequence_number' is only allowed to be NULL for ADDED entries");
		}
		if (!manifest_file.sequence_number) {
			throw InvalidConfigurationException("'manifest_file.sequence_number' is not set");
		}
		return *manifest_file.sequence_number;
	}
	return *file_sequence_number;
}

void IcebergManifestEntry::SetSnapshotId(optional<int64_t> value) {
	snapshot_id = value;
}

bool IcebergManifestEntry::HasSnapshotId() const {
	return !!snapshot_id;
}

int64_t IcebergManifestEntry::GetSnapshotId() const {
	D_ASSERT(HasSnapshotId());
	return *snapshot_id;
}

static Value CreateFieldID(int32_t field_id, bool nullable) {
	child_list_t<Value> fields;
	fields.emplace_back("__duckdb_field_id", Value::INTEGER(field_id));
	fields.emplace_back("__duckdb_nullable", Value::BOOLEAN(nullable));
	return Value::STRUCT(fields);
}

namespace {

template <class MAP>
static void WriteIntIntMap(IntIntMapWriter &writer, const MAP &map) {
	auto list = writer.WriteList(map.size());
	auto it = map.begin();
	for (auto &entry_writer : list) {
		auto &entry = *it++;
		entry_writer.WriteValue([&](auto &key_writer, auto &value_writer) {
			key_writer.WriteValue(entry.first);
			value_writer.WriteValue(entry.second);
		});
	}
}

static void WriteBoundsMap(IntStringMapWriter &writer, const unordered_map<int32_t, Value> &bounds) {
	auto list = writer.WriteList(bounds.size());
	auto it = bounds.begin();
	for (auto &entry_writer : list) {
		auto &entry = *it++;
		entry_writer.WriteValue([&](auto &key_writer, auto &value_writer) {
			key_writer.WriteValue(entry.first);
			if (entry.second.IsNull()) {
				value_writer.WriteNull();
			} else {
				value_writer.WriteValue(entry.second.GetValueUnsafe<string_t>());
			}
		});
	}
}

static void WriteInt32List(Int32ListWriter &writer, const vector<int32_t> &values) {
	auto list = writer.WriteList(values.size());
	auto it = values.begin();
	for (auto &entry_writer : list) {
		entry_writer.WriteValue(*it++);
	}
}

static void WriteInt64List(Int64ListWriter &writer, const vector<int64_t> &values) {
	auto list = writer.WriteList(values.size());
	auto it = values.begin();
	for (auto &entry_writer : list) {
		entry_writer.WriteValue(*it++);
	}
}

static void WritePartitionValue(Vector &vector, idx_t row_idx, const Value &value) {
	if (value.IsNull()) {
		FlatVector::SetNull(vector, row_idx, true);
		return;
	}
	Value cast_value;
	string error_message;
	if (!value.DefaultTryCastAs(vector.GetType(), cast_value, &error_message, true)) {
		throw InvalidInputException("Could not cast partition value %s to %s", value.type().ToString(),
		                            vector.GetType().ToString());
	}
	vector.SetValue(row_idx, cast_value);
}

static void WritePartitionStructRow(Vector &partition_vector, idx_t row_idx, const IcebergDataFile &data_file,
                                    const IcebergTableMetadata &table_metadata,
                                    const vector<IcebergExtendedPartitionInfo> &schema_partition_info) {
	auto &partition_children = StructVector::GetEntries(partition_vector);
	if (schema_partition_info.empty()) {
		D_ASSERT(!partition_children.empty());
		FlatVector::SetNull(partition_children[0], row_idx, true);
		return;
	}

	auto extended_partition_info = data_file.GetExtendedPartitionInfo(table_metadata);
	unordered_map<uint64_t, const Value *> value_by_field_id;
	for (auto &entry : extended_partition_info) {
		value_by_field_id.emplace(entry.field_id, &entry.value);
	}
	for (idx_t child_idx = 0; child_idx < schema_partition_info.size(); child_idx++) {
		auto &schema_entry = schema_partition_info[child_idx];
		auto value_it = value_by_field_id.find(schema_entry.field_id);
		if (value_it == value_by_field_id.end()) {
			FlatVector::SetNull(partition_children[child_idx], row_idx, true);
			continue;
		}
		WritePartitionValue(partition_children[child_idx], row_idx, *value_it->second);
	}
}

} // namespace

namespace manifest_file {

static LogicalType PartitionStructType(vector<IcebergExtendedPartitionInfo> extended_partition_info) {
	child_list_t<LogicalType> children;
	if (extended_partition_info.empty()) {
		children.emplace_back("__duckdb_empty_struct_marker", LogicalType::INTEGER);
	} else {
		//! NOTE: all entries in the file should have the same schema, otherwise it can't be in the same manifest file
		//! anyways
		for (auto &entry : extended_partition_info) {
			switch (entry.transform.Type()) {
			case IcebergTransformType::TRUNCATE:
			case IcebergTransformType::IDENTITY:
				children.emplace_back(entry.name, entry.source_type);
				break;
			case IcebergTransformType::BUCKET:
			case IcebergTransformType::DAY:
			case IcebergTransformType::MONTH:
			case IcebergTransformType::YEAR:
			case IcebergTransformType::HOUR:
				children.emplace_back(entry.name, LogicalType::INTEGER);
				break;
			case IcebergTransformType::INVALID:
			case IcebergTransformType::VOID:
				throw InvalidInputException("Cannot use this transform type");
				break;
			default:
				throw InvalidInputException("Unrecognized transform");
			}
		}
	}
	return LogicalType::STRUCT(children);
}

static Value FieldIdsForMap(int32_t map_field_id, int32_t map_key_field_id, int32_t map_value_field_id) {
	child_list_t<Value> members;
	members.emplace_back("__duckdb_field_id", Value::INTEGER(map_field_id));

	child_list_t<Value> map_key_members;
	map_key_members.emplace_back("__duckdb_field_id", Value::INTEGER(map_key_field_id));
	map_key_members.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
	members.emplace_back("key", Value::STRUCT(map_key_members));

	child_list_t<Value> map_value_members;
	map_value_members.emplace_back("__duckdb_field_id", Value::INTEGER(map_value_field_id));
	map_value_members.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
	members.emplace_back("value", Value::STRUCT(map_value_members));
	return Value::STRUCT(members);
}

static Value FieldIdsForList(int32_t list_field_id, int32_t element_field_id) {
	child_list_t<Value> members;
	members.emplace_back("list", CreateFieldID(element_field_id, false));
	members.emplace_back("__duckdb_field_id", Value::INTEGER(list_field_id));
	return Value::STRUCT(members);
}

idx_t WriteToFile(const IcebergTableMetadata &table_metadata, const IcebergManifestListEntry &manifest_entry,
                  CopyFunction &copy, DatabaseInstance &db, ClientContext &context) {
	auto &manifest_file = manifest_entry.file;
	if (!manifest_entry.manifest_metadata) {
		throw InternalException("Manifest entry for '%s' is missing typed manifest metadata",
		                        manifest_file.manifest_path);
	}
	auto &manifest_entries = manifest_entry.GetManifestEntries();
	auto &entry_metadata = *manifest_entry.manifest_metadata;
	auto manifest_metadata = GetManifestMetadataMap(table_metadata, entry_metadata);
	auto manifest_format_version = entry_metadata.format_version;
	D_ASSERT(!manifest_entries.empty());
	auto &allocator = db.GetBufferManager().GetBufferAllocator();
	auto &path = manifest_file.manifest_path;

	//! Create the types for the DataChunk

	child_list_t<Value> field_ids;
	vector<Identifier> names;
	vector<LogicalType> types;

	// status: int
	names.push_back("status");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("status", CreateFieldID(STATUS, false));

	// snapshot_id: long
	names.push_back("snapshot_id");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("snapshot_id", Value::INTEGER(SNAPSHOT_ID));

	// sequence_number: long
	names.push_back("sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("sequence_number", Value::INTEGER(SEQUENCE_NUMBER));

	// file_sequence_number: long
	names.push_back("file_sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("file_sequence_number", Value::INTEGER(FILE_SEQUENCE_NUMBER));

	//! DataFile struct

	child_list_t<Value> data_file_field_ids;
	child_list_t<LogicalType> children;

	// file_path: string
	children.emplace_back("file_path", LogicalType::VARCHAR);
	data_file_field_ids.emplace_back("file_path", CreateFieldID(FILE_PATH, false));

	// file_format: string
	children.emplace_back("file_format", LogicalType::VARCHAR);
	data_file_field_ids.emplace_back("file_format", CreateFieldID(FILE_FORMAT, false));

	auto &first_entry = manifest_entries.front();
	auto &data_file = first_entry.data_file;

	auto extended_partition_info = data_file.GetExtendedPartitionInfo(table_metadata);
	child_list_t<Value> partition;
	// partition: struct(...)
	children.emplace_back("partition", PartitionStructType(extended_partition_info));
	partition.emplace_back("__duckdb_field_id", Value::INTEGER(PARTITION));
	partition.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
	for (auto &entry : extended_partition_info) {
		partition.emplace_back(entry.name, Value::INTEGER(static_cast<int32_t>(entry.field_id)));
	}
	data_file_field_ids.emplace_back("partition", Value::STRUCT(partition));

	// record_count: long
	children.emplace_back("record_count", LogicalType::BIGINT);
	data_file_field_ids.emplace_back("record_count", CreateFieldID(RECORD_COUNT, false));

	// file_size_in_bytes: long
	children.emplace_back("file_size_in_bytes", LogicalType::BIGINT);
	data_file_field_ids.emplace_back("file_size_in_bytes", CreateFieldID(FILE_SIZE_IN_BYTES, false));

	child_list_t<LogicalType> map_int_long_fields;
	map_int_long_fields.emplace_back("key", LogicalType::INTEGER);
	map_int_long_fields.emplace_back("value", LogicalType::BIGINT);

	// column_sizes: map<int, long>
	children.emplace_back("column_sizes", LogicalType::MAP(LogicalType::STRUCT(map_int_long_fields)));
	data_file_field_ids.emplace_back("column_sizes",
	                                 FieldIdsForMap(COLUMN_SIZES, COLUMN_SIZES_KEY, COLUMN_SIZES_VALUE));

	// value_counts: map<int, long>
	children.emplace_back("value_counts", LogicalType::MAP(LogicalType::STRUCT(map_int_long_fields)));
	data_file_field_ids.emplace_back("value_counts",
	                                 FieldIdsForMap(VALUE_COUNTS, VALUE_COUNTS_KEY, VALUE_COUNTS_VALUE));

	// null_value_counts: map<int, long>
	children.emplace_back("null_value_counts", LogicalType::MAP(LogicalType::STRUCT(map_int_long_fields)));
	data_file_field_ids.emplace_back("null_value_counts",
	                                 FieldIdsForMap(NULL_VALUE_COUNTS, NULL_VALUE_COUNTS_KEY, NULL_VALUE_COUNTS_VALUE));

	// nan_value_counts: map<int, long>
	children.emplace_back("nan_value_counts", LogicalType::MAP(LogicalType::STRUCT(map_int_long_fields)));
	data_file_field_ids.emplace_back("nan_value_counts",
	                                 FieldIdsForMap(NAN_VALUE_COUNTS, NAN_VALUE_COUNTS_KEY, NAN_VALUE_COUNTS_VALUE));

	child_list_t<LogicalType> bounds_fields;
	bounds_fields.emplace_back("key", LogicalType::INTEGER);
	bounds_fields.emplace_back("value", LogicalType::BLOB);

	// lower bounds: map<int, binary>
	children.emplace_back("lower_bounds", LogicalType::MAP(LogicalType::STRUCT(bounds_fields)));
	data_file_field_ids.emplace_back("lower_bounds",
	                                 FieldIdsForMap(LOWER_BOUNDS, LOWER_BOUNDS_KEY, LOWER_BOUNDS_VALUE));

	// upper bounds: map<int, binary>
	children.emplace_back("upper_bounds", LogicalType::MAP(LogicalType::STRUCT(bounds_fields)));
	data_file_field_ids.emplace_back("upper_bounds",
	                                 FieldIdsForMap(UPPER_BOUNDS, UPPER_BOUNDS_KEY, UPPER_BOUNDS_VALUE));

	// split_offsets: list<long>
	children.emplace_back("split_offsets", LogicalType::LIST(LogicalType::BIGINT));
	data_file_field_ids.emplace_back("split_offsets", FieldIdsForList(SPLIT_OFFSETS, SPLIT_OFFSETS_ELEMENT));

	// equality_ids: list<int> - the field-ids an equality-delete file applies to
	children.emplace_back("equality_ids", LogicalType::LIST(LogicalType::INTEGER));
	data_file_field_ids.emplace_back("equality_ids", FieldIdsForList(EQUALITY_IDS, EQUALITY_IDS_ELEMENT));

	// sort_order_id: optional int
	children.emplace_back("sort_order_id", LogicalType::INTEGER);
	data_file_field_ids.emplace_back("sort_order_id", CreateFieldID(SORT_ORDER_ID, true));

	// v2-only suffix
	if (manifest_format_version >= 2) {
		// content: int
		children.emplace_back("content", LogicalType::INTEGER);
		data_file_field_ids.emplace_back("content", CreateFieldID(CONTENT, false));

		// referenced_data_file: string
		children.emplace_back("referenced_data_file", LogicalType::VARCHAR);
		data_file_field_ids.emplace_back("referenced_data_file", CreateFieldID(REFERENCED_DATA_FILE, true));
	}

	// v3-only suffix
	if (manifest_format_version >= 3) {
		// first_row_id: optional long
		children.emplace_back("first_row_id", LogicalType::BIGINT);
		data_file_field_ids.emplace_back("first_row_id", CreateFieldID(FIRST_ROW_ID, true));

		// content_offset: long
		children.emplace_back("content_offset", LogicalType::BIGINT);
		data_file_field_ids.emplace_back("content_offset", CreateFieldID(CONTENT_OFFSET, true));

		// content_size_in_bytes: long
		children.emplace_back("content_size_in_bytes", LogicalType::BIGINT);
		data_file_field_ids.emplace_back("content_size_in_bytes", CreateFieldID(CONTENT_SIZE_IN_BYTES, true));
	}

	// data_file: struct(...)
	names.push_back("data_file");
	types.push_back(LogicalType::STRUCT(std::move(children)));
	data_file_field_ids.emplace_back("__duckdb_field_id", Value::INTEGER(DATA_FILE));
	data_file_field_ids.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
	field_ids.emplace_back("data_file", Value::STRUCT(data_file_field_ids));

	//! Populate the DataChunk with the data files

	DataChunk chunk;
	chunk.Initialize(allocator, types, STANDARD_VECTOR_SIZE);

	child_list_t<Value> metadata_values;
	constexpr const char *required_keys[] = {"schema",         "schema-id", "partition-spec", "partition-spec-id",
	                                         "format-version", "content"};
	for (auto key : required_keys) {
		auto entry = manifest_metadata.find(key);
		if (entry == manifest_metadata.end()) {
			throw InvalidInputException("Manifest metadata missing required key '%s'", key);
		}
		metadata_values.emplace_back(key, entry->second);
	}
	auto metadata_map = Value::STRUCT(std::move(metadata_values));

	CopyInfo copy_info;
	copy_info.is_from = false;
	copy_info.options["root_name"].push_back(Value("manifest_entry"));
	copy_info.options["field_ids"].push_back(Value::STRUCT(field_ids));
	copy_info.options["metadata"].push_back(metadata_map);

	//! write.manifest.compression-codec: let the Avro COPY writer emit the codec natively.
	//! "null" is the COPY default (uncompressed), so only set the option for a compressing codec.
	auto avro_codec =
	    iceberg_avro_codec::ResolveAvroCodec(table_metadata.GetTableProperty("write.manifest.compression-codec"));
	if (!StringUtil::CIEquals(avro_codec, "null")) {
		copy_info.options["codec"].push_back(Value(avro_codec));
	}

	CopyFunctionBindInput input(copy_info);
	input.file_extension = "avro";

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto bind_data = copy.copy_to_bind(context, input, names, types);

	auto global_state = copy.copy_to_initialize_global(context, *bind_data, path);
	auto local_state = copy.copy_to_initialize_local(execution_context, *bind_data);
	CopyFunctionFileStatistics stats;
	copy.copy_to_get_written_statistics(context, *bind_data, *global_state, stats);

	for (idx_t offset = 0; offset < manifest_entries.size(); offset += STANDARD_VECTOR_SIZE) {
		const auto chunk_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, manifest_entries.size() - offset);
		if (offset > 0) {
			chunk.Reset();
		}

		auto status_writer = FlatVector::Writer<int32_t>(chunk.data[0], chunk_count);
		auto snapshot_id_writer = FlatVector::Writer<int64_t>(chunk.data[1], chunk_count);
		auto sequence_number_writer = FlatVector::Writer<int64_t>(chunk.data[2], chunk_count);
		auto file_sequence_number_writer = FlatVector::Writer<int64_t>(chunk.data[3], chunk_count);
		DataFileVectorWriters data_file_writers(chunk.data[4], chunk_count, table_metadata, manifest_format_version);

		for (idx_t i = 0; i < chunk_count; i++) {
			auto &manifest_entry = manifest_entries[offset + i];
			status_writer.WriteValue(static_cast<int32_t>(manifest_entry.status));
			//! FIXME: this is missing logic, needs to be looked into
			//! SPEC: Snapshot id where the file was added, or deleted if status is 2. Inherited when null.
			// snapshot_id: long
			if (manifest_entry.HasSnapshotId()) {
				snapshot_id_writer.WriteValue(manifest_entry.GetSnapshotId());
			} else {
				snapshot_id_writer.WriteNull();
			}
			// sequence_number: long
			// file_sequence_number: long
			if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
				auto sequence_number = manifest_entry.ExplicitSequenceNumber();
				if (sequence_number) {
					sequence_number_writer.WriteValue(*sequence_number);
				} else {
					sequence_number_writer.WriteNull();
				}
				auto file_sequence_number = manifest_entry.ExplicitFileSequenceNumber();
				if (file_sequence_number) {
					file_sequence_number_writer.WriteValue(*file_sequence_number);
				} else {
					file_sequence_number_writer.WriteNull();
				}
			} else {
				sequence_number_writer.WriteValue(manifest_entry.GetSequenceNumber(manifest_file));
				file_sequence_number_writer.WriteValue(manifest_entry.GetFileSequenceNumber(manifest_file));
			}

			data_file_writers.WriteRow(i, manifest_entry.data_file, extended_partition_info);
		}

		chunk.SetChildCardinality(chunk_count);
		copy.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, chunk);
	}
	copy.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
	copy.copy_to_finalize(context, *bind_data, *global_state);
	if (stats.row_count != manifest_entries.size()) {
		throw InternalException("Avro copy for manifest failed, expected %d written, found only %d",
		                        manifest_entries.size(), stats.row_count);
	}
	return stats.file_size_bytes;
}

} // namespace manifest_file

} // namespace duckdb
