#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/string.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/optional.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"

namespace duckdb {

namespace manifest_file {

ManifestReader::ManifestReader(const AvroScan &scan) : BaseManifestReader(scan) {
}

ManifestReader::~ManifestReader() {
}

void ManifestReader::Read() {
	if (finished) {
		return;
	}
	ScanInternal();
	auto &scan_info = scan.scan_info->Cast<IcebergManifestFileScanInfo>();
	if (finished) {
		if (current_batch.IsValid()) {
			scan_info.FinishBatch(current_batch.GetIndex());
		}
		if (AllReadersFinished()) {
			scan_info.FlushBatches();
		}
		return;
	}

	auto &job = local_state->Cast<MultiFileLocalState>().job;
	if (!job || !job->reader) {
		throw InternalException("Manifest chunk was produced without a scan job");
	}
	auto batch_index = job->batch_index;
	auto manifest_file_idx = scan_info.GetManifestIndex(job->reader->file_list_idx.GetIndex());
	if (current_batch.IsValid() && current_batch.GetIndex() != batch_index) {
		scan_info.FinishBatch(current_batch.GetIndex());
	}
	current_batch = batch_index;

	auto &manifest_file = scan_info.manifest_files[manifest_file_idx];
	//! InitializeReader publishes this immutable value under the per-manifest lock
	//! before this reader can produce chunks.
	D_ASSERT(manifest_file.manifest_metadata);
	auto &manifest_metadata = *manifest_file.manifest_metadata;
	if (!scan_info.partition_types_by_spec.count(manifest_metadata.partition_spec_id)) {
		throw InvalidConfigurationException("Manifest file '%s' key-value metadata references partition-spec-id: "
		                                    "%d, which is absent from the manifest scan",
		                                    manifest_file.GetFile().manifest_path, manifest_metadata.partition_spec_id);
	}

	vector<IcebergManifestEntry> decoded_entries;
	ReadChunk(scan_info, manifest_metadata, decoded_entries);
	scan_info.manifest_entries_converted += decoded_entries.size();
	scan_info.AddEntries(batch_index, manifest_file_idx, std::move(decoded_entries));
}

using IntStringMapEntries = VectorIterator<VectorListType<VectorStructType<int32_t, string_t>>>;
using IntIntMapEntries = VectorIterator<VectorListType<VectorStructType<int32_t, int64_t>>>;
using Int32ListEntries = VectorIterator<VectorListType<int32_t>>;
using Int64ListEntries = VectorIterator<VectorListType<int64_t>>;
using Int32Entries = VectorIterator<int32_t>;
using Int64Entries = VectorIterator<int64_t>;
using StringEntries = VectorIterator<string_t>;

template <class T, class ENTRY>
static T ReadRequiredField(const char *name, const ENTRY &entry) {
	if (!entry.IsValid()) {
		throw InvalidConfigurationException("required field '%s' is NULL!", name);
	}
	return entry.GetValueUnsafe();
}

static IcebergManifestEntryStatusType ReadEntryStatus(const Int32Entries::ValueEntry &entry) {
	auto status = ReadRequiredField<int32_t>("status", entry);
	if (status < 0 || status > static_cast<int32_t>(IcebergManifestEntryStatusType::DELETED)) {
		throw InvalidConfigurationException("Invalid manifest entry status: %d", status);
	}
	return static_cast<IcebergManifestEntryStatusType>(status);
}

void ManifestReader::ReadPaths(vector<IcebergManifestListEntry> &manifest_files, const IcebergTableMetadata &metadata,
                               ClientContext &context, const IcebergFileSecret &file_secret,
                               const std::function<void(const string &)> &visit_path) {
	const IcebergSnapshotScanInfo snapshot_info {nullptr, metadata.GetCurrentSchemaId()};
	const IcebergOptions options;
	auto scan_info = make_shared_ptr<IcebergManifestFileScanInfo>(
	    context, metadata, snapshot_info, manifest_files, options, FileSystem::GetFileSystem(context),
	    metadata.GetLocation(), nullptr, vector<idx_t> {}, true);
	scan_info->file_secret = file_secret;
	AvroScan scan("placeholder", context, std::move(scan_info));
	ManifestReader reader(scan);
	while (true) {
		reader.ScanInternal();
		if (reader.Finished()) {
			return;
		}
		auto statuses = reader.chunk.data[0].Values<int32_t>();
		auto files = reader.chunk.data[1].Values<VectorStructType<string_t>>();
		for (idx_t row = 0; row < reader.chunk.size(); row++) {
			auto status = ReadEntryStatus(statuses[row]);
			auto file = files[row];
			if (!file.IsValid()) {
				throw InvalidConfigurationException("required field 'data_file' is NULL!");
			}
			auto path = ReadRequiredField<string_t>("file_path", file.GetChildValue<0>());
			if (path.GetSize() == 0) {
				throw InvalidConfigurationException("data_file field 'file_path' cannot be empty");
			}
			if (status != IcebergManifestEntryStatusType::DELETED) {
				visit_path(path.GetString());
			}
		}
	}
}

static int64_t ReadNonNegativeField(const char *name, const Int64Entries::ValueEntry &entry) {
	auto value = ReadRequiredField<int64_t>(name, entry);
	if (value < 0) {
		throw InvalidConfigurationException("data_file field '%s' cannot be negative", name);
	}
	return value;
}

template <class T, class ENTRY>
static optional<T> ReadOptionalField(const ENTRY &entry) {
	if (entry.IsValid()) {
		return entry.GetValueUnsafe();
	}
	return std::nullopt;
}

static unordered_map<int32_t, Value> GetBounds(const IntStringMapEntries::ValueEntry &entry) {
	unordered_map<int32_t, Value> parsed_bounds;
	if (!entry.IsValid()) {
		return parsed_bounds;
	}

	for (const auto bounds_entry : entry.GetChildValues()) {
		if (!bounds_entry.IsValid()) {
			throw InvalidConfigurationException("Bounds map contains a NULL entry");
		}
		auto key_entry = bounds_entry.template GetChildValue<0>();
		auto value_entry = bounds_entry.template GetChildValue<1>();
		auto key = ReadRequiredField<int32_t>("bounds.key", key_entry);
		auto value = Value(LogicalType::BLOB);
		if (value_entry.IsValid()) {
			auto &str = value_entry.GetValueUnsafe();
			value = Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
		}
		parsed_bounds[key] = std::move(value);
	}
	return parsed_bounds;
}

static unordered_map<int32_t, int64_t> GetCounts(const char *name, const IntIntMapEntries::ValueEntry &entry) {
	unordered_map<int32_t, int64_t> parsed_counts;
	if (!entry.IsValid()) {
		return parsed_counts;
	}

	for (const auto count_entry : entry.GetChildValues()) {
		if (!count_entry.IsValid()) {
			throw InvalidConfigurationException("'%s' map contains a NULL entry", name);
		}
		auto key_entry = count_entry.template GetChildValue<0>();
		auto value_entry = count_entry.template GetChildValue<1>();
		auto key = ReadRequiredField<int32_t>("counts.key", key_entry);
		if (!value_entry.IsValid()) {
			throw InvalidConfigurationException("'%s' map's value for key '%d' is NULL", name, key);
		}
		auto count = value_entry.GetValueUnsafe();
		if (count < 0) {
			throw InvalidConfigurationException("'%s' map's value for key '%d' cannot be negative", name, key);
		}
		parsed_counts[key] = count;
	}
	return parsed_counts;
}

template <class T>
static vector<T> GetListTemplated(const char *name,
                                  const typename VectorIterator<VectorListType<T>>::ValueEntry &entry) {
	vector<T> result;
	if (!entry.IsValid()) {
		return result;
	}
	for (const auto list_entry : entry.GetChildValues()) {
		if (!list_entry.IsValid()) {
			throw InvalidConfigurationException("'%s' list contains NULL", name);
		}
		result.push_back(list_entry.GetValueUnsafe());
	}

	return result;
}

static vector<int32_t> GetEqualityIds(const Int32ListEntries::ValueEntry &entry) {
	return GetListTemplated<int32_t>("equality_ids", entry);
}

static vector<int64_t> GetSplitOffsets(const Int64ListEntries::ValueEntry &entry) {
	return GetListTemplated<int64_t>("split_offsets", entry);
}

void ManifestReader::ReadChunk(const IcebergManifestFileScanInfo &scan_info, const IcebergManifestMetadata &manifest,
                               vector<IcebergManifestEntry> &result) {
	idx_t count = chunk.size();
	auto &manifest_format_version = manifest.format_version;
	auto &table_format_version = scan_info.metadata.iceberg_version;
	auto &partition_field_id_to_type = scan_info.partition_field_id_to_type;
	auto &spec_types = scan_info.partition_types_by_spec.at(manifest.partition_spec_id);
	//! The columns read below follow the table's format version, so a newer manifest has fields we did not read
	if (manifest_format_version > table_format_version) {
		throw InvalidConfigurationException(
		    "Manifest file has format-version %d, which is newer than the table's format-version %d",
		    manifest_format_version, table_format_version);
	}

	//! Setup logic

	//! NOTE: the order of these columns is defined by the order that they are produced in BuildManifestSchema
	//! see `iceberg_avro_multi_file_reader.cpp`
	idx_t vector_index = 0;

	auto &status = chunk.data[vector_index++];
	auto status_entries = status.Values<int32_t>();

	auto &snapshot_id = chunk.data[vector_index++];
	auto snapshot_id_entries = snapshot_id.Values<int64_t>();

	auto &sequence_number = chunk.data[vector_index++];
	auto sequence_number_entries = sequence_number.Values<int64_t>();

	auto &file_sequence_number = chunk.data[vector_index++];
	auto file_sequence_number_entries = file_sequence_number.Values<int64_t>();

	auto &data_file = chunk.data[vector_index++];
	idx_t entry_index = 0;
	auto &data_file_entries = StructVector::GetEntries(data_file);
	auto &file_path = data_file_entries[entry_index++];
	auto file_path_entries = file_path.Values<string_t>();

	auto &file_format = data_file_entries[entry_index++];
	auto file_format_entries = file_format.Values<string_t>();

	auto &partition = data_file_entries[entry_index++];

	auto &record_count = data_file_entries[entry_index++];
	auto record_count_entries = record_count.Values<int64_t>();

	auto &file_size_in_bytes = data_file_entries[entry_index++];
	auto file_size_in_bytes_entries = file_size_in_bytes.Values<int64_t>();

	auto &column_sizes = data_file_entries[entry_index++];
	auto column_sizes_entries = column_sizes.Values<VectorListType<VectorStructType<int32_t, int64_t>>>();

	auto &value_counts = data_file_entries[entry_index++];
	auto value_counts_entries = value_counts.Values<VectorListType<VectorStructType<int32_t, int64_t>>>();

	auto &null_value_counts = data_file_entries[entry_index++];
	auto null_value_counts_entries = null_value_counts.Values<VectorListType<VectorStructType<int32_t, int64_t>>>();

	auto &nan_value_counts = data_file_entries[entry_index++];
	auto nan_value_counts_entries = nan_value_counts.Values<VectorListType<VectorStructType<int32_t, int64_t>>>();

	auto &lower_bounds = data_file_entries[entry_index++];
	auto lower_bounds_entries = lower_bounds.Values<VectorListType<VectorStructType<int32_t, string_t>>>();

	auto &upper_bounds = data_file_entries[entry_index++];
	auto upper_bounds_entries = upper_bounds.Values<VectorListType<VectorStructType<int32_t, string_t>>>();

	auto &split_offsets = data_file_entries[entry_index++];
	auto split_offsets_entries = split_offsets.Values<VectorListType<int64_t>>();

	auto &equality_ids = data_file_entries[entry_index++];
	auto equality_ids_entries = equality_ids.Values<VectorListType<int32_t>>();

	auto &sort_order_id = data_file_entries[entry_index++];
	auto sort_order_id_entries = sort_order_id.Values<int32_t>();

	optional_ptr<Vector> content;
	optional<Int32Entries> content_entries;
	optional_ptr<Vector> referenced_data_file;
	optional<StringEntries> referenced_data_file_entries;
	if (table_format_version >= 2) {
		content = data_file_entries[entry_index++];
		content_entries.emplace(content->Values<int32_t>());

		referenced_data_file = data_file_entries[entry_index++];
		referenced_data_file_entries.emplace(referenced_data_file->Values<string_t>());
	}

	optional_ptr<Vector> first_row_id;
	optional<Int64Entries> first_row_id_entries;
	if (table_format_version >= 3) {
		first_row_id = data_file_entries[entry_index++];
		first_row_id_entries.emplace(first_row_id->Values<int64_t>());
	}
	optional_ptr<Vector> content_offset;
	optional<Int64Entries> content_offset_entries;

	optional_ptr<Vector> content_size_in_bytes;
	optional<Int64Entries> content_size_in_bytes_entries;
	if (table_format_version >= 3) {
		content_offset = data_file_entries[entry_index++];
		content_size_in_bytes = data_file_entries[entry_index++];
		content_offset_entries.emplace(content_offset->Values<int64_t>());
		content_size_in_bytes_entries.emplace(content_size_in_bytes->Values<int64_t>());
	}

	struct PartitionColumn {
		idx_t field_id;
		reference<Vector> values;
		optional_ptr<const LogicalType> unpack_as;
	};
	vector<PartitionColumn> partition_vectors;
	if (partition.GetType().id() != LogicalTypeId::SQLNULL) {
		auto &partition_children = StructVector::GetEntries(partition);
		D_ASSERT(partition_children.size() == partition_field_id_to_type.size());
		idx_t child_index = 0;
		for (auto &field : partition_field_id_to_type) {
			auto &values = partition_children[child_index++];
			auto type = spec_types.find(field.first);
			if (type != spec_types.end()) {
				partition_vectors.push_back(
				    {field.first, values, field.second == type->second ? nullptr : &type->second});
			}
		}
	}

	//! Conversion logic
	for (idx_t i = 0; i < count; i++) {
		IcebergManifestEntry entry;

		entry.status = ReadEntryStatus(status_entries[i]);

		auto &data_file = entry.data_file;
		data_file.file_path = ReadRequiredField<string_t>("file_path", file_path_entries[i]).GetString();
		if (data_file.file_path.empty()) {
			throw InvalidConfigurationException("data_file field 'file_path' cannot be empty");
		}
		data_file.file_format = ReadRequiredField<string_t>("file_format", file_format_entries[i]).GetString();
		data_file.record_count = ReadNonNegativeField("record_count", record_count_entries[i]);
		data_file.file_size_in_bytes = ReadNonNegativeField("file_size_in_bytes", file_size_in_bytes_entries[i]);

		data_file.lower_bounds = GetBounds(lower_bounds_entries[i]);
		data_file.upper_bounds = GetBounds(upper_bounds_entries[i]);
		data_file.column_sizes = GetCounts("column_sizes", column_sizes_entries[i]);
		data_file.value_counts = GetCounts("value_counts", value_counts_entries[i]);
		data_file.null_value_counts = GetCounts("null_value_counts", null_value_counts_entries[i]);
		data_file.nan_value_counts = GetCounts("nan_value_counts", nan_value_counts_entries[i]);

		data_file.split_offsets = GetSplitOffsets(split_offsets_entries[i]);
		data_file.sort_order_id = ReadOptionalField<int32_t>(sort_order_id_entries[i]);

		// The field is required in V1, but its value may still be NULL so the snapshot ID can be inherited.
		// At this point a missing field and a present NULL field are indistinguishable, so do not use
		// ReadRequiredField here.
		entry.SetSnapshotId(ReadOptionalField<int64_t>(snapshot_id_entries[i]));

		//! >= V2
		if (manifest_format_version >= 2) {
			D_ASSERT(content_entries);
			D_ASSERT(referenced_data_file_entries);
			auto content_value = ReadRequiredField<int32_t>("content", (*content_entries)[i]);
			if (content_value < 0 || content_value > 2) {
				throw InvalidConfigurationException("Invalid data file content: %d", content_value);
			}
			data_file.content = static_cast<IcebergManifestEntryContentType>(content_value);
			data_file.equality_ids = GetEqualityIds(equality_ids_entries[i]);

			entry.SetSequenceNumber(ReadOptionalField<int64_t>(sequence_number_entries[i]));
			entry.SetFileSequenceNumber(ReadOptionalField<int64_t>(file_sequence_number_entries[i]));

			if (auto val = ReadOptionalField<string_t>((*referenced_data_file_entries)[i])) {
				data_file.referenced_data_file = val->GetString();
			}
		} else {
			//! SPEC: Data file field content must default to 0 (data)
			data_file.content = IcebergManifestEntryContentType::DATA;
			//! SPEC: Manifest entry field sequence_number must default to 0
			entry.SetSequenceNumber(0);
			//! SPEC: Manifest entry field file_sequence_number must default to 0
			entry.SetFileSequenceNumber(0);
		}

		//! >= V3
		if (manifest_format_version >= 3) {
			D_ASSERT(first_row_id_entries);
			D_ASSERT(content_offset_entries);
			D_ASSERT(content_size_in_bytes_entries);
			data_file.SetFirstRowId(ReadOptionalField<int64_t>((*first_row_id_entries)[i]));
			data_file.content_offset = ReadOptionalField<int64_t>((*content_offset_entries)[i]);
			data_file.content_size_in_bytes = ReadOptionalField<int64_t>((*content_size_in_bytes_entries)[i]);
		}

		for (auto &column : partition_vectors) {
			auto value = column.values.get().GetValue(i);
			if (column.unpack_as) {
				value = value.IsNull() ? Value(*column.unpack_as) : VariantValue::GetValue(value);
				// Spark can write day-transform integers as Avro dates (see FixSamePhysicalTypeCasts).
				if (value.type().id() == LogicalTypeId::DATE && column.unpack_as->id() == LogicalTypeId::INTEGER) {
					value = Value::INTEGER(DateValue::Get(value).days);
				} else if (value.type() != *column.unpack_as) {
					value = value.CastAs(scan.context, *column.unpack_as);
				}
			}
			data_file.partition_info.push_back({column.field_id, std::move(value)});
		}
		result.push_back(std::move(entry));
	}
}

} // namespace manifest_file

} // namespace duckdb
