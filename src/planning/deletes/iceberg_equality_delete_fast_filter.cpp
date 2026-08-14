#include "planning/deletes/iceberg_equality_delete_fast_filter.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

namespace {

struct FileLayout {
	idx_t file_index;
	idx_t layout_index;
	vector<idx_t> source_indices;
};

static bool TypeIsSupported(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::VARCHAR:
		return true;
	default:
		return false;
	}
}

static string LayoutKey(const vector<idx_t> &column_indices) {
	string result;
	result.resize(column_indices.size() * sizeof(idx_t));
	if (!result.empty()) {
		memcpy(&result[0], column_indices.data(), result.size());
	}
	return result;
}

static bool AddDeleteFile(IcebergEqualityDeleteFastFilter::Layout &layout, const IcebergEqualityDeleteFile &delete_file,
                          const vector<idx_t> &source_indices) {
	auto count = delete_file.equality_values.size();
	vector<Vector> cast_columns;
	cast_columns.reserve(layout.types.size());
	DataChunk groups;
	groups.InitializeEmpty(layout.types);

	for (idx_t column_idx = 0; column_idx < layout.types.size(); column_idx++) {
		auto source = Vector::Ref(delete_file.equality_values.data[source_indices[column_idx]]);
		if (source.GetType() == layout.types[column_idx]) {
			groups.data[column_idx].Reference(source);
			continue;
		}
		cast_columns.emplace_back(layout.types[column_idx], count);
		string error;
		if (!VectorOperations::DefaultTryCast(source, cast_columns.back(), count, &error)) {
			return false;
		}
		groups.data[column_idx].Reference(cast_columns.back());
	}
	groups.SetCardinalityUnsafe(count);

	//! GroupedAggregateHashTable's probe state is vector-sized. Delete-file
	//! state can contain many vectors, so add it in standard-sized slices.
	for (idx_t offset = 0; offset < count; offset += STANDARD_VECTOR_SIZE) {
		auto end = MinValue<idx_t>(offset + STANDARD_VECTOR_SIZE, count);
		DataChunk slice;
		slice.InitializeEmpty(layout.types);
		slice.Slice(groups, offset, end);
		Vector addresses(LogicalType::POINTER);
		layout.hash_table->FindOrCreateGroups(slice, addresses);
	}
	return true;
}

} // namespace

IcebergEqualityDeleteFastFilter::BuildResult
IcebergEqualityDeleteFastFilter::Build(const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
                                       const unordered_map<int32_t, idx_t> &field_indexes,
                                       const vector<LogicalType> &target_types, ClientContext &context,
                                       Allocator &allocator) {
	BuildResult result;
	result.filter = make_shared_ptr<IcebergEqualityDeleteFastFilter>();
	result.accelerated_files.resize(delete_files.size(), false);
	auto &filter = *result.filter;

	unordered_map<string, idx_t> layout_indexes;
	vector<FileLayout> file_layouts;
	for (idx_t file_idx = 0; file_idx < delete_files.size(); file_idx++) {
		auto &file = delete_files[file_idx].get();
		if (file.equality_values.size() == 0) {
			result.accelerated_files[file_idx] = true;
			continue;
		}
		if (file.equality_values.ColumnCount() != file.equality_ids.size()) {
			throw InvalidConfigurationException("Equality delete file contains an unexpected number of columns");
		}

		struct Field {
			int32_t field_id;
			idx_t source_index;
			idx_t target_index;
		};
		vector<Field> fields;
		bool supported = !file.equality_ids.empty();
		for (idx_t source_idx = 0; source_idx < file.equality_ids.size(); source_idx++) {
			auto entry = field_indexes.find(file.equality_ids[source_idx]);
			if (entry == field_indexes.end() || entry->second >= target_types.size() ||
			    !TypeIsSupported(target_types[entry->second])) {
				supported = false;
				break;
			}
			fields.push_back({file.equality_ids[source_idx], source_idx, entry->second});
		}
		if (!supported) {
			continue;
		}
		std::sort(fields.begin(), fields.end(), [](const Field &a, const Field &b) { return a.field_id < b.field_id; });

		vector<idx_t> columns;
		vector<idx_t> sources;
		vector<LogicalType> types;
		for (auto &field : fields) {
			columns.push_back(field.target_index);
			sources.push_back(field.source_index);
			types.push_back(target_types[field.target_index]);
		}
		auto key = LayoutKey(columns);
		auto layout_entry = layout_indexes.find(key);
		idx_t layout_idx;
		if (layout_entry == layout_indexes.end()) {
			layout_idx = filter.layouts.size();
			layout_indexes.emplace(std::move(key), layout_idx);
			Layout layout;
			layout.column_indices = std::move(columns);
			layout.types = std::move(types);
			layout.hash_table = make_uniq<GroupedAggregateHashTable>(context, allocator, layout.types,
			                                                         TupleDataValidityType::CAN_HAVE_NULL_VALUES);
			filter.layouts.push_back(std::move(layout));
		} else {
			layout_idx = layout_entry->second;
		}
		file_layouts.push_back({file_idx, layout_idx, std::move(sources)});
	}

	for (auto &descriptor : file_layouts) {
		auto &file = delete_files[descriptor.file_index].get();
		auto &layout = filter.layouts[descriptor.layout_index];
		if (AddDeleteFile(layout, file, descriptor.source_indices)) {
			result.accelerated_files[descriptor.file_index] = true;
		}
	}

	bool has_filter_keys = false;
	for (auto &layout : filter.layouts) {
		has_filter_keys |= layout.hash_table->Count() > 0;
	}
	if (!has_filter_keys) {
		//! In particular, do not install an empty filter on ordinary data files
		//! with no equality deletes: a zero-expression key chunk has no cardinality.
		result.filter.reset();
	}
	return result;
}

void IcebergEqualityDeleteFastFilter::MarkDeleted(DataChunk &keys, vector<bool> &deleted) const {
	auto count = keys.size();
	D_ASSERT(deleted.size() == count);
	for (auto &layout : layouts) {
		if (layout.hash_table->Count() == 0) {
			continue;
		}
		DataChunk groups;
		groups.InitializeEmpty(layout.types);
		for (idx_t column_idx = 0; column_idx < layout.column_indices.size(); column_idx++) {
			groups.data[column_idx].Reference(keys.data[layout.column_indices[column_idx]]);
		}
		groups.SetCardinalityUnsafe(count);

		AggregateHTLookupState lookup_state;
		SelectionVector found_groups(count);
		auto found_count = layout.hash_table->LookupGroups(groups, lookup_state, found_groups);
		for (idx_t found_idx = 0; found_idx < found_count; found_idx++) {
			deleted[found_groups.get_index(found_idx)] = true;
		}
	}
}

} // namespace duckdb
