#include "planning/deletes/iceberg_equality_delete_fast_filter.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

namespace {

struct FileLayout {
	idx_t file_index;
	vector<idx_t> source_indices;
};

struct LayoutGroup {
	vector<idx_t> column_indices;
	vector<int32_t> field_ids;
	vector<LogicalType> types;
	vector<FileLayout> files;
};

struct EqualityDeleteField {
	int32_t field_id;
	idx_t source_index;
	idx_t target_index;
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

static bool AddDeleteFile(GroupedAggregateHashTable &hash_table, const vector<LogicalType> &types,
                          const IcebergEqualityDeleteFile &delete_file, const vector<idx_t> &source_indices) {
	auto count = delete_file.equality_values.size();
	vector<Vector> cast_columns;
	cast_columns.reserve(types.size());
	DataChunk groups;
	groups.InitializeEmpty(types);

	for (idx_t column_idx = 0; column_idx < types.size(); column_idx++) {
		auto source = Vector::Ref(delete_file.equality_values.data[source_indices[column_idx]]);
		if (source.GetType() == types[column_idx]) {
			groups.data[column_idx].Reference(source);
			continue;
		}
		cast_columns.emplace_back(types[column_idx], count);
		string error;
		if (!VectorOperations::DefaultTryCast(source, cast_columns.back(), count, &error)) {
			return false;
		}
		groups.data[column_idx].Reference(cast_columns.back());
	}
	groups.SetCardinalityUnsafe(count);

	//! GroupedAggregateHashTable's probe state is vector-sized. Delete-file
	//! state can contain many vectors, so add it in standard-sized slices.
	DataChunk slice;
	slice.InitializeEmpty(types);
	Vector addresses(LogicalType::POINTER);
	for (idx_t offset = 0; offset < count; offset += STANDARD_VECTOR_SIZE) {
		auto end = MinValue<idx_t>(offset + STANDARD_VECTOR_SIZE, count);
		slice.Reset();
		slice.Slice(groups, offset, end);
		hash_table.FindOrCreateGroups(slice, addresses);
	}
	return true;
}

} // namespace

IcebergEqualityDeleteFastFilter::BuildResult IcebergEqualityDeleteFastFilterCache::GetOrCreate(
    const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
    const unordered_map<int32_t, idx_t> &field_indexes, const vector<LogicalType> &target_types,
    const set<int32_t> &local_field_ids, ClientContext &context, Allocator &allocator) {
	IcebergEqualityDeleteFastFilter::BuildResult result;
	result.accelerated_files.resize(delete_files.size(), false);

	//! This lightweight mapping is reader-specific: applicability, physical field presence,
	//! evolved target types and reader column indexes can all differ between data files. Only
	//! the expensive immutable hash tables below are shared through the layout cache.
	map<vector<idx_t>, idx_t> layout_indexes;
	vector<LayoutGroup> layout_groups;
	for (idx_t file_idx = 0; file_idx < delete_files.size(); file_idx++) {
		auto &file = delete_files[file_idx].get();
		if (file.equality_values.size() == 0) {
			result.accelerated_files[file_idx] = true;
			continue;
		}
		if (file.equality_values.ColumnCount() != file.equality_ids.size()) {
			throw InvalidConfigurationException("Equality delete file '%s' contains an unexpected number of columns",
			                                    file.file_path);
		}

		vector<EqualityDeleteField> fields;
		bool supported = !file.equality_ids.empty();
		for (idx_t source_idx = 0; source_idx < file.equality_ids.size(); source_idx++) {
			auto field_id = file.equality_ids[source_idx];
			auto entry = field_indexes.find(field_id);
			if (entry == field_indexes.end() || entry->second >= target_types.size() ||
			    !local_field_ids.count(field_id) || !TypeIsSupported(target_types[entry->second])) {
				supported = false;
				break;
			}
			fields.push_back({field_id, source_idx, entry->second});
		}
		if (!supported) {
			continue;
		}
		std::sort(fields.begin(), fields.end(),
		          [](const EqualityDeleteField &a, const EqualityDeleteField &b) { return a.field_id < b.field_id; });

		vector<idx_t> columns;
		vector<idx_t> sources;
		vector<int32_t> field_ids;
		vector<LogicalType> types;
		for (auto &field : fields) {
			columns.push_back(field.target_index);
			sources.push_back(field.source_index);
			field_ids.push_back(field.field_id);
			types.push_back(target_types[field.target_index]);
		}
		auto layout_entry = layout_indexes.find(columns);
		idx_t layout_idx;
		if (layout_entry == layout_indexes.end()) {
			layout_idx = layout_groups.size();
			layout_indexes.emplace(columns, layout_idx);
			layout_groups.push_back({std::move(columns), std::move(field_ids), std::move(types), {}});
		} else {
			layout_idx = layout_entry->second;
		}
		layout_groups[layout_idx].files.push_back({file_idx, std::move(sources)});
	}

	for (auto &group : layout_groups) {
		EqualityDeleteFastFilterKey key;
		key.delete_files.reserve(group.files.size());
		for (auto &descriptor : group.files) {
			key.delete_files.push_back(&delete_files[descriptor.file_index].get());
		}
		key.columns.reserve(group.field_ids.size());
		for (idx_t column_idx = 0; column_idx < group.field_ids.size(); column_idx++) {
			key.columns.push_back({group.field_ids[column_idx], group.types[column_idx]});
		}

		shared_ptr<LayoutLoadState> load;
		bool build = false;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto entry = layouts.find(key);
			if (entry == layouts.end()) {
				load = make_shared_ptr<LayoutLoadState>();
				layouts.emplace(std::move(key), load);
				build = true;
			} else {
				load = entry->second;
			}
		}
		if (build) {
			IcebergEqualityDeleteFastFilter::LayoutBuildResult built;
			ErrorData error;
			string error_context;
			D_ASSERT(!group.files.empty());
			string error_file_path = delete_files[group.files[0].file_index].get().file_path;
			try {
				auto layout = make_shared_ptr<IcebergEqualityDeleteFastFilter::Layout>();
				layout->types = group.types;
				layout->hash_table = make_uniq<GroupedAggregateHashTable>(context, allocator, layout->types,
				                                                          TupleDataValidityType::CAN_HAVE_NULL_VALUES);
				built.layout = layout;
				built.accelerated_files.resize(group.files.size(), false);
				for (idx_t file_idx = 0; file_idx < group.files.size(); file_idx++) {
					auto &descriptor = group.files[file_idx];
					auto &file = delete_files[descriptor.file_index].get();
					error_file_path = file.file_path;
					built.accelerated_files[file_idx] =
					    AddDeleteFile(*layout->hash_table, layout->types, file, descriptor.source_indices);
				}
			} catch (std::exception &ex) {
				error = ErrorData(ex);
				error_context = StringUtil::Format("Failed to build an equality-delete hash filter for file '%s': ",
				                                   error_file_path);
			} catch (...) { // LCOV_EXCL_START
				error = ErrorData(StringUtil::Format(
				    "Unknown exception while building an Iceberg equality-delete hash filter for file '%s'",
				    error_file_path));
			} // LCOV_EXCL_STOP

			{
				lock_guard<mutex> guard(load->lock);
				load->result = std::move(built);
				load->error = std::move(error);
				load->error_context = std::move(error_context);
				load->complete = true;
			}
			load->cv.notify_all();
		}

		IcebergEqualityDeleteFastFilter::LayoutBuildResult cached;
		{
			unique_lock<mutex> guard(load->lock);
			load->cv.wait(guard, [&load] { return load->complete; });
			if (load->error.HasError()) {
				load->error.Throw(load->error_context);
			}
			cached = load->result;
		}

		for (idx_t file_idx = 0; file_idx < group.files.size(); file_idx++) {
			result.accelerated_files[group.files[file_idx].file_index] = cached.accelerated_files[file_idx];
		}
		if (cached.layout->hash_table->Count() > 0) {
			if (!result.filter) {
				result.filter = make_shared_ptr<IcebergEqualityDeleteFastFilter>();
			}
			result.filter->layouts.push_back({group.column_indices, cached.layout});
		}
	}
	return result;
}

idx_t IcebergEqualityDeleteFastFilter::Filter(DataChunk &keys, SelectionVector &sel, idx_t count) const {
	for (auto &layout : layouts) {
		if (count == 0) {
			break;
		}
		if (layout.layout->hash_table->Count() == 0) {
			continue;
		}
		DataChunk groups;
		groups.InitializeEmpty(layout.layout->types);
		for (idx_t column_idx = 0; column_idx < layout.column_indices.size(); column_idx++) {
			groups.data[column_idx].Reference(keys.data[layout.column_indices[column_idx]]);
		}
		groups.SetCardinalityUnsafe(keys.size());
		groups.Slice(sel, count);

		AggregateHTLookupState lookup_state;
		SelectionVector found_groups(count);
		auto found_count = layout.layout->hash_table->LookupGroups(groups, lookup_state, found_groups);
		if (found_count == 0) {
			continue;
		}

		//! LookupGroups returns matching positions in sorted order. Invert those
		//! positions, then compose the survivors with the existing selection.
		SelectionVector survivors(count);
		auto survivor_count = SelectionVector::Inverted(found_groups, survivors, found_count, count);
		sel.SliceInPlace(survivors, survivor_count);
		count = survivor_count;
	}
	return count;
}

} // namespace duckdb
