#include "planning/deletes/iceberg_equality_delete_fast_filter.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/arena_containers/arena_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cstring>
#include <type_traits>

namespace duckdb {

namespace {

constexpr idx_t MAXIMUM_POWER_OF_TWO = NumericLimits<idx_t>::Maximum() / 2 + 1;

constexpr bool CanRoundHashTableCapacity(idx_t expected_count) {
	return expected_count <= MAXIMUM_POWER_OF_TWO / 2;
}

constexpr bool CanAllocateHashTableEntries(idx_t capacity) {
	return capacity <= NumericLimits<idx_t>::Maximum() / sizeof(IcebergEqualityDeleteFastFilter::FlatShard::Entry);
}

//! Boundary checks for the two size calculations in FlatShard::Initialize.
static_assert(CanRoundHashTableCapacity(MAXIMUM_POWER_OF_TWO / 2), "Maximum supported key count must fit");
static_assert(!CanRoundHashTableCapacity(MAXIMUM_POWER_OF_TWO / 2 + 1),
              "Key counts whose rounded capacity overflows must be rejected");
static_assert(CanAllocateHashTableEntries(NumericLimits<idx_t>::Maximum() /
                                          sizeof(IcebergEqualityDeleteFastFilter::FlatShard::Entry)),
              "Maximum supported entry allocation must fit");
static_assert(!CanAllocateHashTableEntries(
                  NumericLimits<idx_t>::Maximum() / sizeof(IcebergEqualityDeleteFastFilter::FlatShard::Entry) + 1),
              "Overflowing entry allocations must be rejected");
static_assert(std::is_trivially_destructible<IcebergEqualityDeleteFastFilter::FlatShard::Entry>::value,
              "FlatShard entries must not require per-entry destruction");

} // namespace

void IcebergEqualityDeleteFastFilter::FlatShard::Initialize(Allocator &allocator, idx_t expected_count) {
	if (expected_count == 0) {
		return;
	}
	//! NextPowerOfTwo cannot represent a power larger than the highest bit in idx_t.
	//! Check that rounding expected_count * 2 can succeed before calling it.
	if (!CanRoundHashTableCapacity(expected_count)) {
		throw OutOfMemoryException("Too many Iceberg equality-delete keys");
	}
	capacity = NumericCast<idx_t>(NextPowerOfTwo(MaxValue<idx_t>(expected_count * 2, 16)));
	if (!CanAllocateHashTableEntries(capacity)) {
		throw OutOfMemoryException("Iceberg equality-delete hash table is too large");
	}
	auto allocation_size = capacity * sizeof(Entry);
	entries = allocator.Allocate(allocation_size);
	auto entry_data = GetEntries();
	for (idx_t entry_idx = 0; entry_idx < capacity; entry_idx++) {
		new (&entry_data[entry_idx]) Entry();
	}
	capacity_mask = capacity - 1;
}

void IcebergEqualityDeleteFastFilter::FlatShard::Insert(hash_t hash, string_t key) {
	D_ASSERT(capacity > 0);
	//! The low bits already select the outer shard. Use independent bits for
	//! the per-shard table so every key in a shard does not start in one slot.
	auto slot = (hash >> 6) & capacity_mask;
	while (true) {
		auto &entry = GetEntries()[slot];
		if (!entry.occupied) {
			entry.hash = hash;
			entry.key = key;
			entry.occupied = true;
			return;
		}
		if (entry.hash == hash && entry.key == key) {
			return;
		}
		slot = (slot + 1) & capacity_mask;
	}
}

bool IcebergEqualityDeleteFastFilter::FlatShard::Contains(hash_t hash, string_t key) const {
	if (capacity == 0) {
		return false;
	}
	auto slot = (hash >> 6) & capacity_mask;
	while (true) {
		auto &entry = GetEntries()[slot];
		if (!entry.occupied) {
			return false;
		}
		if (entry.hash == hash && entry.key == key) {
			return true;
		}
		slot = (slot + 1) & capacity_mask;
	}
}

namespace {

struct FileLayout {
	idx_t file_index;
	idx_t layout_index;
	vector<idx_t> source_indices;
};

struct StagedKey {
	hash_t hash;
	string_t key;
};

static bool TypeIsByteComparable(const LogicalType &type) {
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

static inline const char *RowBytes(const UnifiedVectorFormat &format, const LogicalType &type, idx_t index,
                                   uint32_t &length) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		auto &value = UnifiedVectorFormat::GetData<string_t>(format)[index];
		length = value.GetSize();
		return value.GetData();
	}
	auto width = GetTypeIdSize(type.InternalType());
	length = NumericCast<uint32_t>(width);
	return const_char_ptr_cast(format.data + width * index);
}

static string LayoutKey(const vector<idx_t> &column_indices) {
	string result;
	result.resize(column_indices.size() * sizeof(idx_t));
	if (!result.empty()) {
		memcpy(&result[0], column_indices.data(), result.size());
	}
	return result;
}

} // namespace

IcebergEqualityDeleteFastFilter::BuildResult
IcebergEqualityDeleteFastFilter::Build(const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
                                       const unordered_map<int32_t, idx_t> &field_indexes,
                                       const vector<LogicalType> &target_types, Allocator &allocator) {
	BuildResult result;
	result.filter = make_shared_ptr<IcebergEqualityDeleteFastFilter>(allocator);
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
			    !TypeIsByteComparable(target_types[entry->second])) {
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
			filter.layouts.push_back(std::move(layout));
		} else {
			layout_idx = layout_entry->second;
		}
		file_layouts.push_back({file_idx, layout_idx, std::move(sources)});
	}

	//! Staging can be comparable in size to the finished table. Allocate it through
	//! the buffer manager as well, and release it as a group when Build returns.
	ArenaAllocator staging_arena(allocator);
	using StagedKeyVector = unsafe_arena_vector<StagedKey>;
	vector<vector<StagedKeyVector>> staged(filter.layouts.size());
	for (auto &layout_staged : staged) {
		layout_staged.reserve(SHARD_COUNT);
		for (idx_t shard_idx = 0; shard_idx < SHARD_COUNT; shard_idx++) {
			layout_staged.emplace_back(staging_arena);
		}
	}
	for (auto &descriptor : file_layouts) {
		auto &file = delete_files[descriptor.file_index].get();
		auto &layout = filter.layouts[descriptor.layout_index];
		auto count = file.equality_values.size();
		vector<Vector> cast_columns;
		vector<UnifiedVectorFormat> formats(layout.types.size());
		cast_columns.reserve(layout.types.size());
		bool cast_ok = true;
		for (idx_t column_idx = 0; column_idx < layout.types.size(); column_idx++) {
			//! The cached delete state is immutable; use a mutable reference vector for
			//! vector-format/cast APIs without mutating its backing vectors.
			auto source = Vector::Ref(file.equality_values.data[descriptor.source_indices[column_idx]]);
			cast_columns.emplace_back(layout.types[column_idx], count);
			if (source.GetType() == layout.types[column_idx]) {
				source.ToUnifiedFormat(formats[column_idx]);
			} else {
				string error;
				if (!VectorOperations::DefaultTryCast(source, cast_columns.back(), count, &error)) {
					cast_ok = false;
					break;
				}
				cast_columns.back().ToUnifiedFormat(formats[column_idx]);
			}
		}
		if (!cast_ok) {
			continue;
		}

		for (idx_t row = 0; row < count; row++) {
			if (layout.types.size() == 1) {
				auto index = formats[0].sel->get_index(row);
				if (!formats[0].validity.RowIsValid(index)) {
					layout.has_null_key = true;
					continue;
				}
				uint32_t length;
				auto bytes = RowBytes(formats[0], layout.types[0], index, length);
				auto target = filter.arena.Allocate(MaxValue<idx_t>(length, 1));
				if (length) {
					memcpy(target, bytes, length);
				}
				string_t key(const_char_ptr_cast(target), length);
				auto hash = Hash(key);
				staged[descriptor.layout_index][hash & (SHARD_COUNT - 1)].push_back({hash, key});
			} else {
				idx_t total_length = 0;
				for (idx_t column_idx = 0; column_idx < layout.types.size(); column_idx++) {
					auto index = formats[column_idx].sel->get_index(row);
					uint32_t length = 0;
					if (formats[column_idx].validity.RowIsValid(index)) {
						RowBytes(formats[column_idx], layout.types[column_idx], index, length);
					}
					total_length += 1 + sizeof(uint32_t) + length;
				}
				auto target = filter.arena.Allocate(total_length);
				idx_t offset = 0;
				for (idx_t column_idx = 0; column_idx < layout.types.size(); column_idx++) {
					auto index = formats[column_idx].sel->get_index(row);
					uint32_t length = 0;
					const char *bytes = nullptr;
					auto valid = formats[column_idx].validity.RowIsValid(index);
					if (valid) {
						bytes = RowBytes(formats[column_idx], layout.types[column_idx], index, length);
					}
					target[offset++] = valid ? 1 : 0;
					memcpy(target + offset, &length, sizeof(uint32_t));
					offset += sizeof(uint32_t);
					if (length) {
						memcpy(target + offset, bytes, length);
						offset += length;
					}
				}
				string_t key(const_char_ptr_cast(target), NumericCast<uint32_t>(total_length));
				auto hash = Hash(key);
				staged[descriptor.layout_index][hash & (SHARD_COUNT - 1)].push_back({hash, key});
			}
		}
		result.accelerated_files[descriptor.file_index] = true;
	}

	bool has_filter_keys = false;
	for (idx_t layout_idx = 0; layout_idx < filter.layouts.size(); layout_idx++) {
		auto &layout = filter.layouts[layout_idx];
		for (idx_t shard_idx = 0; shard_idx < SHARD_COUNT; shard_idx++) {
			auto &records = staged[layout_idx][shard_idx];
			layout.shards[shard_idx].Initialize(allocator, records.size());
			for (auto &record : records) {
				layout.shards[shard_idx].Insert(record.hash, record.key);
			}
			layout.key_count += records.size();
		}
		has_filter_keys |= layout.key_count > 0 || layout.has_null_key;
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
		if (layout.key_count == 0 && !layout.has_null_key) {
			continue;
		}
		vector<UnifiedVectorFormat> formats(layout.column_indices.size());
		for (idx_t column_idx = 0; column_idx < layout.column_indices.size(); column_idx++) {
			keys.data[layout.column_indices[column_idx]].ToUnifiedFormat(formats[column_idx]);
		}
		vector<data_t> buffer;
		for (idx_t row = 0; row < count; row++) {
			if (deleted[row]) {
				continue;
			}
			if (formats.size() == 1) {
				auto index = formats[0].sel->get_index(row);
				if (!formats[0].validity.RowIsValid(index)) {
					deleted[row] = layout.has_null_key;
					continue;
				}
				uint32_t length;
				auto bytes = RowBytes(formats[0], layout.types[0], index, length);
				string_t key(bytes, length);
				auto hash = Hash(key);
				deleted[row] = layout.shards[hash & (SHARD_COUNT - 1)].Contains(hash, key);
				continue;
			}

			buffer.clear();
			for (idx_t column_idx = 0; column_idx < formats.size(); column_idx++) {
				auto index = formats[column_idx].sel->get_index(row);
				uint32_t length = 0;
				const char *bytes = nullptr;
				auto valid = formats[column_idx].validity.RowIsValid(index);
				if (valid) {
					bytes = RowBytes(formats[column_idx], layout.types[column_idx], index, length);
				}
				buffer.push_back(valid ? 1 : 0);
				auto offset = buffer.size();
				buffer.resize(offset + sizeof(uint32_t) + length);
				memcpy(buffer.data() + offset, &length, sizeof(uint32_t));
				if (length) {
					memcpy(buffer.data() + offset + sizeof(uint32_t), bytes, length);
				}
			}
			string_t key(const_char_ptr_cast(buffer.data()), NumericCast<uint32_t>(buffer.size()));
			auto hash = Hash(key);
			deleted[row] = layout.shards[hash & (SHARD_COUNT - 1)].Contains(hash, key);
		}
	}
}

} // namespace duckdb
