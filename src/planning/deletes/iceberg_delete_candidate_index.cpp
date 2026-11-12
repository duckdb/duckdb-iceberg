#include "planning/deletes/iceberg_delete_candidate_index.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"

namespace duckdb {

struct IcebergDeleteCandidateIndex::PartitionIndex {
	PartitionIndex(ClientContext &context, const vector<LogicalType> &types, idx_t entry_count)
	    : table(context, BufferAllocator::Get(context), types, {}, vector<AggregateObject>(),
	            NextPowerOfTwo(MaxValue<idx_t>(2, MinValue<idx_t>(STANDARD_VECTOR_SIZE, entry_count) * 2))) {
	}

	void Add(DataChunk &groups, const vector<idx_t> &entry_indexes) {
		Vector addresses(LogicalType::POINTER);
		table.FindOrCreateGroups(groups, addresses);
		auto rows = addresses.Values<data_ptr_t>();
		for (idx_t i = 0; i < entry_indexes.size(); i++) {
			entries_by_group[rows[i].GetValue()].push_back(entry_indexes[i]);
		}
	}

	optional_ptr<const vector<idx_t>> Lookup(const vector<Value> &key, const vector<LogicalType> &types) const {
		DataChunk groups;
		groups.InitializeEmpty(types);
		for (idx_t i = 0; i < key.size(); i++) {
			groups.data[i].Reference(key[i], count_t(1));
		}
		groups.CheckCardinality(1);
		// LookupGroups never mutates the table. Every concurrent caller owns its scratch.
		AggregateHTLookupState state;
		SelectionVector found(1);
		if (!table.LookupGroups(groups, state, found)) {
			return nullptr;
		}
		auto address = state.addresses.Values<data_ptr_t>()[0].GetValue();
		return entries_by_group.at(address);
	}

	// Group rows stay pinned and stable across table growth. This map associates
	// their addresses with entry lists; all partition hashing/equality is DuckDB's.
	GroupedAggregateHashTable table;
	unordered_map<data_ptr_t, vector<idx_t>> entries_by_group;
};

IcebergDeleteCandidateIndex::~IcebergDeleteCandidateIndex() = default;

static bool CanGroupPartitionType(const LogicalType &type) {
	// Preserve the conservative type contract: comparing unlike values can
	// coerce them, while native groups require one exact logical type per column.
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::UUID:
		return true;
	default:
		return false;
	}
}

IcebergDeleteCandidateIndex::IcebergDeleteCandidateIndex(ClientContext &context, vector<IcebergManifestEntry> entries_p,
                                                         const IcebergPartitionSpec &spec)
    : entries(std::move(entries_p)), spec_id(spec.spec_id) {
	if (!spec.IsUnpartitioned()) {
		for (auto &field : spec.fields) {
			partition_fields.push_back(field.partition_field_id);
			partition_types.emplace_back(LogicalType::SQLNULL);
		}
	}
	for (idx_t entry_idx = 0; entry_idx < entries.size(); entry_idx++) {
		auto &entry = entries[entry_idx];
		if (entry.status == IcebergManifestEntryStatusType::DELETED) {
			continue;
		}
		auto &file = entry.data_file;
		if (file.referenced_data_file) {
			by_referenced_path[*file.referenced_data_file].push_back(entry_idx);
			continue;
		}
		unreferenced.push_back(entry_idx);
		if (partition_fields.empty()) {
			continue;
		}
		auto values = IcebergFilePruner::PartitionValueMap(file);
		for (idx_t i = 0; i < partition_fields.size(); i++) {
			auto value = values.find(partition_fields[i]);
			if (partition_types[i].id() == LogicalTypeId::SQLNULL && value != values.end() &&
			    !value->second.get().IsNull()) {
				partition_types[i] = value->second.get().type();
			}
		}
	}
	if (partition_fields.empty()) {
		return;
	}

	// Fix the type of every column before inserting any keys, including keys
	// whose first entries were NULL. Unsupported or inconsistent entries stay local.
	DataChunk groups;
	vector<idx_t> entry_indexes;
	for (auto entry_idx : unreferenced) {
		auto values = IcebergFilePruner::PartitionValueMap(entries[entry_idx].data_file);
		vector<Value> key;
		if (TryPartitionKey(values, key)) {
			if (!by_partition) {
				by_partition = make_uniq<PartitionIndex>(context, partition_types, unreferenced.size());
				groups.Initialize(context, partition_types);
			}
			for (idx_t i = 0; i < key.size(); i++) {
				groups.data[i].Append(key[i]);
			}
			entry_indexes.push_back(entry_idx);
			if (entry_indexes.size() == STANDARD_VECTOR_SIZE) {
				groups.CheckCardinality(entry_indexes.size());
				by_partition->Add(groups, entry_indexes);
				groups.Reset();
				entry_indexes.clear();
			}
		} else {
			fallback.push_back(entry_idx);
		}
	}
	if (!entry_indexes.empty()) {
		groups.CheckCardinality(entry_indexes.size());
		by_partition->Add(groups, entry_indexes);
	}
}

bool IcebergDeleteCandidateIndex::TryPartitionKey(const partition_value_map_t &values, vector<Value> &key) const {
	for (idx_t i = 0; i < partition_fields.size(); i++) {
		auto value = values.find(partition_fields[i]);
		if (value == values.end()) {
			return false;
		}
		if (partition_types[i].id() != LogicalTypeId::SQLNULL && !CanGroupPartitionType(partition_types[i])) {
			return false;
		}
		auto &v = value->second.get();
		if (!v.IsNull() && (v.type() != partition_types[i] || !CanGroupPartitionType(v.type()))) {
			return false;
		}
		key.push_back(v.IsNull() ? Value(partition_types[i]) : v);
	}
	return true;
}

vector<reference<const vector<idx_t>>>
IcebergDeleteCandidateIndex::Lookup(const string &original_data_path, int32_t data_spec_id,
                                    const partition_value_map_t &partition_values) const {
	vector<reference<const vector<idx_t>>> result;
	auto referenced = by_referenced_path.find(original_data_path);
	if (referenced != by_referenced_path.end()) {
		result.push_back(referenced->second);
	}
	if (unreferenced.empty()) {
		return result;
	}
	if (partition_fields.empty()) {
		result.push_back(unreferenced);
	} else if (data_spec_id == spec_id) {
		vector<Value> key;
		if (!TryPartitionKey(partition_values, key)) {
			result.push_back(unreferenced);
		} else {
			auto partition = by_partition ? by_partition->Lookup(key, partition_types) : nullptr;
			if (partition) {
				result.push_back(*partition);
			}
			if (!fallback.empty()) {
				result.push_back(fallback);
			}
		}
	}
	return result;
}

} // namespace duckdb
