#include "planning/deletes/iceberg_delete_candidate_index.hpp"

#include "duckdb/common/types/hash.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"

namespace duckdb {

hash_t IcebergDeleteCandidateIndex::PartitionHash::operator()(const vector<Value> &key) const {
	hash_t result = 0;
	for (auto &value : key) {
		// Value::Hash normalizes signed zero and NaN, and hashes all NULLs alike.
		result = CombineHash(result, value.Hash());
	}
	return result;
}

bool IcebergDeleteCandidateIndex::PartitionEqual::operator()(const vector<Value> &left,
                                                             const vector<Value> &right) const {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (!Value::NotDistinctFrom(left[i], right[i])) {
			return false;
		}
	}
	return true;
}

static bool CanHashPartitionType(const LogicalType &type) {
	// Only hash like-typed scalar values. NotDistinctFrom can coerce unlike
	// types, whose hashes need not agree; those use the conservative path.
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

IcebergDeleteCandidateIndex::IcebergDeleteCandidateIndex(vector<IcebergManifestEntry> entries_p,
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
		vector<Value> key;
		if (TryPartitionKey(values, key)) {
			by_partition[std::move(key)].push_back(entry_idx);
		} else {
			fallback.push_back(entry_idx);
		}
	}
}

bool IcebergDeleteCandidateIndex::TryPartitionKey(const partition_value_map_t &values, vector<Value> &key) const {
	for (idx_t i = 0; i < partition_fields.size(); i++) {
		auto value = values.find(partition_fields[i]);
		if (value == values.end()) {
			return false;
		}
		auto &v = value->second.get();
		if (!v.IsNull() && (v.type() != partition_types[i] || !CanHashPartitionType(v.type()))) {
			return false;
		}
		key.push_back(v);
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
			auto partition = by_partition.find(key);
			if (partition != by_partition.end()) {
				result.push_back(partition->second);
			}
			if (!fallback.empty()) {
				result.push_back(fallback);
			}
		}
	}
	return result;
}

} // namespace duckdb
