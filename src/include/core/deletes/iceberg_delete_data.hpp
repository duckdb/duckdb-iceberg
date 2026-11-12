#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "core/metadata/iceberg_file_identity.hpp"

namespace duckdb {

enum class IcebergDeleteType : uint8_t { POSITIONAL_DELETE, DELETION_VECTOR };

struct IcebergDeleteData {
public:
	explicit IcebergDeleteData(IcebergDeleteType type) : type(type) {
	}
	IcebergDeleteData(IcebergDeleteType type, const IcebergFileIdentity &file) : type(type) {
		source_files.push_back(file);
	}
	virtual ~IcebergDeleteData() {
	}

public:
	virtual unique_ptr<DeleteFilter> ToFilter() const = 0;
	virtual void ToSet(set<idx_t> &out) const = 0;

public:
	IcebergDeleteType type;
	//! Files a deletion vector written for the data file supersedes: the vector these positions were read from,
	//! or the file-scoped position-delete files that hold them. Partition-scoped files stay live.
	vector<IcebergFileIdentity> source_files;
};

} // namespace duckdb
