#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "metadata/iceberg_manifest.hpp"

namespace duckdb {

enum class IcebergDeleteType : uint8_t { POSITIONAL_DELETE, DELETION_VECTOR };

struct IcebergDeleteData {
public:
	IcebergDeleteData(IcebergDeleteType type, const IcebergManifestEntry &entry) : type(type) {
		entries.push_back(entry);
	}
	virtual ~IcebergDeleteData() {
	}

public:
	virtual unique_ptr<DeleteFilter> ToFilter() const = 0;
	virtual void ToSet(set<idx_t> &out) const = 0;

public:
	IcebergDeleteType type;
	//! The manifest entry(s) that created this delete data
	vector<reference<const IcebergManifestEntry>> entries;
};

} // namespace duckdb
