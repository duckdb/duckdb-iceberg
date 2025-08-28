//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

struct IcebergConstants {
	static constexpr const idx_t TRANSACTION_LOCAL_ID_START = 9223372036854775808ULL;
};

struct DataFileIndex {
	DataFileIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit DataFileIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const DataFileIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const DataFileIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const DataFileIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};

} // namespace duckdb
