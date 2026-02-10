#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

namespace duckdb {

struct IcebergDeleteData {
public:
	IcebergDeleteData() {
	}
	virtual ~IcebergDeleteData() {
	}

public:
	virtual unique_ptr<DeleteFilter> ToFilter() const = 0;
};

} // namespace duckdb
