#pragma once

#include "duckdb.hpp"

namespace duckdb {

class IcebergExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;
};

} // namespace duckdb
