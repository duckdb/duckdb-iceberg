//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_test_table_loader_extension.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Test-only: registers a GovernedIcebergTableLoader that sqllogictests drive and inspect
class IcebergTestTableLoaderExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	string Name() override;
};

} // namespace duckdb
