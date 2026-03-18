#pragma once

#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "metadata/iceberg_table_schema.hpp"

namespace duckdb {

struct CopyIcebergBindData : public FunctionData {
public:
	CopyIcebergBindData(const CopyInfo &info, vector<string> &&names, vector<LogicalType> &&types,
	                    ClientContext &context);
	CopyIcebergBindData(const vector<string> &names, const vector<LogicalType> &types, const string &file_path,
	                    unique_ptr<IcebergTableMetadata> table_metadata, unique_ptr<IcebergTableSchema> table_schema);

public:
	unique_ptr<FunctionData> Copy() const;
	bool Equals(const FunctionData &other) const;

public:
	vector<string> names;
	vector<LogicalType> types;
	string file_path;

	// Metadata and schema created during binding
	unique_ptr<IcebergTableMetadata> table_metadata;
	shared_ptr<IcebergTableSchema> table_schema;
};

struct IcebergCopyFunction {
	static CopyFunction Create();
};

} // namespace duckdb
