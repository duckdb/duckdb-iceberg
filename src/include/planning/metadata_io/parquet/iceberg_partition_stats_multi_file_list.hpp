#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

struct IcebergPartitionStatsScanFile {
	string path;
	int64_t file_size_in_bytes;
};

class IcebergPartitionStatsScanInfo : public TableFunctionInfo {
public:
	explicit IcebergPartitionStatsScanInfo(vector<IcebergPartitionStatsScanFile> files_p);
	virtual ~IcebergPartitionStatsScanInfo();

public:
	vector<IcebergPartitionStatsScanFile> files;
};

class IcebergPartitionStatsMultiFileList : public SimpleMultiFileList {
public:
	IcebergPartitionStatsMultiFileList(shared_ptr<IcebergPartitionStatsScanInfo> info_p, vector<OpenFileInfo> paths);
	virtual ~IcebergPartitionStatsMultiFileList();

public:
	shared_ptr<IcebergPartitionStatsScanInfo> info;
};

} // namespace duckdb
