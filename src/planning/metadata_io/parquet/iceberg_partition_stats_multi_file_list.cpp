#include "planning/metadata_io/parquet/iceberg_partition_stats_multi_file_list.hpp"

namespace duckdb {

IcebergPartitionStatsScanInfo::IcebergPartitionStatsScanInfo(vector<IcebergPartitionStatsScanFile> files_p)
    : files(std::move(files_p)) {
}

IcebergPartitionStatsScanInfo::~IcebergPartitionStatsScanInfo() {
}

IcebergPartitionStatsMultiFileList::IcebergPartitionStatsMultiFileList(shared_ptr<IcebergPartitionStatsScanInfo> info_p,
                                                                       vector<OpenFileInfo> paths)
    : SimpleMultiFileList(std::move(paths)), info(std::move(info_p)) {
}

IcebergPartitionStatsMultiFileList::~IcebergPartitionStatsMultiFileList() {
}

} // namespace duckdb
