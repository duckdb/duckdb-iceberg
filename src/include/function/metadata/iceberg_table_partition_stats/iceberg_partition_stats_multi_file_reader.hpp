#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

struct IcebergPartitionStatsMultiFileReader : public MultiFileReader {
public:
	explicit IcebergPartitionStatsMultiFileReader(shared_ptr<TableFunctionInfo> function_info_p)
	    : function_info(std::move(function_info_p)) {
	}

public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);

public:
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	ReaderInitializeType CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
	                                   const vector<MultiFileColumnDefinition> &global_columns,
	                                   const vector<ColumnIndex> &global_column_ids,
	                                   optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
	                                   const MultiFileReaderBindData &bind_data,
	                                   const virtual_column_map_t &virtual_columns,
	                                   MultiFileColumnMappingMode mapping_mode) override;
	ReaderInitializeType CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
	                                   const vector<MultiFileColumnDefinition> &global_columns,
	                                   const vector<ColumnIndex> &global_column_ids,
	                                   optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
	                                   const MultiFileReaderBindData &bind_data,
	                                   const virtual_column_map_t &virtual_columns) override;

public:
	shared_ptr<TableFunctionInfo> function_info;
};

} // namespace duckdb
