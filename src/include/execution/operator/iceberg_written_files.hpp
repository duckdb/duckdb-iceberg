#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp_base.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
struct IcebergManifestEntry;
struct IcebergTable;

enum class IcebergWriteOperation : uint8_t {
	INSERT,
	CTAS,
	UPDATE,
	MERGE_INSERT,
	MERGE_UPDATE,
	COPY_ON_WRITE_DELETE,
	REWRITE_DATA_FILES
};

struct IcebergWrittenFile {
	string file_path;
	int64_t record_count = 0;
	int64_t file_size_in_bytes = 0;
};

//! With iceberg_store_written_files, the data files an Iceberg write staged are added to the temporary table
//! iceberg_written_files, in the write's transaction
struct IcebergWrittenFiles {
	static constexpr const char *SETTING = "iceberg_store_written_files";
	static constexpr const char *TABLE = "iceberg_written_files";

	//! Capture statistics before the manifest entries move into transaction state.
	static vector<IcebergWrittenFile> Collect(const vector<IcebergManifestEntry> &entries);
	//! Publish after staging, with the operation and one wall-clock timestamp for this batch of files.
	static void Record(ClientContext &context, const IcebergTable &table, IcebergWriteOperation operation,
	                   const vector<IcebergWrittenFile> &files);
	static bool IsEnabled(ClientContext &context);

private:
	static void Append(ClientContext &context, const string &table, const string &operation, timestamp_tz_t written_at,
	                   const vector<IcebergWrittenFile> &files);
};

} // namespace duckdb
