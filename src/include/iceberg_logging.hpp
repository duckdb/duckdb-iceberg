#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp_base.hpp"

namespace duckdb {

class ClientContext;

struct IcebergLogType : public LogType {
	static constexpr const char *NAME = "Iceberg";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	//! Construct the log type
	IcebergLogType();

	static LogicalType GetLogType() {
		return LogicalType::VARCHAR;
	}

	//! Honor redact_http_logs for sensitive context in diagnostic messages.
	static string Redact(ClientContext &context, const string &value);
	static string Redact(ClientContext &context, const vector<string> &scopes);

	template <typename... ARGS>
	static string ConstructLogMessage(const string &str, ARGS... params) {
		return StringUtil::Format(str, params...);
	}
};

//! A data file an Iceberg write staged in its transaction, one entry per file
struct IcebergDataFileWrittenLogType : public LogType {
	static constexpr const char *NAME = "IcebergDataFileWritten";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	IcebergDataFileWrittenLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(const string &table, const string &file_path, int64_t record_count,
	                                  int64_t file_size_in_bytes, const string &operation, timestamp_tz_t written_at);
};

} // namespace duckdb
