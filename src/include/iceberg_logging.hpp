#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/string_util.hpp"

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

} // namespace duckdb
