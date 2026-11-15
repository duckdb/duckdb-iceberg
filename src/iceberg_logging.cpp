#include "iceberg_logging.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

constexpr LogLevel IcebergLogType::LEVEL;
constexpr LogLevel IcebergDataFileWrittenLogType::LEVEL;

IcebergLogType::IcebergLogType() : LogType(NAME, LEVEL) {
}

string IcebergLogType::Redact(ClientContext &context, const string &value) {
	return Settings::Get<RedactHttpLogsSetting>(context) ? HTTPLogType::REDACTED_VALUE : value;
}

string IcebergLogType::Redact(ClientContext &context, const vector<string> &scopes) {
	if (Settings::Get<RedactHttpLogsSetting>(context)) {
		return scopes.empty() ? string() : HTTPLogType::REDACTED_VALUE;
	}
	return StringUtil::Join(scopes, ", ");
}

IcebergDataFileWrittenLogType::IcebergDataFileWrittenLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType IcebergDataFileWrittenLogType::GetLogType() {
	child_list_t<LogicalType> children = {
	    {"table", LogicalType::VARCHAR},       {"file_path", LogicalType::VARCHAR},
	    {"record_count", LogicalType::BIGINT}, {"file_size_in_bytes", LogicalType::BIGINT},
	    {"operation", LogicalType::VARCHAR},   {"written_at", LogicalType::TIMESTAMP_TZ}};
	return LogicalType::STRUCT(children);
}

string IcebergDataFileWrittenLogType::ConstructLogMessage(const string &table, const string &file_path,
                                                          int64_t record_count, int64_t file_size_in_bytes,
                                                          const string &operation, timestamp_tz_t written_at) {
	child_list_t<Value> children = {{"table", Value(table)},
	                                {"file_path", Value(file_path)},
	                                {"record_count", Value::BIGINT(record_count)},
	                                {"file_size_in_bytes", Value::BIGINT(file_size_in_bytes)},
	                                {"operation", Value(operation)},
	                                {"written_at", Value::TIMESTAMPTZ(written_at)}};
	return Value::STRUCT(std::move(children)).ToString();
}

} // namespace duckdb
