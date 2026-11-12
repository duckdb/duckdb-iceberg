#include "iceberg_logging.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

constexpr LogLevel IcebergLogType::LEVEL;

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

} // namespace duckdb
