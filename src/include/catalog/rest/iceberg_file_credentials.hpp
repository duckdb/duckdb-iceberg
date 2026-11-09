//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_file_credentials.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

struct IcebergTable;

//! Selects a registered DuckDB secret for reads of a table of an attached Iceberg catalog.
//! An extension registers one as an ExtensionCallback; without one, reads use normal secret lookup by path.
class IcebergFileCredentialsCallback : public ExtensionCallback {
public:
	//! The name of a registered secret for a read of `table`, or "" for normal lookup by path.
	//! Asked when binding a scan (including iceberg_scan and metadata functions) and before reading table metadata.
	//! The callback owns registration and lifetime, e.g. CreateSecret with SecretPersistType::TRANSACTION.
	//! Named secrets retain DuckDB's normal visibility, scope checks, setting fallback and cache behavior.
	// TODO: writes to a table with credentials (INSERT, UPDATE, DELETE) read and write its files by path
	virtual string GetFileSecret(ClientContext &context, const IcebergTable &table) = 0;

	//! A metadata file naming the secret selected for this read.
	static OpenFileInfo GetFileInfo(ClientContext &context, const IcebergTable &table, string path) {
		OpenFileInfo file(std::move(path));
		auto name = Get(context, table);
		if (!name.empty()) {
			file.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
			file.extended_info->options[ExtendedOpenFileInfo::SECRET_OPTION] = Value(std::move(name));
		}
		return file;
	}

	//! What the registered callback returns; "" when none is registered. Throws when more than one is.
	static string Get(ClientContext &context, const IcebergTable &table) {
		optional_ptr<IcebergFileCredentialsCallback> found;
		for (auto &callback : ExtensionCallback::Iterate(context)) {
			auto credentials_callback = dynamic_cast<IcebergFileCredentialsCallback *>(callback.get());
			if (!credentials_callback) {
				continue;
			}
			if (found) {
				throw InvalidConfigurationException(
				    "More than one extension registered an IcebergFileCredentialsCallback");
			}
			found = credentials_callback;
		}
		return found ? found->GetFileSecret(context, table) : string();
	}
};

} // namespace duckdb
