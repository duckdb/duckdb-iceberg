//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_file_credentials.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/main/secret/file_credentials.hpp"
#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

struct IcebergTable;

//! Gives the reads of a table of an attached Iceberg catalog the credentials to open its files with
//! (FileCredentials). An extension registers one as an ExtensionCallback; without one, every file is read with the
//! secret that matches its path.
class IcebergFileCredentialsCallback : public ExtensionCallback {
public:
	//! The credentials for a read of `table`, or nullptr to open its files by path. Asked each time a scan of the table
	//! is bound (also by iceberg_scan and the metadata functions), and before a metadata file of the table is read;
	//! returning the same object within a transaction lets those reads share it.
	// TODO: writes to a table with credentials (INSERT, UPDATE, DELETE) read and write its files by path
	virtual shared_ptr<FileCredentials> GetCredentials(ClientContext &context, const IcebergTable &table) = 0;

	//! What the registered callback returns; nullptr when none is registered. Throws when more than one is.
	static shared_ptr<FileCredentials> Get(ClientContext &context, const IcebergTable &table) {
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
		return found ? found->GetCredentials(context, table) : nullptr;
	}
};

} // namespace duckdb
