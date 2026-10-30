#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"
#include "duckdb_static_extension.h"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_file_credentials.hpp"

namespace duckdb {

namespace {

//! The key id of the secret a table's file secret holds, unless it is given one
static constexpr const char *CREDENTIALS_KEY_ID = "ICEBERG_TEST_CREDENTIALS";

//! An open or a listing of a file under the recorded directory
struct RecordedRead {
	string path;
	bool listing;
	//! Whether it carried a file secret
	bool has_credentials;
	//! The key id its file secret gave it; empty when it gave none, or refused the path
	string key_id;
};

//! Serves the local files below 'root', as the local file system does, and records each open and listing with the
//! file secret it carries. Like httpfs's S3 file system, it reads the secret for the path, which refuses a path
//! outside its scope, and passes a listing's file secret to each file it lists.
class RecordingFileSystem : public LocalFileSystem {
public:
	explicit RecordingFileSystem(string root_p) : root(std::move(root_p)) {
	}

	string GetName() const override {
		return "IcebergTestRecordingFileSystem";
	}
	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, root);
	}

	vector<RecordedRead> Reads() {
		lock_guard<mutex> guard(lock);
		return reads;
	}
	void Clear() {
		lock_guard<mutex> guard(lock);
		reads.clear();
	}

protected:
	bool SupportsOpenFileExtended() const override {
		return true;
	}
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override {
		Record(file.path, false, file.GetFileSecret(), opener);
		return LocalFileSystem::OpenFile(file.path, flags, opener);
	}
	unique_ptr<MultiFileList> GlobFilesExtended(const string &path, const FileGlobInput &input,
	                                            optional_ptr<FileOpener> opener) override {
		Record(path, true, input.file_secret, opener);
		auto files = LocalFileSystem::GlobFilesExtended(path, input, opener)->GetAllFiles();
		for (auto &file : files) {
			file = file.WithFileSecret(input.file_secret);
		}
		return make_uniq<SimpleMultiFileList>(std::move(files));
	}

private:
	void Record(const string &path, bool listing, const string &file_secret, optional_ptr<FileOpener> opener) {
		// recorded before the secret is read, so that a read the file secret refuses is recorded
		idx_t index;
		{
			lock_guard<mutex> guard(lock);
			index = reads.size();
			reads.push_back(RecordedRead {path, listing, !file_secret.empty(), string()});
		}
		if (file_secret.empty()) {
			return;
		}
		if (!opener) {
			throw InvalidConfigurationException("The file secret for '%s' cannot be used without a file opener", path);
		}
		FileOpenerInfo info(OpenFileInfo(path).WithFileSecret(file_secret));
		KeyValueSecretReader reader(*opener, &info, "s3");
		Value key_id;
		if (reader.TryGetSecretKey("key_id", key_id)) {
			lock_guard<mutex> guard(lock);
			reads[index].key_id = key_id.ToString();
		}
	}

	string root;
	mutex lock;
	vector<RecordedRead> reads;
};

//! The file secret the reads of a table get: an S3 secret with key id `key_id` for the paths under `scope`
struct TableCredentials {
	string scope;
	string key_id;
};

//! `path`, from the working directory when it is relative: the native catalog's warehouse paths are absolute
string Absolute(const string &path) {
	if (LocalFileSystem().IsPathAbsolute(path)) {
		return path;
	}
	return LocalFileSystem().JoinPath(FileSystem::GetWorkingDirectory(), path);
}

//! The key of a table of an attached catalog
string TableKey(const string &catalog, const string &schema, const string &table) {
	return catalog + "." + schema + "." + table;
}

//! The file secret a connection's transaction registered for each table, so that the table's reads in it share
//! it, as the callback contract asks
class TransactionCredentials : public ClientContextState {
public:
	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		by_table.clear();
	}
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		by_table.clear();
	}

	case_insensitive_map_t<string> by_table;
};

//! Gives the reads of the tables named with PRAGMA iceberg_test_credentials their file secret
class TestIcebergFileCredentials : public IcebergFileCredentialsCallback {
public:
	string GetFileSecret(ClientContext &context, const IcebergTable &table) override {
		auto key =
		    TableKey(table.catalog.GetName().GetIdentifierName(), table.schema.name.GetIdentifierName(), table.name);
		TableCredentials config;
		{
			lock_guard<mutex> guard(lock);
			auto entry = tables.find(key);
			if (entry == tables.end()) {
				return string();
			}
			config = entry->second;
		}
		auto state = context.registered_state->GetOrCreate<TransactionCredentials>("iceberg_test_credentials");
		auto &name = state->by_table[key];
		if (name.empty()) {
			auto key_id = config.key_id;
			name = SecretManager::Get(context).RegisterFileSecret(context, config.scope, [key_id](ClientContext &) {
				auto secret = make_shared_ptr<KeyValueSecret>(vector<string>(), Identifier("s3"), Identifier("config"),
				                                              Identifier());
				secret->secret_map[Identifier("key_id")] = Value(key_id);
				secret->secret_map[Identifier("secret")] = Value("iceberg_test_credentials_secret");
				secret->secret_map[Identifier("region")] = Value("us-east-1");
				shared_ptr<const KeyValueSecret> result = std::move(secret);
				return result;
			});
		}
		return name;
	}

public:
	mutex lock;
	//! By TableKey
	case_insensitive_map_t<TableCredentials> tables;
	//! Set by PRAGMA iceberg_test_credentials_record; owned by the virtual file system
	optional_ptr<RecordingFileSystem> recording;
};

TestIcebergFileCredentials &GetTestCallback(ClientContext &context) {
	for (auto &callback : ExtensionCallback::Iterate(context)) {
		auto test_callback = dynamic_cast<TestIcebergFileCredentials *>(callback.get());
		if (test_callback) {
			return *test_callback;
		}
	}
	throw InternalException("iceberg_test_credentials is not registered");
}

//! PRAGMA iceberg_test_credentials('<catalog>', '<schema>', '<table>', '<scope>'[, key_id = '<key id>']): the reads
//! of the table of the attached catalog use credentials for the scope (from the working directory when relative),
//! whose S3 secret has key id ICEBERG_TEST_CREDENTIALS (or key_id); an empty scope removes them
void TableCredentialsPragma(ClientContext &context, const FunctionParameters &parameters) {
	auto key = TableKey(parameters.values[0].GetValue<string>(), parameters.values[1].GetValue<string>(),
	                    parameters.values[2].GetValue<string>());
	auto scope = parameters.values[3].GetValue<string>();
	string key_id = CREDENTIALS_KEY_ID;
	auto key_id_parameter = parameters.named_parameters.find(Identifier("key_id"));
	if (key_id_parameter != parameters.named_parameters.end() && !key_id_parameter->second.IsNull()) {
		key_id = key_id_parameter->second.GetValue<string>();
	}
	auto &callback = GetTestCallback(context);
	lock_guard<mutex> guard(callback.lock);
	if (scope.empty()) {
		callback.tables.erase(key);
		return;
	}
	callback.tables[key] = TableCredentials {Absolute(scope), key_id};
}

//! PRAGMA iceberg_test_credentials_register: registers a second callback
void RegisterSecondCallback(ClientContext &context, const FunctionParameters &parameters) {
	ExtensionCallback::Register(DBConfig::GetConfig(context), make_shared_ptr<TestIcebergFileCredentials>());
}

//! PRAGMA iceberg_test_credentials_record('<directory>'): serves the files below the directory (from the working
//! directory when relative) with a file system that records each open and listing (FROM
//! iceberg_test_credentials_reads()). Once per database; a second call fails.
void RecordReads(ClientContext &context, const FunctionParameters &parameters) {
	auto &callback = GetTestCallback(context);
	lock_guard<mutex> guard(callback.lock);
	if (callback.recording) {
		throw InvalidInputException("iceberg_test_credentials_record was already called for this database");
	}
	auto directory = Absolute(parameters.values[0].GetValue<string>());
	StringUtil::RTrim(directory, "/");
	auto recording = make_uniq<RecordingFileSystem>(directory + "/");
	callback.recording = recording.get();
	FileSystem::GetFileSystem(context).RegisterSubSystem(std::move(recording));
}

RecordingFileSystem &GetRecording(ClientContext &context) {
	auto &callback = GetTestCallback(context);
	lock_guard<mutex> guard(callback.lock);
	if (!callback.recording) {
		throw InvalidInputException("call PRAGMA iceberg_test_credentials_record first");
	}
	return *callback.recording;
}

//! PRAGMA iceberg_test_credentials_clear_reads: forgets the recorded reads
void ClearReads(ClientContext &context, const FunctionParameters &parameters) {
	GetRecording(context).Clear();
}

struct ReadsBindData : public TableFunctionData {
	vector<RecordedRead> reads;
};

unique_ptr<FunctionData> ReadsBind(ClientContext &context, TableFunctionBindInput &input,
                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ReadsBindData>();
	result->reads = GetRecording(context).Reads();
	names.emplace_back("path");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("listing");
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("has_credentials");
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("key_id");
	return_types.push_back(LogicalType::VARCHAR);
	return std::move(result);
}

struct ReadsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<GlobalTableFunctionState> ReadsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ReadsState>();
}

//! FROM iceberg_test_credentials_reads(): the opens and listings recorded since the last clear, in order. key_id is
//! NULL for a read whose credentials gave it no secret, and for a read without credentials.
void ReadsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &reads = data.bind_data->Cast<ReadsBindData>().reads;
	auto &state = data.global_state->Cast<ReadsState>();
	idx_t count = 0;
	for (; state.offset < reads.size() && count < STANDARD_VECTOR_SIZE; state.offset++, count++) {
		auto &read = reads[state.offset];
		output.data[0].Append(Value(read.path));
		output.data[1].Append(Value::BOOLEAN(read.listing));
		output.data[2].Append(Value::BOOLEAN(read.has_credentials));
		output.data[3].Append(read.key_id.empty() ? Value(LogicalType::VARCHAR) : Value(read.key_id));
	}
	output.CheckCardinality(count);
}

void LoadInternal(ExtensionLoader &loader) {
	ExtensionCallback::Register(DBConfig::GetConfig(loader.GetDatabaseInstance()),
	                            make_shared_ptr<TestIcebergFileCredentials>());
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall(Identifier("iceberg_test_credentials"), TableCredentialsPragma,
	                               FunctionSignature()
	                                   .AddPositionalOnly("catalog_name", LogicalType::VARCHAR)
	                                   .AddPositionalOnly("schema_name", LogicalType::VARCHAR)
	                                   .AddPositionalOnly("table_name", LogicalType::VARCHAR)
	                                   .AddPositionalOnly("scope", LogicalType::VARCHAR)
	                                   .AddKeywordOnly("key_id", LogicalType::VARCHAR, Value(LogicalType::VARCHAR))));
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall(Identifier("iceberg_test_credentials_record"), RecordReads, {LogicalType::VARCHAR}));
	loader.RegisterFunction(
	    PragmaFunction::PragmaStatement(Identifier("iceberg_test_credentials_clear_reads"), ClearReads));
	loader.RegisterFunction(TableFunction("iceberg_test_credentials_reads", {}, ReadsScan, ReadsBind, ReadsInit));
	loader.RegisterFunction(
	    PragmaFunction::PragmaStatement(Identifier("iceberg_test_credentials_register"), RegisterSecondCallback));
}

} // namespace

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(iceberg_test_credentials, loader) {
	duckdb::LoadInternal(loader);
}

static int32_t iceberg_test_credentials_describe(duckdb_extension_descriptor *descriptor) {
	descriptor->version = DUCKDB_EXTENSION_DESCRIPTOR_VERSION;
	descriptor->name = "iceberg_test_credentials";
	descriptor->entry_cpp = reinterpret_cast<void (*)(void)>(iceberg_test_credentials_duckdb_cpp_init);
	return 0;
}
}

// Linked into unittest only, which no generated loader registers it in: register before main
static const int32_t iceberg_test_credentials_registered =
    duckdb_register_static_extension(iceberg_test_credentials_describe);
