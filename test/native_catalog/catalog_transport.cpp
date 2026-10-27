#include "catalog_model.hpp"
#include "duckdb/common/http_transport_manager.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {
namespace iceberg_test {

static constexpr const char *ORIGIN = "http://iceberg-mock.invalid";
static constexpr const char *PROVIDER_NAME = "IcebergTestHTTPUtil";

class IcebergTestHTTPClient : public HTTPClient {
public:
	explicit IcebergTestHTTPClient(shared_ptr<Catalog> catalog_p) : HTTPClient(ORIGIN), catalog(std::move(catalog_p)) {
	}
	void Initialize(HTTPParams &) override {
	}
	unique_ptr<HTTPResponse> Get(GetRequestInfo &info) override {
		auto result = Request("GET", info, "");
		if (info.response_handler && !info.response_handler(*result)) {
			throw IOException("Native catalog response callback rejected response");
		}
		if (info.content_handler &&
		    !info.content_handler(reinterpret_cast<const_data_ptr_t>(result->body.data()), result->body.size())) {
			throw IOException("Native catalog content callback rejected response");
		}
		return result;
	}
	unique_ptr<HTTPResponse> Post(PostRequestInfo &info) override {
		auto result = Request(info.send_post_as_get_request ? "GET" : "POST", info,
		                      string(reinterpret_cast<const char *>(info.buffer_in), info.buffer_in_len));
		info.buffer_out = result->body;
		return result;
	}
	unique_ptr<HTTPResponse> Head(HeadRequestInfo &info) override {
		return Request("HEAD", info, "");
	}
	unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &info) override {
		return Request("DELETE", info, "");
	}
	unique_ptr<HTTPResponse> Put(PutRequestInfo &info) override {
		return Request("PUT", info, string(reinterpret_cast<const char *>(info.buffer_in), info.buffer_in_len));
	}
	unique_ptr<HTTPResponse> Options(OptionsRequestInfo &info) override {
		return Request("OPTIONS", info, "");
	}

private:
	unique_ptr<HTTPResponse> Request(const string &method, BaseRequest &info, const string &body) {
		if (info.proto_host_port != ORIGIN) {
			throw InvalidInputException("Native Iceberg test catalog only accepts '%s'", ORIGIN);
		}
		auto result = catalog->Request(method, info.path, body);
		result->url = info.url;
		result->reason = HTTPUtil::GetStatusMessage(result->status);
		return result;
	}

	shared_ptr<Catalog> catalog;
};

class IcebergTestHTTPUtil : public HTTPUtil {
public:
	explicit IcebergTestHTTPUtil(shared_ptr<Catalog> catalog_p) : catalog(std::move(catalog_p)) {
	}
	string GetName() const override {
		return PROVIDER_NAME;
	}
	unique_ptr<HTTPParams> InitializeParameters(ClientContext &context, const string &url) override {
		CheckOrigin(url);
		ClientContextFileOpener opener(context);
		return HTTPUtil::InitializeParameters(&opener, nullptr);
	}
	unique_ptr<HTTPParams> InitializeParameters(DatabaseInstance &db, const string &url) override {
		CheckOrigin(url);
		DatabaseFileOpener opener(db);
		return HTTPUtil::InitializeParameters(&opener, nullptr);
	}
	unique_ptr<HTTPParams> InitializeParameters(optional_ptr<FileOpener>, optional_ptr<FileOpenerInfo>) override {
		// HTTPFS casts filesystem-session parameters to its own HTTPFSParams before
		// making requests. Reject that entry point rather than returning base params
		// to an incompatible consumer. Iceberg uses the context overload above.
		throw InvalidInputException("Native Iceberg test catalog does not support HTTP filesystem I/O");
	}
	unique_ptr<HTTPClient> InitializeClient(HTTPParams &, const string &origin) override {
		if (origin != ORIGIN) {
			throw InvalidInputException("Native Iceberg test catalog only accepts '%s'", ORIGIN);
		}
		return make_uniq<IcebergTestHTTPClient>(catalog);
	}
	const string &Root() const {
		return catalog->root;
	}

private:
	static void CheckOrigin(const string &url) {
		string path;
		string origin;
		HTTPUtil::DecomposeURL(url, path, origin);
		if (origin != ORIGIN) {
			throw InvalidInputException("Native Iceberg test catalog only accepts '%s'", ORIGIN);
		}
	}

	shared_ptr<Catalog> catalog;
};

struct TestCatalogBindData : public TableFunctionData {
	bool initialize;
	vector<string> arguments;
};

struct TestCatalogGlobalState : public GlobalTableFunctionState {
	bool finished = false;
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<TestCatalogGlobalState>();
	}
};

static unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                     vector<duckdb::Identifier> &names) {
	auto result = make_uniq<TestCatalogBindData>();
	result->initialize = input.inputs.size() == 1;
	for (idx_t i = 0; i < input.inputs.size(); i++) {
		if (input.inputs[i].IsNull() && (result->initialize || i < 2)) {
			throw InvalidInputException("Native catalog root, method and path cannot be NULL");
		}
		result->arguments.push_back(input.inputs[i].IsNull() ? "" : input.inputs[i].GetValue<string>());
	}
	if (result->initialize) {
		types = {LogicalType::BOOLEAN};
		names = {"Success"};
	} else {
		types = {LogicalType::INTEGER, LogicalType::VARCHAR};
		names = {"status", "body"};
	}
	return std::move(result);
}

static void InitializeCatalog(ClientContext &context, const string &input_root) {
	if (input_root.empty() || FileSystem::IsRemoteFile(input_root)) {
		throw InvalidInputException("Native catalog requires a nonempty local directory");
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = FileSystem::GetFileSystem(context);
	auto root = fs.ExpandPath(input_root);
	if (!fs.IsPathAbsolute(root)) {
		root = fs.JoinPath(FileSystem::GetWorkingDirectory(), root);
	}
	auto &config = db.config;
	if (config.GetHTTPUtil().GetName() == PROVIDER_NAME) {
		if (config.GetHTTPUtil().Cast<IcebergTestHTTPUtil>().Root() != root) {
			throw InvalidInputException("Native Iceberg test catalog is already initialized with another root");
		}
		return;
	}
	// HTTPFS publishes its HTTP provider during loading. Install ours afterwards.
	ExtensionHelper::AutoLoadExtension(db, "httpfs");
	if (!db.ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("Native Iceberg test catalog requires httpfs");
	}
	// The provider outlives individual connections; do not retain a ClientFileSystem.
	auto catalog = make_shared_ptr<Catalog>(db.GetFileSystem(), root);
	config.SetHTTPUtil(make_shared_ptr<IcebergTestHTTPUtil>(std::move(catalog)));
}

static unique_ptr<HTTPResponse> ProtocolRequest(ClientContext &context, const vector<string> &arguments) {
	auto &db = DatabaseInstance::GetDatabase(context);
	if (db.config.GetHTTPUtil().GetName() != PROVIDER_NAME) {
		throw InvalidInputException("Call iceberg_test_catalog_init before issuing native catalog requests");
	}
	auto method = StringUtil::Upper(arguments[0]);
	auto &path = arguments[1];
	if (path.empty() || path[0] != '/' || StringUtil::StartsWith(path, "//")) {
		throw InvalidInputException("Native catalog request path must start with a single '/'");
	}
	auto url = string(ORIGIN) + path;
	auto session = db.config.GetHTTPTransportManager().CreateSession(context, url);
	auto &params = session.Parameters();
	HTTPHeaders headers;
	auto &body = arguments[2];
	if (method == "GET") {
		GetRequestInfo request(url, headers, params, nullptr, nullptr);
		request.try_request = true;
		return session.Request(request);
	}
	if (method == "POST") {
		PostRequestInfo request(url, headers, params, reinterpret_cast<const_data_ptr_t>(body.data()), body.size());
		request.try_request = true;
		auto result = session.Request(request);
		result->body = request.buffer_out;
		return result;
	}
	if (method == "DELETE") {
		DeleteRequestInfo request(url, headers, params);
		request.try_request = true;
		return session.Request(request);
	}
	if (method == "HEAD") {
		HeadRequestInfo request(url, headers, params);
		request.try_request = true;
		return session.Request(request);
	}
	if (method == "PUT") {
		string content_type = "application/json";
		PutRequestInfo request(url, headers, params, reinterpret_cast<const_data_ptr_t>(body.data()), body.size(),
		                       content_type);
		request.try_request = true;
		return session.Request(request);
	}
	if (method == "OPTIONS") {
		OptionsRequestInfo request(url, headers, params);
		request.try_request = true;
		return session.Request(request);
	}
	throw InvalidInputException("Unsupported native catalog request method '%s'", method);
}

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<TestCatalogGlobalState>();
	if (state.finished) {
		return;
	}
	state.finished = true;
	auto &data = input.bind_data->Cast<TestCatalogBindData>();
	if (data.initialize) {
		InitializeCatalog(context, data.arguments[0]);
		output.data[0].Append(Value::BOOLEAN(true));
	} else {
		auto response = ProtocolRequest(context, data.arguments);
		output.data[0].Append(Value::INTEGER(static_cast<int>(response->status)));
		output.data[1].Append(Value(response->body));
	}
	output.CheckCardinality(1);
}

} // namespace iceberg_test
} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(iceberg_mock, loader) {
	using namespace duckdb;
	loader.RegisterFunction(TableFunction("iceberg_test_catalog_init", {LogicalType::VARCHAR}, iceberg_test::Execute,
	                                      iceberg_test::Bind, iceberg_test::TestCatalogGlobalState::Init));
	loader.RegisterFunction(TableFunction(
	    "iceberg_test_catalog_request", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    iceberg_test::Execute, iceberg_test::Bind, iceberg_test::TestCatalogGlobalState::Init));
}
}
