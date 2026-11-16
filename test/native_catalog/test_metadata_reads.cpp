#include "catch.hpp"
#include "duckdb.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"
#include "http/httpfs_client.hpp"

using namespace duckdb;

namespace {

static constexpr const char *ORIGIN = "http://iceberg-metadata.invalid";

struct MetadataRequest {
	RequestType type;
	string path;
	HTTPHeaders headers;
	string bearer_token;
};

struct MetadataHTTPState {
	unordered_map<string, string> files;
	vector<MetadataRequest> requests;

	idx_t Count(RequestType type) const {
		idx_t count = 0;
		for (auto &request : requests) {
			count += request.type == type;
		}
		return count;
	}
};

// Only replace the transport: HTTPFS still opens, downloads and caches the files.
class MetadataHTTPClient : public HTTPClient {
public:
	MetadataHTTPClient(const string &origin, shared_ptr<MetadataHTTPState> state_p)
	    : HTTPClient(origin), state(std::move(state_p)) {
	}

	void Initialize(HTTPParams &) override {
	}

	unique_ptr<HTTPResponse> Get(GetRequestInfo &info) override {
		auto response = Respond(info);
		if (info.response_handler && !info.response_handler(*response)) {
			return response;
		}
		if (info.content_handler) {
			for (idx_t offset = 0; offset < response->body.size(); offset += 1023) {
				auto length = MinValue<idx_t>(1023, response->body.size() - offset);
				if (!info.content_handler(const_data_ptr_cast(response->body.data() + offset), length)) {
					break;
				}
			}
		}
		return response;
	}

	unique_ptr<HTTPResponse> Head(HeadRequestInfo &info) override {
		return Respond(info);
	}

	unique_ptr<HTTPResponse> Post(PostRequestInfo &) override {
		throw InternalException("Unexpected metadata POST");
	}
	unique_ptr<HTTPResponse> Put(PutRequestInfo &) override {
		throw InternalException("Unexpected metadata PUT");
	}
	unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &) override {
		throw InternalException("Unexpected metadata DELETE");
	}
	unique_ptr<HTTPResponse> Options(OptionsRequestInfo &) override {
		throw InternalException("Unexpected metadata OPTIONS");
	}

private:
	unique_ptr<HTTPResponse> Respond(BaseRequest &info) {
		REQUIRE(info.proto_host_port == ORIGIN);
		state->requests.push_back({info.type, info.path, info.headers, info.params.Cast<HTTPFSParams>().bearer_token});
		auto entry = state->files.find(info.path);
		auto status = entry == state->files.end() ? HTTPStatusCode::NotFound_404 : HTTPStatusCode::OK_200;
		auto response = make_uniq<HTTPResponse>(status);
		response->url = info.url;
		response->body = entry == state->files.end() ? "missing metadata" : entry->second;
		response->headers.Insert("ETag", "\"metadata\"");
		response->headers.Insert("Last-Modified", "Sun, 15 Nov 2026 00:00:00 GMT");
		if (status == HTTPStatusCode::OK_200 && info.headers.HasHeader("Range")) {
			auto range = info.headers.GetHeaderValue("Range");
			REQUIRE(StringUtil::StartsWith(range, "bytes="));
			auto separator = range.find('-');
			REQUIRE(separator != string::npos);
			auto start = std::stoull(range.substr(6, separator - 6));
			auto end = std::stoull(range.substr(separator + 1));
			REQUIRE(start <= end);
			REQUIRE(end < response->body.size());
			response->status = HTTPStatusCode::PartialContent_206;
			response->headers.Insert("Content-Range",
			                         StringUtil::Format("bytes %llu-%llu/%llu", start, end, response->body.size()));
			response->body = response->body.substr(start, end - start + 1);
		}
		response->headers.Insert("Content-Length", to_string(response->body.size()));
		response->reason = HTTPUtil::GetStatusMessage(response->status);
		return response;
	}

	shared_ptr<MetadataHTTPState> state;
};

class MetadataHTTPUtil : public HTTPFSUtil {
public:
	explicit MetadataHTTPUtil(shared_ptr<MetadataHTTPState> state_p) : state(std::move(state_p)) {
	}

	unique_ptr<HTTPClient> InitializeClient(HTTPParams &params, const string &origin) override {
		auto client = make_uniq<MetadataHTTPClient>(origin, state);
		client->Initialize(params);
		return client;
	}

private:
	shared_ptr<MetadataHTTPState> state;
};

struct MetadataReadTest {
	MetadataReadTest() : state(make_shared_ptr<MetadataHTTPState>()), database(nullptr) {
		REQUIRE(database.ExtensionIsLoaded("httpfs"));
		DBConfig::GetConfig(*database.instance).SetHTTPUtil(make_shared_ptr<MetadataHTTPUtil>(state));
		Query("SET GLOBAL threads = 1");
		Query("SET GLOBAL http_retries = 0");
		Query("SET GLOBAL enable_http_metadata_cache = false");
	}

	void Query(const string &sql) {
		Connection connection(database);
		auto result = connection.Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
	}

	string ReadText(const OpenFileInfo &file) {
		Connection connection(database);
		connection.BeginTransaction();
		auto result = IcebergUtils::FileToString(file, FileSystem::GetFileSystem(*connection.context));
		connection.Commit();
		return result;
	}

	rest_api_objects::TableMetadata ReadMetadata(const OpenFileInfo &file, const string &codec = "") {
		// Each read gets a new connection and HTTPFS query state. Only the database's external cache survives.
		Connection connection(database);
		connection.BeginTransaction();
		auto fs = make_shared_ptr<CachingFileSystemWrapper>(FileSystem::GetFileSystem(*connection.context),
		                                                    *database.instance);
		auto result = IcebergTableMetadata::Parse(file, *fs, codec);
		connection.Commit();
		return result;
	}

	static string Fixture(const string &path) {
		auto fs = FileSystem::CreateLocal();
		return IcebergUtils::FileToString(
		    fs->JoinPath(FileSystem::GetWorkingDirectory(), "data/persistent/iceberg", path), *fs);
	}

	shared_ptr<MetadataHTTPState> state;
	DuckDB database;
};

} // namespace

TEST_CASE("Metadata downloads preserve shared open options and named credentials", "[iceberg_metadata]") {
	MetadataReadTest test;
	test.Query("CREATE SECRET metadata_token (TYPE HTTP, BEARER_TOKEN 'metadata-test-token', "
	           "SCOPE 'http://iceberg-metadata.invalid')");
	auto contents = MetadataReadTest::Fixture("lineitem_iceberg/metadata/v2.metadata.json");
	test.state->files["/metadata.json"] = contents;
	OpenFileInfo file(string(ORIGIN) + "/metadata.json");
	file.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file.extended_info->options["secret"] = Value("metadata_token");
	auto original = file.ToValue();
	auto shared_file = file;

	SECTION("The whole-file reader") {
		REQUIRE(test.ReadText(file) == contents);
	}
	SECTION("The metadata parser") {
		auto metadata = test.ReadMetadata(file);
		REQUIRE(metadata.current_snapshot_id);
		CHECK(*metadata.current_snapshot_id == 2354745328521181395);
	}
	CHECK(file.ToValue() == original);
	CHECK(shared_file.ToValue() == original);
	REQUIRE(test.state->requests.size() == 1);
	CHECK(test.state->requests[0].type == RequestType::GET_REQUEST);
	CHECK_FALSE(test.state->requests[0].headers.HasHeader("Range"));
	CHECK(test.state->requests[0].bearer_token == "metadata-test-token");
}

TEST_CASE("Metadata downloads respect explicit download choices and known file metadata", "[iceberg_metadata]") {
	MetadataReadTest test;
	test.state->files["/metadata.json"] = "metadata bytes";
	OpenFileInfo file(string(ORIGIN) + "/metadata.json");
	file.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	idx_t expected_heads = 0;
	bool expected_range = false;

	SECTION("The caller requests a full download") {
		file.extended_info->options["force_full_download"] = Value::BOOLEAN(true);
	}
	SECTION("The caller disables a full download") {
		file.extended_info->options["force_full_download"] = Value::BOOLEAN(false);
		expected_heads = 1;
		expected_range = true;
	}
	SECTION("The caller supplies the file size and validators") {
		file.extended_info->options["file_size"] = Value::UBIGINT(14);
		file.extended_info->options["etag"] = Value("\"metadata\"");
		file.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
		expected_range = true;
	}
	SECTION("The caller supplies only the file size") {
		file.extended_info->options["file_size"] = Value::UBIGINT(14);
		expected_heads = 1;
		expected_range = true;
	}
	auto original = file.ToValue();
	REQUIRE(test.ReadText(file) == "metadata bytes");
	CHECK(file.ToValue() == original);
	CHECK(test.state->Count(RequestType::HEAD_REQUEST) == expected_heads);
	CHECK(test.state->Count(RequestType::GET_REQUEST) == 1);
	CHECK(test.state->requests.back().headers.HasHeader("Range") == expected_range);
}

TEST_CASE("A single metadata GET preserves empty and multi-buffer binary contents", "[iceberg_metadata]") {
	MetadataReadTest test;
	for (idx_t size : {0, 1, 32767, 32768, 32769, 131079}) {
		INFO(size);
		string contents(size, '\0');
		for (idx_t i = 0; i < size; i++) {
			contents[i] = static_cast<char>(i % 251);
		}
		auto path = "/body-" + to_string(size);
		test.state->files[path] = contents;
		auto request_count = test.state->requests.size();
		REQUIRE(test.ReadText(string(ORIGIN) + path) == contents);
		REQUIRE(test.state->requests.size() == request_count + 1);
		CHECK(test.state->requests.back().type == RequestType::GET_REQUEST);
		CHECK_FALSE(test.state->requests.back().headers.HasHeader("Range"));
	}
}

TEST_CASE("Versioned metadata reuses the external file cache across connections", "[iceberg_metadata]") {
	MetadataReadTest test;
	string path = "/v2.metadata.json";
	string codec;
	string fixture = "lineitem_iceberg/metadata/v2.metadata.json";
	int64_t snapshot_id = 2354745328521181395;
	SECTION("Uncompressed metadata") {
	}
	SECTION("Gzip recognized by the metadata filename") {
		path = "/v2.gz.metadata.json";
		fixture = "lineitem_iceberg_gz/metadata/v2.gz.metadata.json";
		snapshot_id = 4468019210336628573;
	}
	SECTION("Gzip requested explicitly") {
		codec = "gzip";
		fixture = "lineitem_iceberg_gz/metadata/v2.gz.metadata.json";
		snapshot_id = 4468019210336628573;
	}
	test.state->files[path] = MetadataReadTest::Fixture(fixture);
	OpenFileInfo file(string(ORIGIN) + path);
	for (idx_t i = 0; i < 2; i++) {
		auto metadata = test.ReadMetadata(file, codec);
		REQUIRE(metadata.current_snapshot_id);
		CHECK(*metadata.current_snapshot_id == snapshot_id);
		CHECK(test.state->Count(RequestType::GET_REQUEST) == 1);
		CHECK(test.state->Count(RequestType::HEAD_REQUEST) == 0);
	}
	CHECK_FALSE(file.extended_info);

	// A new metadata location must not reuse the previous version's bytes.
	test.state->files["/v1.metadata.json"] = MetadataReadTest::Fixture("lineitem_iceberg/metadata/v1.metadata.json");
	auto previous = test.ReadMetadata(string(ORIGIN) + "/v1.metadata.json");
	REQUIRE(previous.current_snapshot_id);
	CHECK(*previous.current_snapshot_id == 7817332053627255703);
	CHECK(test.state->Count(RequestType::GET_REQUEST) == 2);
	CHECK(test.state->Count(RequestType::HEAD_REQUEST) == 0);
}

TEST_CASE("Metadata caching respects explicit validation and disabled caching", "[iceberg_metadata]") {
	MetadataReadTest test;
	test.state->files["/metadata.json"] = MetadataReadTest::Fixture("lineitem_iceberg/metadata/v2.metadata.json");
	OpenFileInfo file(string(ORIGIN) + "/metadata.json");
	SECTION("The caller requires validation") {
		file.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(true);
	}
	SECTION("The external file cache is disabled") {
		test.Query("SET GLOBAL enable_external_file_cache = false");
	}
	auto original = file.ToValue();
	for (idx_t i = 0; i < 2; i++) {
		auto metadata = test.ReadMetadata(file);
		REQUIRE(metadata.current_snapshot_id);
		CHECK(*metadata.current_snapshot_id == 2354745328521181395);
		CHECK(test.state->Count(RequestType::GET_REQUEST) == i + 1);
		CHECK(test.state->Count(RequestType::HEAD_REQUEST) == 0);
	}
	CHECK(file.ToValue() == original);
}

TEST_CASE("A failed metadata download is retried by the next read", "[iceberg_metadata]") {
	MetadataReadTest test;
	OpenFileInfo file(string(ORIGIN) + "/metadata.json");
	REQUIRE_THROWS_AS(test.ReadMetadata(file), HTTPException);
	CHECK(test.state->Count(RequestType::GET_REQUEST) == 1);
	test.state->files["/metadata.json"] = MetadataReadTest::Fixture("lineitem_iceberg/metadata/v2.metadata.json");
	auto metadata = test.ReadMetadata(file);
	REQUIRE(metadata.current_snapshot_id);
	CHECK(*metadata.current_snapshot_id == 2354745328521181395);
	CHECK(test.state->Count(RequestType::GET_REQUEST) == 2);
	CHECK(test.state->Count(RequestType::HEAD_REQUEST) == 0);
}

TEST_CASE("Version hints remain fresh across reads", "[iceberg_metadata]") {
	MetadataReadTest test;
	for (auto version : {"1", "2"}) {
		test.state->files["/metadata/version-hint.text"] = version;
		Connection connection(test.database);
		connection.BeginTransaction();
		auto actual = IcebergTableMetadata::GetTableVersionFromHint(
		    string(ORIGIN) + "/metadata", FileSystem::GetFileSystem(*connection.context), "version-hint.text");
		connection.Commit();
		CHECK(actual == version);
	}
	CHECK(test.state->Count(RequestType::GET_REQUEST) == 2);
	CHECK(test.state->Count(RequestType::HEAD_REQUEST) == 0);
}
