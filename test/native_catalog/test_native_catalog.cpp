#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "catalog_model.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/uuid.hpp"

using namespace duckdb;
using iceberg_test::Json;

namespace {

string TestRoot() {
	auto fs = FileSystem::CreateLocal();
	return fs->JoinPath(FileSystem::GetWorkingDirectory(), "duckdb_unittest_tempdir",
	                    "native-api-" + UUID::ToString(UUID::GenerateRandomUUID()));
}

//! Fail at the filesystem boundary after one metadata file has already been completed.
class FailingFileSystem : public FileSystem {
public:
	FailingFileSystem() : local(FileSystem::CreateLocal()) {
	}
	string GetName() const override {
		return "NativeCatalogFailingFileSystem";
	}
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		if (flags.OpenForWriting() && StringUtil::EndsWith(path, ".metadata.json")) {
			metadata_writes++;
			if (fail_at && metadata_writes == fail_at) {
				throw IOException("Injected metadata write failure");
			}
		}
		return local->OpenFile(path, flags, opener);
	}
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override {
		local->CreateDirectoriesRecursive(path, opener);
	}
	void RemoveOwnedRoot(const string &root) {
		local->RemoveDirectory(root);
	}

	idx_t metadata_writes = 0;
	idx_t fail_at = 0;

private:
	unique_ptr<FileSystem> local;
};

unique_ptr<QueryResult> Query(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	result->Collection();
	return result;
}

} // namespace

TEST_CASE("Native catalog publishes no table when the second metadata write fails", "[iceberg_native]") {
	FailingFileSystem fs;
	auto root = TestRoot();
	iceberg_test::Catalog catalog(fs, root);
	REQUIRE(catalog.Request("POST", "/v1/namespaces", R"({"namespace":["atomic"]})")->status == HTTPStatusCode::OK_200);
	for (auto name : {"a", "b"}) {
		auto body =
		    Json::Object({{"name", name}, {"schema", Json::Parse(R"({"type":"struct","schema-id":0,"fields":[]})")}});
		auto start = iceberg_test::NowMillis();
		auto response = catalog.Request("POST", "/v1/namespaces/atomic/tables", body.Dump());
		REQUIRE(response->status == HTTPStatusCode::OK_200);
		auto timestamp = Json::Parse(response->body).At("metadata").At("last-updated-ms").Integer();
		REQUIRE(timestamp > start);
		REQUIRE(timestamp <= iceberg_test::NowMillis());
	}
	auto before_a = catalog.Request("GET", "/v1/namespaces/atomic/tables/a", "")->body;
	auto before_b = catalog.Request("GET", "/v1/namespaces/atomic/tables/b", "")->body;
	auto changes = Json::Array();
	for (auto name : {"a", "b"}) {
		changes.Push(
		    Json::Object({{"identifier", Json::Object({{"namespace", Json::Array({"atomic"})}, {"name", name}})},
		                  {"requirements", Json::Array()},
		                  {"updates", Json::Parse(R"([{"action":"set-properties","updates":{"changed":"yes"}}])")}}));
	}
	auto transaction = Json::Object({{"table-changes", changes}}).Dump();
	fs.fail_at = fs.metadata_writes + 2;
	REQUIRE(catalog.Request("POST", "/v1/transactions/commit", transaction)->status ==
	        HTTPStatusCode::InternalServerError_500);
	REQUIRE(fs.metadata_writes == fs.fail_at);
	REQUIRE(catalog.Request("GET", "/v1/namespaces/atomic/tables/a", "")->body == before_a);
	REQUIRE(catalog.Request("GET", "/v1/namespaces/atomic/tables/b", "")->body == before_b);

	fs.fail_at = 0;
	REQUIRE(catalog.Request("POST", "/v1/transactions/commit", transaction)->status == HTTPStatusCode::NoContent_204);
	for (auto name : {"a", "b"}) {
		auto response = catalog.Request("GET", string("/v1/namespaces/atomic/tables/") + name, "");
		auto metadata = Json::Parse(response->body).At("metadata");
		REQUIRE(metadata.At("properties").At("changed").String() == "yes");
		REQUIRE(metadata.At("metadata-log").Items().size() == 1);
	}
	fs.RemoveOwnedRoot(root);
}

TEST_CASE("Native catalog state belongs to the database, not its initializing connection", "[iceberg_native]") {
	auto root = TestRoot();
	{
		DuckDB database(nullptr);
		// The mock must work independently of the Iceberg extension.
		REQUIRE(database.ExtensionIsLoaded("iceberg_mock"));
		REQUIRE_FALSE(database.ExtensionIsLoaded("iceberg"));
		{
			Connection initializer(database);
			Query(initializer, "CALL iceberg_test_catalog_init('" + root + "')");
			auto result =
			    Query(initializer, "SELECT status FROM iceberg_test_catalog_request('POST', '/v1/namespaces', "
			                       "'{\"namespace\":[\"survives\"]}')");
			REQUIRE(result->Collection().GetValue(0, 0).GetValue<int32_t>() == 200);
		}
		Connection connection(database);
		Query(connection, "CALL iceberg_test_catalog_init('" + root + "')");
		auto result = Query(connection,
		                    "SELECT status FROM iceberg_test_catalog_request('GET', '/v1/namespaces/survives', NULL)");
		REQUIRE(result->Collection().GetValue(0, 0).GetValue<int32_t>() == 200);

		DuckDB other_database(nullptr);
		Connection other(other_database);
		Query(other, "CALL iceberg_test_catalog_init('" + root + "')");
		auto isolated =
		    Query(other, "SELECT status FROM iceberg_test_catalog_request('GET', '/v1/namespaces/survives', NULL)");
		REQUIRE(isolated->Collection().GetValue(0, 0).GetValue<int32_t>() == 404);
	}
	{
		DuckDB reopened(nullptr);
		Connection connection(reopened);
		Query(connection, "CALL iceberg_test_catalog_init('" + root + "')");
		auto result = Query(connection,
		                    "SELECT status FROM iceberg_test_catalog_request('GET', '/v1/namespaces/survives', NULL)");
		REQUIRE(result->Collection().GetValue(0, 0).GetValue<int32_t>() == 404);
	}
	FileSystem::CreateLocal()->RemoveDirectory(root);
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
