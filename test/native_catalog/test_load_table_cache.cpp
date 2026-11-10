#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/logging/log_type.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"

using namespace duckdb;

namespace {

struct LoadTableCacheTest {
	LoadTableCacheTest()
	    : root(FileSystem::CreateLocal()->JoinPath(FileSystem::GetWorkingDirectory(), "duckdb_unittest_tempdir",
	                                               "load-table-api-" + UUID::ToString(UUID::GenerateRandomUUID()))),
	      database(nullptr), connection(database) {
		Query("CALL iceberg_test_catalog_init('" + root + "')");
		Query("ATTACH '' AS lake (TYPE ICEBERG, URI 'http://iceberg-mock.invalid', "
		      "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', STAGE_CREATE_TABLES true)");
		Query("CREATE SCHEMA lake.default");
		Query("CREATE TABLE lake.default.t AS SELECT 1 AS id");
		Query("SET VARIABLE original_response = (SELECT body::JSON FROM "
		      "iceberg_test_catalog_request('GET', '/v1/namespaces/default/tables/t', NULL))");
		Query("CALL enable_logging('HTTP', storage = 'memory')");
		Query("SET redact_http_logs = false");
		Query("BEGIN");
		Publish("old", "\"A\"");
	}

	~LoadTableCacheTest() {
		connection.Query("ROLLBACK");
		FileSystem::CreateLocal()->RemoveDirectory(root);
	}

	template <typename... ARGS>
	unique_ptr<QueryResult> Query(const string &sql, ARGS... args) {
		auto result = connection.Query(sql, args...);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
		result->Collection();
		return result;
	}

	IcebergCatalog &Catalog() {
		return duckdb::Catalog::GetCatalog(*connection.context, Identifier("lake")).Cast<IcebergCatalog>();
	}

	LoadTableResultCache &Cache() {
		return Catalog().table_request_cache;
	}

	IcebergTable &Table() {
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
		                       QualifiedName(Identifier("lake"), Identifier("default"), Identifier("t")));
		return duckdb::Catalog::GetEntry(*connection.context, lookup).Cast<IcebergTableSchemaVersion>().table_info;
	}

	void Respond(const string &etag, int status = 200, const string &patch = "{}") {
		auto result = Query("SELECT status FROM iceberg_test_catalog_request('POST', '/__test/response', "
		                    "json_object('method', 'GET', 'path', '/v1/namespaces/default/tables/t', 'status', ?, "
		                    "'headers', json_object('ETag', ?), 'body', CASE WHEN ? = 304 THEN NULL ELSE "
		                    "json_merge_patch(getvariable('original_response'), ?::JSON) END)::VARCHAR)",
		                    status, Value(etag), status, Value(patch));
		REQUIRE(result->Collection().GetValue(0, 0).GetValue<int32_t>() == 204);
	}

	void Publish(const string &version, const string &etag) {
		Respond(etag, 200, "{\"config\":{\"test-version\":\"" + version + "\"}}");
		auto publication = Cache().BeginLoad(key);
		auto result =
		    IcebergLoadTableRequest({"default"}, "t", publication.get()).Execute(*connection.context, Catalog());
		REQUIRE_FALSE(result.error_);
		REQUIRE(publication->TryPublish(std::move(result.result_), std::move(result.etag)));
	}

	Value LastValidator() {
		auto result = Query("SELECT message FROM duckdb_logs() WHERE type = 'HTTP' "
		                    "ORDER BY timestamp DESC LIMIT 1");
		REQUIRE(result->Collection().Count() == 1);
		auto log = result->Collection().GetValue(0, 0).DefaultCastAs(HTTPLogType::GetLogType());
		auto &request = StructValue::GetChildren(log)[0];
		auto &fields = StructValue::GetChildren(request);
		REQUIRE(fields[0].ToString() == "GET");
		REQUIRE(fields[1].ToString() == "http://iceberg-mock.invalid/v1/namespaces/default/tables/t");
		for (auto &entry : MapValue::GetChildren(fields.back())) {
			auto &pair = StructValue::GetChildren(entry);
			if (pair[0].ToString() == "If-None-Match") {
				return pair[1];
			}
		}
		return Value();
	}

	string root;
	DuckDB database;
	Connection connection;
	const string key = "default.t";
};

} // namespace

TEST_CASE("LoadTable revalidation retains its response when the cache is replaced", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto publication = test.Cache().BeginLoad(test.key);
	IcebergLoadTableRequest request({"default"}, "t", publication.get());
	string expected_etag = "\"B\"";

	SECTION("A newer load publishes first") {
		test.Publish("new", "\"B\"");
	}
	SECTION("The scheduling owner is destroyed before execution") {
		publication.reset();
		test.Publish("new", "\"B\"");
	}
	SECTION("Authoritative publication clears the validator") {
		auto replacement = make_shared_ptr<rest_api_objects::LoadTableResult>(publication->cached_result->Copy());
		(*replacement->config)["test-version"] = "new";
		test.Cache().SetOrOverwrite(test.key, std::move(replacement));
		expected_etag.clear();
	}

	test.Respond("\"A\"", 304);
	test.Query("CALL truncate_duckdb_logs()");
	auto result = request.Execute(*test.connection.context, test.Catalog());
	REQUIRE(result.status_ == HTTPStatusCode::NotModified_304);
	REQUIRE(result.result_);
	REQUIRE(result.result_->config->at("test-version") == "old");
	REQUIRE(test.LastValidator().ToString() == "\"A\"");
	if (publication) {
		REQUIRE_FALSE(publication->TryPublish(std::move(result.result_), std::move(result.etag)));
	}
	auto current = test.Cache().BeginLoad(test.key);
	REQUIRE(current->cached_result);
	REQUIRE(current->cached_result->config->at("test-version") == "new");
	// SetOrOverwrite has no HTTP validator; a fetched replacement carries its own.
	REQUIRE(current->etag == expected_etag);
}

TEST_CASE("Superseded LoadTable revalidations cannot publish after a newer load is abandoned", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto first = test.Cache().BeginLoad(test.key);
	auto second = test.Cache().BeginLoad(test.key);
	test.Respond("\"A\"", 304);
	auto result =
	    IcebergLoadTableRequest({"default"}, "t", first.get()).Execute(*test.connection.context, test.Catalog());

	SECTION("The newer load is still pending") {
		REQUIRE_FALSE(first->TryPublish(result.result_, "\"stale\""));
		REQUIRE(second->TryPublish(result.result_, "\"B\""));
		auto current = test.Cache().BeginLoad(test.key);
		REQUIRE(current->etag == "\"B\"");
	}
	SECTION("The newer load is cancelled") {
		second.reset();
		REQUIRE_FALSE(first->TryPublish(result.result_, "\"stale\""));
		auto current = test.Cache().BeginLoad(test.key);
		REQUIRE(current->etag == "\"A\"");
		REQUIRE(current->TryPublish(result.result_, "\"B\""));
	}
}

TEST_CASE("An evicted LoadTable response stays alive for its request but cannot repopulate the cache",
          "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto &table = test.Table();
	auto publication = test.Cache().BeginLoad(test.key);
	IcebergLoadTableRequest request({"default"}, "t", publication.get());
	test.Cache().EvictIfCurrent(table);

	test.Respond("\"A\"", 304);
	auto result = request.Execute(*test.connection.context, test.Catalog());
	REQUIRE(result.result_);
	REQUIRE(result.result_->config->at("test-version") == "old");
	REQUIRE_FALSE(publication->TryPublish(std::move(result.result_), std::move(result.etag)));
	auto current = test.Cache().BeginLoad(test.key);
	REQUIRE_FALSE(current->cached_result);
	REQUIRE(current->etag.empty());
}

TEST_CASE("LoadTable refresh revalidates metadata while credential refresh remains unconditional", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto &table = test.Table();
	auto snapshot_id = table.table_metadata.current_snapshot_id;
	test.Respond("\"B\"", 304);
	test.Query("CALL truncate_duckdb_logs()");
	table.RefreshFromCatalog(*test.connection.context);
	REQUIRE(table.table_metadata.current_snapshot_id == snapshot_id);
	REQUIRE(test.LastValidator().ToString() == "\"A\"");
	{
		auto current = test.Cache().BeginLoad(test.key);
		REQUIRE(current->etag == "\"B\"");
	}

	test.Catalog().supported_urls.erase("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials");
	test.Respond("\"B\"", 200, R"({"config":{"s3.access-key-id":"NEW_CONFIG_KEY"},
	    "storage-credentials":[{"prefix":"s3://test/","config":{"s3.access-key-id":"NEW_SCOPED_KEY"}}]})");
	test.Query("CALL truncate_duckdb_logs()");
	auto credentials = IRCAPI::GetTableCredentials(*test.connection.context, test.Catalog(), table.schema, table.name);
	REQUIRE_FALSE(credentials.error_);
	REQUIRE(credentials.result_.config);
	REQUIRE(credentials.result_.config->at("s3.access-key-id") == "NEW_CONFIG_KEY");
	REQUIRE(credentials.result_.storage_credentials.size() == 1);
	REQUIRE(credentials.result_.storage_credentials[0].config.at("s3.access-key-id") == "NEW_SCOPED_KEY");
	REQUIRE(test.LastValidator().IsNull());

	test.Respond("\"B\"", 304);
	REQUIRE_THROWS_WITH(IRCAPI::GetTableCredentials(*test.connection.context, test.Catalog(), table.schema, table.name),
	                    Catch::Contains("HTTP 304 without a cached response"));
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
