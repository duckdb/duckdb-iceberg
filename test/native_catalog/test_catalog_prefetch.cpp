#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "catalog_model.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <chrono>
#include <future>
#include <thread>

using namespace duckdb;
using iceberg_test::Json;

namespace {

unique_ptr<QueryResult> Query(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	result->Collection();
	return result;
}

struct PrefetchTest {
	PrefetchTest() : database(nullptr), connection(database) {
		auto fs = FileSystem::CreateLocal();
		root = fs->JoinPath(FileSystem::GetWorkingDirectory(), "duckdb_unittest_tempdir",
		                    "prefetch-" + UUID::ToString(UUID::GenerateRandomUUID()));
		Query(connection, "SET threads = 1; SET async_threads = 4;");
		Query(connection, "CALL iceberg_test_catalog_init('" + root + "')");
	}

	~PrefetchTest() {
		// Release gates even after a failed assertion, before catalog destruction drains requests.
		connection.Query("SELECT * FROM iceberg_test_catalog_request('DELETE', '/__test/response', NULL)");
		if (detach_on_destroy) {
			connection.Query("DETACH IF EXISTS reader");
		}
		FileSystem::CreateLocal()->RemoveDirectory(root);
	}

	Json Request(const string &method, const string &path, const string &body = "", int expected_status = 200) {
		auto escaped = StringUtil::Replace(body, "'", "''");
		auto result = Query(connection, "SELECT * FROM iceberg_test_catalog_request('" + method + "', '" + path +
		                                    "', '" + escaped + "')");
		REQUIRE(result->Collection().GetValue(0, 0).GetValue<int32_t>() == expected_status);
		auto value = result->Collection().GetValue(1, 0).GetValue<string>();
		return value.empty() ? Json() : Json::Parse(value);
	}

	void Block(const string &path, const Json &body) {
		Request("POST", "/__test/response",
		        Json::Object({{"method", "GET"},
		                      {"path", path},
		                      {"status", int64_t(200)},
		                      {"body", body},
		                      {"blocked", Json::Boolean(true)}})
		            .Dump(),
		        204);
	}

	void WaitForBlocked(int64_t count) {
		Request("POST", "/__test/wait", Json::Object({{"count", count}}).Dump(), 204);
	}

	void Release() {
		Request("DELETE", "/__test/response", "", 204);
	}

	void Seed(const string &ns) {
		Request("POST", "/v1/namespaces", Json::Object({{"namespace", Json::Array({ns})}}).Dump());
		Request(
		    "POST", "/v1/namespaces/" + ns + "/tables",
		    R"({"name":"t","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]}})");
	}

	void Attach(Connection &target, const string &options = "") {
		Query(target, "ATTACH '' AS reader (TYPE ICEBERG, URI 'http://iceberg-mock.invalid', "
		              "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none'" +
		                  options + ")");
	}

	bool IsCached(const string &ns, const string &name = "t", bool validate = true) {
		auto &catalog = Catalog::GetCatalog(*connection.context, Identifier("reader")).Cast<IcebergCatalog>();
		auto key = IcebergTable::GetTableKey(catalog, {ns}, name);
		return catalog.table_request_cache.Get(
		    *connection.context, key, [](const rest_api_objects::LoadTableResult &) {}, validate);
	}

	void WaitForCache(const string &ns, const string &name = "t", bool validate = true) {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (!IsCached(ns, name, validate) && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		REQUIRE(IsCached(ns, name, validate));
	}

	string root;
	bool detach_on_destroy = true;
	DuckDB database;
	Connection connection;
};

} // namespace

TEST_CASE("ATTACH returns while independent catalog requests run in the background", "[iceberg_prefetch]") {
	PrefetchTest test;
	SECTION("Dedicated async workers") {
		Query(test.connection, "SET threads = 1; SET async_threads = 4");
	}
	SECTION("Regular workers without an async pool") {
		Query(test.connection, "SET threads = 4; SET async_threads = 0");
	}
	for (auto ns : {"a", "b"}) {
		test.Seed(ns);
		auto path = string("/v1/namespaces/") + ns + "/tables";
		test.Block(path, test.Request("GET", path));
	}
	{
		Connection attaching(test.database);
		test.Attach(attaching, ", TABLE_RESOLUTION 'eager', MAX_TABLE_STALENESS '5 minutes'");
	}
	// Both responses must be in flight, even though the attaching connection is already gone.
	test.WaitForBlocked(2);
	Query(test.connection, "SELECT 42");
	test.Release();
	Query(test.connection, "BEGIN");
	test.WaitForCache("a");
	test.WaitForCache("b");
	Query(test.connection, "COMMIT");
	Query(test.connection, "SELECT * FROM reader.a.t UNION ALL SELECT * FROM reader.b.t");
}

TEST_CASE("Background table loads cannot overwrite newer foreground metadata", "[iceberg_prefetch]") {
	PrefetchTest test;
	Query(test.connection, "SET async_threads = 1");
	test.Seed("a");
	test.Request(
	    "POST", "/v1/namespaces/a/tables",
	    R"({"name":"z","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]}})");
	auto old = test.Request("GET", "/v1/namespaces/a/tables/t");
	test.Block("/v1/namespaces/a/tables/t", old);
	test.Attach(test.connection, ", TABLE_RESOLUTION 'eager', MAX_TABLE_STALENESS '5 minutes'");
	test.WaitForBlocked(1);

	// Replace the route while the background request retains its old response, then perform a foreground load.
	auto fresh = old;
	fresh["metadata"]["properties"]["prefetch-test"] = "new";
	test.Request(
	    "POST", "/__test/response",
	    Json::Object(
	        {{"method", "GET"}, {"path", "/v1/namespaces/a/tables/t"}, {"status", int64_t(200)}, {"body", fresh}})
	        .Dump(),
	    204);
	// New requests use the replacement while the original request still holds its old response.
	Query(test.connection, "SELECT * FROM reader.a.t");
	Query(test.connection, "BEGIN");
	auto &catalog = Catalog::GetCatalog(*test.connection.context, Identifier("reader")).Cast<IcebergCatalog>();
	auto key = IcebergTable::GetTableKey(catalog, {"a"}, "t");
	test.Release();
	// With one worker, the next table is cached only after the older response attempted publication.
	test.WaitForCache("a", "z");
	REQUIRE(
	    catalog.table_request_cache.Get(*test.connection.context, key, [&](const rest_api_objects::LoadTableResult &r) {
		    REQUIRE(r.metadata.properties->at("prefetch-test") == "new");
	    }));
	Query(test.connection, "COMMIT");
}

TEST_CASE("Background table prefetch is bounded and refills its request window", "[iceberg_prefetch]") {
	PrefetchTest test;
	Query(test.connection, "SET async_threads = 3");
	test.Seed("a");
	for (idx_t i = 0; i < 12; i++) {
		auto name = "t" + to_string(i);
		auto result = test.Request(
		    "POST", "/v1/namespaces/a/tables",
		    Json::Object({{"name", name}, {"schema", Json::Parse(R"({"type":"struct","schema-id":0,"fields":[]})")}})
		        .Dump());
		test.Block("/v1/namespaces/a/tables/" + name, result);
	}
	test.Attach(test.connection, ", TABLE_RESOLUTION 'eager', MAX_TABLE_STALENESS '5 minutes'");
	test.WaitForBlocked(3);
	test.Release();
	Query(test.connection, "BEGIN");
	for (idx_t i = 0; i < 12; i++) {
		test.WaitForCache("a", "t" + to_string(i));
	}
	Query(test.connection, "COMMIT");
}

TEST_CASE("Background discovery reuses work already done by a foreground lookup", "[iceberg_prefetch]") {
	PrefetchTest test;
	Query(test.connection, "SET async_threads = 1");
	test.Seed("a");
	test.Request(
	    "POST", "/v1/namespaces/a/tables",
	    R"({"name":"z","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]}})");
	test.Block("/v1/namespaces/a/tables", test.Request("GET", "/v1/namespaces/a/tables"));
	test.Attach(test.connection, ", TABLE_RESOLUTION 'eager', MAX_TABLE_STALENESS '5 minutes'");
	test.WaitForBlocked(1);
	Query(test.connection, "SELECT * FROM reader.a.t");
	test.Release();
	Query(test.connection, "BEGIN");
	test.WaitForCache("a", "z");
	auto requests = Query(test.connection, "SELECT count(*) FROM read_json_auto('" + test.root +
	                                           "/*/requests.jsonl') WHERE method = 'GET' "
	                                           "AND path = '/v1/namespaces/a/tables/t'");
	REQUIRE(requests->Collection().GetValue(0, 0).GetValue<int64_t>() == 1);
	Query(test.connection, "COMMIT");
}

TEST_CASE("Prefetched metadata respects the default freshness policy", "[iceberg_prefetch]") {
	PrefetchTest test;
	test.Seed("a");
	test.Attach(test.connection, ", TABLE_RESOLUTION 'eager'");
	Query(test.connection, "BEGIN");
	test.WaitForCache("a", "t", false);
	REQUIRE_FALSE(test.IsCached("a"));
	Query(test.connection, "COMMIT");
	Query(test.connection, "SELECT * FROM reader.a.t");
}

TEST_CASE("DETACH drains in-flight catalog discovery", "[iceberg_prefetch]") {
	PrefetchTest test;
	test.Seed("a");
	test.Block("/v1/namespaces", test.Request("GET", "/v1/namespaces"));
	test.Attach(test.connection);
	test.WaitForBlocked(1);
	Connection detaching(test.database);
	auto detached = std::async(std::launch::async, [&]() { return detaching.Query("DETACH reader"); });
	CHECK(detached.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
	test.Release();
	REQUIRE(detached.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
	REQUIRE_FALSE(detached.get()->HasError());
}

TEST_CASE("A foreground schema scan can overtake background discovery with one worker", "[iceberg_prefetch]") {
	PrefetchTest test;
	Query(test.connection, "SET async_threads = 1");
	test.Request("POST", "/v1/namespaces", R"({"namespace":["a"]})");
	auto namespaces = test.Request("GET", "/v1/namespaces");
	test.Block("/v1/namespaces", namespaces);
	test.Attach(test.connection);
	test.WaitForBlocked(1);
	// The foreground request will see the removal, while the background response still contains "a".
	test.Request("DELETE", "/v1/namespaces/a", "", 204);
	Connection scanning(test.database);
	auto scan = std::async(std::launch::async, [&]() {
		return scanning.Query("SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'reader'");
	});
	auto &scheduler = TaskScheduler::GetScheduler(*test.connection.context);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	while (scheduler.GetNumberOfTasks() == 0 && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	auto queued = scheduler.GetNumberOfTasks() > 0;
	test.Release();
	auto status = scan.wait_for(std::chrono::seconds(10));
	if (status != std::future_status::ready) {
		scanning.Interrupt();
	}
	REQUIRE(queued);
	REQUIRE(status == std::future_status::ready);
	auto result = scan.get();
	REQUIRE_FALSE(result->HasError());
	// The foreground listing wins even though the older background response still contains "a".
	REQUIRE(result->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Late namespace discovery cannot resurrect a dropped schema", "[iceberg_prefetch]") {
	PrefetchTest test;
	Query(test.connection, "SET async_threads = 1");
	test.Request("POST", "/v1/namespaces", R"({"namespace":["a"]})");
	test.Block("/v1/namespaces", test.Request("GET", "/v1/namespaces"));
	test.Attach(test.connection);
	test.WaitForBlocked(1);
	Query(test.connection, "DROP SCHEMA reader.a");
	test.Release();
	auto schemas = Query(test.connection, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'reader'");
	REQUIRE(schemas->Collection().GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Late table discovery cannot undo foreground table mutations", "[iceberg_prefetch]") {
	PrefetchTest test;
	Query(test.connection, "SET async_threads = 1");
	test.Seed("a");
	test.Block("/v1/namespaces/a/tables", test.Request("GET", "/v1/namespaces/a/tables"));
	test.Attach(test.connection);
	test.WaitForBlocked(1);
	string expected;
	SECTION("Drop leaves an empty entry map") {
		Query(test.connection, "DROP TABLE reader.a.t");
	}
	SECTION("Rename keeps only the new name") {
		Query(test.connection, "ALTER TABLE reader.a.t RENAME TO renamed");
		expected = "renamed";
	}
	test.Release();
	// A refused listing retains cached names, so it cannot mask resurrection by removing the stale name.
	test.Request(
	    "POST", "/__test/response",
	    R"({"method":"GET","path":"/v1/namespaces/a/tables","status":403,"body":{"error":{"message":"denied","type":"ForbiddenException","code":403}}})",
	    204);
	auto tables = Query(test.connection, "SELECT table_name FROM duckdb_tables() WHERE database_name = 'reader'");
	REQUIRE(tables->Collection().Count() == (expected.empty() ? 0 : 1));
	if (!expected.empty()) {
		REQUIRE(tables->Collection().GetValue(0, 0).GetValue<string>() == expected);
	}
}

TEST_CASE("Ready prefetched listings are adopted before foreground discovery", "[iceberg_prefetch]") {
	PrefetchTest test;
	test.Seed("a");
	test.Attach(test.connection, ", TABLE_RESOLUTION 'eager', MAX_TABLE_STALENESS '5 minutes'");
	Query(test.connection, "BEGIN");
	test.WaitForCache("a");
	Query(test.connection, "COMMIT");
	for (auto path : {"/v1/namespaces", "/v1/namespaces/a/tables"}) {
		test.Request(
		    "POST", "/__test/response",
		    Json::Object(
		        {{"method", "GET"},
		         {"path", path},
		         {"status", int64_t(403)},
		         {"body", Json::Parse(R"({"error":{"message":"denied","type":"ForbiddenException","code":403}})")}})
		        .Dump(),
		    204);
	}
	auto tables =
	    Query(test.connection, "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'reader'");
	REQUIRE(tables->Collection().Count() == 1);
	REQUIRE(tables->Collection().GetValue(0, 0).GetValue<string>() == "a");
	REQUIRE(tables->Collection().GetValue(1, 0).GetValue<string>() == "t");
}

TEST_CASE("Background discovery does not retain the database", "[iceberg_prefetch]") {
	weak_ptr<DatabaseInstance> lifetime;
	{
		PrefetchTest test;
		lifetime = test.database.instance;
		test.Attach(test.connection);
		// Leave the catalog attached when the database is destroyed.
		test.detach_on_destroy = false;
	}
	REQUIRE(lifetime.expired());
}

TEST_CASE("ATTACH works without background workers and tolerates failed discovery", "[iceberg_prefetch]") {
	PrefetchTest test;
	test.Seed("a");
	Query(test.connection, "SET async_threads = 0");
	test.Attach(test.connection);
	Query(test.connection, "SELECT * FROM reader.a.t");
	Query(test.connection, "DETACH reader");
	Query(test.connection, "SET async_threads = 4");
	test.Request(
	    "POST", "/__test/response",
	    R"({"method":"GET","path":"/v1/namespaces","status":403,"body":{"error":{"message":"denied","type":"ForbiddenException","code":403}}})",
	    204);
	test.Attach(test.connection);
	Query(test.connection, "SELECT * FROM reader.a.t");
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
