#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/logging/log_type.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/storage/iceberg_table_secret_provider.hpp"

#include <condition_variable>
#include <thread>

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
		auto replacement =
		    make_shared_ptr<rest_api_objects::LoadTableResult>(publication->cached_result->result.Copy());
		(*replacement->config)["test-version"] = "new";
		test.Cache().SetOrOverwrite(test.key, make_shared_ptr<const IcebergLoadTableResponse>(std::move(*replacement)));
		expected_etag.clear();
	}

	test.Respond("\"A\"", 304);
	test.Query("CALL truncate_duckdb_logs()");
	auto result = request.Execute(*test.connection.context, test.Catalog());
	REQUIRE(result.status_ == HTTPStatusCode::NotModified_304);
	REQUIRE(result.result_);
	REQUIRE(result.result_->result.config->at("test-version") == "old");
	REQUIRE(test.LastValidator().ToString() == "\"A\"");
	if (publication) {
		REQUIRE_FALSE(publication->TryPublish(std::move(result.result_), std::move(result.etag)));
	}
	auto current = test.Cache().BeginLoad(test.key);
	REQUIRE(current->cached_result);
	REQUIRE(current->cached_result->result.config->at("test-version") == "new");
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
	REQUIRE(result.result_->result.config->at("test-version") == "old");
	REQUIRE_FALSE(publication->TryPublish(std::move(result.result_), std::move(result.etag)));
	auto current = test.Cache().BeginLoad(test.key);
	REQUIRE_FALSE(current->cached_result);
	REQUIRE(current->etag.empty());
}

TEST_CASE("LoadTable refresh revalidates metadata while credential refresh remains unconditional", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto &table = test.Table();
	test.Catalog().attach_options.access_mode = IRCAccessDelegationMode::VENDED_CREDENTIALS;
	test.Respond("\"A\"", 200, R"({"metadata":{"location":"s3://test/t"},
	    "config":{"s3.region":"us-east-1","s3.session-token":"EXPIRED_TOKEN","s3.session-token-expires-at-ms":"1"}})");
	table.RefreshFromCatalog(*test.connection.context);
	auto snapshot_id = table.GetMetadata().current_snapshot_id;
	auto parsed = table.GetMetadataOwner();
	test.Respond("\"B\"", 304);
	test.Query("CALL truncate_duckdb_logs()");
	table.RefreshFromCatalog(*test.connection.context);
	REQUIRE(table.GetMetadata().current_snapshot_id == snapshot_id);
	REQUIRE(table.GetMetadataOwner() == parsed);
	REQUIRE(test.LastValidator().ToString() == "\"A\"");
	{
		auto current = test.Cache().BeginLoad(test.key);
		REQUIRE(current->etag == "\"B\"");
	}
	auto prepared = table.GetVendedCredentials(*test.connection.context);
	REQUIRE(prepared.config);
	REQUIRE(prepared.config->provider == IcebergTableSecretProvider::Provider());
	REQUIRE(prepared.config->options.at("session_token").ToString() == "EXPIRED_TOKEN");
	REQUIRE(prepared.config->options.at("expires_at").ToString() == "1");

	test.Catalog().supported_urls.erase("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials");
	test.Respond("\"B\"", 200, R"({"config":{"s3.region":"us-east-1","s3.access-key-id":"NEW_CONFIG_KEY"},
	    "storage-credentials":[{"prefix":"s3://test/","config":{"s3.access-key-id":"NEW_SCOPED_KEY"}}]})");
	test.Query("CALL truncate_duckdb_logs()");
	auto credentials = IRCAPI::GetTableCredentials(*test.connection.context, test.Catalog(), table.schema, table.name);
	REQUIRE_FALSE(credentials.error_);
	REQUIRE(credentials.result_.config);
	REQUIRE(credentials.result_.config->at("s3.access-key-id") == "NEW_CONFIG_KEY");
	REQUIRE(credentials.result_.storage_credentials.size() == 1);
	REQUIRE(credentials.result_.storage_credentials[0].config.at("s3.access-key-id") == "NEW_SCOPED_KEY");
	REQUIRE(test.LastValidator().IsNull());

	// Exercise the provider HTTPFS calls when the credential from the 304 expires.
	auto &refresh = *prepared.config;
	refresh.options["catalog_name"] = Value("lake");
	refresh.options["schema"] = Value("default");
	refresh.options["table"] = Value("t");
	test.Query("CALL truncate_duckdb_logs()");
	auto secret = IcebergTableSecretProvider::CreateSecret(*test.connection.context, refresh);
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret);
	REQUIRE(kv_secret.TryGetValue("key_id").ToString() == "NEW_SCOPED_KEY");
	REQUIRE(kv_secret.TryGetValue("session_token").IsNull());
	REQUIRE(test.LastValidator().IsNull());

	test.Respond("\"B\"", 304);
	REQUIRE_THROWS_WITH(IRCAPI::GetTableCredentials(*test.connection.context, test.Catalog(), table.schema, table.name),
	                    Catch::Contains("HTTP 304 without a cached response"));
}

TEST_CASE("Parsed LoadTable generations survive replacement and rejected publication", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto &table = test.Table();
	auto first = test.Cache().BeginLoad(test.key);
	auto response = first->cached_result;
	auto parsed = response->GetMetadata(*test.connection.context, table);
	REQUIRE(parsed == table.GetMetadataOwner());
	auto snapshot = parsed->GetLatestSnapshot();
	REQUIRE(snapshot);

	test.Respond("\"rotated\"", 304);
	auto unchanged =
	    IcebergLoadTableRequest({"default"}, "t", first.get()).Execute(*test.connection.context, test.Catalog());
	REQUIRE(unchanged.result_ == response);
	REQUIRE(unchanged.result_->GetMetadata(*test.connection.context, table) == parsed);
	REQUIRE(first->TryPublish(unchanged.result_, unchanged.etag));
	first.reset();

	// A request owns A even if B publishes before its 304 completes.
	auto late = test.Cache().BeginLoad(test.key);
	IcebergLoadTableRequest request({"default"}, "t", late.get());
	SECTION("A new 200 uses the same ETag but different metadata") {
		test.Respond("\"rotated\"", 200, R"({"metadata":{"properties":{"owner-test":"new"}}})");
		table.RefreshFromCatalog(*test.connection.context);
		REQUIRE(table.GetMetadataOwner() != parsed);
		REQUIRE(table.GetMetadata().GetTableProperty("owner-test") == "new");
	}
	SECTION("Eviction releases the cache while an old reader stays active") {
		test.Cache().EvictIfCurrent(table);
		auto empty = test.Cache().BeginLoad(test.key);
		REQUIRE_FALSE(empty->cached_result);
	}
	test.Respond("\"late\"", 304);
	auto old = request.Execute(*test.connection.context, test.Catalog());
	REQUIRE(old.result_ == response);
	REQUIRE(old.result_->GetMetadata(*test.connection.context, table) == parsed);
	REQUIRE_FALSE(late->TryPublish(old.result_, old.etag));
	REQUIRE(parsed->GetLatestSnapshot().get() == snapshot.get());
	REQUIRE(parsed->GetTableProperty("owner-test").empty());
}

TEST_CASE("Staleness reads and table copies share metadata until a writer detaches", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto &table = test.Table();
	auto parsed = table.GetMetadataOwner();
	auto copy = table.Copy();
	auto &transaction = IcebergTransaction::Get(*test.connection.context, test.Catalog());
	auto transaction_copy = table.Copy(transaction);
	REQUIRE(copy.GetMetadataOwner() == parsed);
	REQUIRE(transaction_copy.GetMetadataOwner() == parsed);

	// The existing entry has no staleness expiry; republish it with a configured lifetime.
	test.Catalog().attach_options.max_table_staleness_micros = 3600000000ULL;
	auto publication = test.Cache().BeginLoad(test.key);
	REQUIRE(publication->TryPublish(publication->cached_result, publication->etag));
	auto reader = IcebergTable::CreatePlaceholder(test.Catalog(), table.schema, table.name);
	REQUIRE(
	    test.Cache().Get(*test.connection.context, test.key, [&](shared_ptr<const IcebergLoadTableResponse> response) {
		    reader->InitializeFromCatalogResponse(*test.connection.context, std::move(response));
	    }));
	REQUIRE(reader->GetMetadataOwner() == parsed);

	auto &writable = copy.GetMetadataMutable();
	writable.table_properties["owner-test"] = "private";
	REQUIRE(copy.GetMetadataOwner() != parsed);
	REQUIRE(reader->GetMetadata().GetTableProperty("owner-test").empty());
	REQUIRE(table.GetMetadata().GetTableProperty("owner-test").empty());
	REQUIRE(transaction_copy.GetMetadataOwner() == parsed);
	// A scan may retain the private version while a later statement writes again.
	auto scan_owner = copy.GetMetadataOwner();
	copy.GetMetadataMutable().table_properties["owner-test"] = "next";
	REQUIRE(scan_owner->GetTableProperty("owner-test") == "private");
	REQUIRE(copy.GetMetadata().GetTableProperty("owner-test") == "next");
	REQUIRE(scan_owner != copy.GetMetadataOwner());
}

TEST_CASE("Authoritative metadata fallback does not poison a parsed generation", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto &table = test.Table();
	test.Catalog().attach_options.use_metadata_location = true;
	auto wire = table.initialization_source->result.Copy();
	const auto path = test.root + "/authoritative.metadata.json";
	wire.metadata_location = path;
	auto response = make_shared_ptr<const IcebergLoadTableResponse>(std::move(wire));
	auto fallback = response->GetMetadata(*test.connection.context, table);
	auto authoritative = fallback->Copy();
	authoritative.table_properties["owner-test"] = "file";
	authoritative.WriteMetadata(*test.connection.context, path);
	auto from_file = response->GetMetadata(*test.connection.context, table);
	REQUIRE(from_file != fallback);
	REQUIRE(from_file->GetTableProperty("owner-test") == "file");
	REQUIRE(fallback->GetTableProperty("owner-test").empty());
	REQUIRE(response->GetMetadata(*test.connection.context, table) == from_file);
	// A different response generation with no authoritative location must use its embedded metadata.
	auto embedded_wire = table.initialization_source->result.Copy();
	embedded_wire.metadata_location.reset();
	auto embedded = make_shared_ptr<const IcebergLoadTableResponse>(std::move(embedded_wire));
	REQUIRE(embedded->GetMetadata(*test.connection.context, table) != from_file);
	REQUIRE(embedded->GetMetadata(*test.connection.context, table)->GetTableProperty("owner-test").empty());
}

TEST_CASE("Concurrent readers share one parsed payload and release superseded generations", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto &table = test.Table();
	auto response = make_shared_ptr<const IcebergLoadTableResponse>(table.initialization_source->result.Copy());
	test.Cache().SetOrOverwrite(test.key, response);
	weak_ptr<const IcebergLoadTableResponse> response_lifetime(response);
	Connection left(test.database);
	Connection right(test.database);
	shared_ptr<const IcebergTableMetadata> readers[2];
	std::exception_ptr errors[2];
	std::mutex gate;
	std::condition_variable ready;
	idx_t waiting = 0;
	bool start = false;
	auto read = [&](idx_t index, Connection &connection) {
		{
			std::unique_lock<std::mutex> guard(gate);
			waiting++;
			ready.notify_all();
			ready.wait(guard, [&] { return start; });
		}
		try {
			readers[index] = response->GetMetadata(*connection.context, table);
		} catch (...) {
			errors[index] = std::current_exception();
		}
	};
	// Only immutable metadata is shared; each thread has its own ClientContext. No catalog or file mutations.
	std::thread first(read, 0, std::ref(left));
	std::thread second(read, 1, std::ref(right));
	{
		std::unique_lock<std::mutex> guard(gate);
		ready.wait(guard, [&] { return waiting == 2; });
		start = true;
	}
	ready.notify_all();
	first.join();
	second.join();
	for (auto &error : errors) {
		if (error) {
			std::rethrow_exception(error);
		}
	}
	REQUIRE(readers[0] == readers[1]);
	REQUIRE(readers[0] != table.GetMetadataOwner());
	weak_ptr<const IcebergTableMetadata> parsed_lifetime(readers[0]);
	test.Cache().SetOrOverwrite(test.key, table.initialization_source);
	response.reset();
	REQUIRE(response_lifetime.expired());
	REQUIRE_FALSE(parsed_lifetime.expired());
	REQUIRE(readers[0]->GetLatestSnapshot());
	readers[0].reset();
	REQUIRE_FALSE(parsed_lifetime.expired());
	readers[1].reset();
	REQUIRE(parsed_lifetime.expired());
}

TEST_CASE("Historical transaction metadata has its own owner outside the head cache", "[iceberg_cache]") {
	LoadTableCacheTest test;
	auto &table = test.Table();
	auto head = table.GetMetadataOwner();
	auto historical = head->Copy();
	historical.table_properties["owner-test"] = "historical";
	const auto path = test.root + "/historical.metadata.json";
	historical.WriteMetadata(*test.connection.context, path);

	auto future_table = table.Copy();
	auto &future = future_table.GetMetadataMutable();
	auto start = IcebergUtils::GetTransactionStartTimeMS(*test.connection.context);
	future.last_updated_ms = timestamp_ms_t(start.value + 60000);
	future.metadata_log.clear();
	future.metadata_log.emplace_back(path, timestamp_ms_t(start.value - 1));
	future.table_properties["owner-test"] = "future";
	auto &transaction = IcebergTransaction::Get(*test.connection.context, test.Catalog());
	auto reconstructed = future_table.Copy(transaction);
	REQUIRE(reconstructed.GetMetadataOwner() != head);
	REQUIRE(reconstructed.GetMetadataOwner() != future_table.GetMetadataOwner());
	REQUIRE(reconstructed.GetMetadata().GetTableProperty("owner-test") == "historical");
	REQUIRE(future_table.GetMetadata().GetTableProperty("owner-test") == "future");
	auto current = test.Cache().BeginLoad(test.key);
	REQUIRE(current->cached_result->GetMetadata(*test.connection.context, table) == head);
}

TEST_CASE("LoadTable does not revalidate expired Azure vended credentials", "[iceberg_cache]") {
	for (const string scheme : {"abfs", "abfss", "az"}) {
		for (const bool scoped : {false, true}) {
			DYNAMIC_SECTION(scheme << (scoped ? " scoped credentials" : " config credentials")) {
				LoadTableCacheTest test;
				auto &schema = test.Table().schema;
				auto &catalog = test.Catalog();
				catalog.attach_options.access_mode = IRCAccessDelegationMode::VENDED_CREDENTIALS;
				const auto location = scheme + "://container@account.dfs.core.windows.net/t";
				const string expired = "se=2000-01-01T00%3A00%3A00Z&sig=expired";
				const string fresh = "se=2999-01-01T00%3A00%3A00Z&sig=fresh";
				auto respond = [&](const string &token) {
					const auto config = R"({"adls.sas-token.account.dfs.core.windows.net":")" + token + "\"}";
					auto patch = "{\"metadata\":{\"location\":\"" + location + "\"},";
					if (scoped) {
						patch +=
						    "\"storage-credentials\":[{\"prefix\":\"" + location + "\",\"config\":" + config + "}]";
					} else {
						patch += "\"config\":" + config;
					}
					test.Respond("\"A\"", 200, patch + "}");
				};

				// Seed an old cached response whose SAS has expired, without a timing-dependent sleep.
				respond(expired);
				auto initial = IcebergLoadTableRequest({"default"}, "t").Execute(*test.connection.context, catalog);
				REQUIRE_FALSE(initial.error_);
				auto publication = test.Cache().BeginLoad(test.key);
				REQUIRE(publication->TryPublish(initial.result_, initial.etag));
				IcebergTable old_table(catalog, schema, "t", initial.result_);
				auto old_credentials = old_table.GetVendedCredentials(*test.connection.context);
				auto &old_secret = scoped ? old_credentials.storage_credentials.at(0) : *old_credentials.config;
				REQUIRE(old_secret.provider == "config");
				REQUIRE(StringUtil::Contains(old_secret.options.at("connection_string").ToString(), expired));

				test.Respond("\"A\"", 304);
				test.Query("CALL truncate_duckdb_logs()");
				auto reload = test.Cache().BeginLoad(test.key);
				REQUIRE_THROWS_WITH(
				    IcebergLoadTableRequest({"default"}, "t", reload.get()).Execute(*test.connection.context, catalog),
				    Catch::Contains("HTTP 304 without a cached response to revalidate"));
				REQUIRE(test.LastValidator().IsNull());

				// Identical metadata/ETag can carry a new SAS; always obtain the full response.
				respond(fresh);
				test.Query("CALL truncate_duckdb_logs()");
				auto result =
				    IcebergLoadTableRequest({"default"}, "t", reload.get()).Execute(*test.connection.context, catalog);
				REQUIRE_FALSE(result.error_);
				REQUIRE(result.status_ == HTTPStatusCode::OK_200);
				REQUIRE(test.LastValidator().IsNull());
				IcebergTable fresh_table(catalog, schema, "t", result.result_);
				auto credentials = fresh_table.GetVendedCredentials(*test.connection.context);
				auto &secret = scoped ? credentials.storage_credentials.at(0) : *credentials.config;
				REQUIRE(StringUtil::Contains(secret.options.at("connection_string").ToString(), fresh));
				REQUIRE(reload->TryPublish(result.result_, result.etag));

				// With user-managed credentials, the Azure metadata remains safe to revalidate.
				catalog.attach_options.access_mode = IRCAccessDelegationMode::NONE;
				test.Respond("\"A\"", 304);
				test.Query("CALL truncate_duckdb_logs()");
				auto metadata_only = test.Cache().BeginLoad(test.key);
				auto unchanged = IcebergLoadTableRequest({"default"}, "t", metadata_only.get())
				                     .Execute(*test.connection.context, catalog);
				REQUIRE(unchanged.status_ == HTTPStatusCode::NotModified_304);
				REQUIRE(unchanged.result_ == result.result_);
				REQUIRE(test.LastValidator().ToString() == "\"A\"");
			}
		}
	}
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
