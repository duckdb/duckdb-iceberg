#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
// Catch includes Windows headers, whose DELETE macro conflicts with Iceberg's enums.
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include <algorithm>

using namespace duckdb;

namespace {

unique_ptr<QueryResult> Query(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	result->Collection();
	return result;
}

Value Scalar(Connection &connection, const string &sql) {
	auto result = Query(connection, sql);
	REQUIRE(result->Collection().Count() == 1);
	return result->Collection().GetValue(0, 0);
}

void CheckRows(Connection &connection, const vector<int32_t> &expected, const string &suffix = "") {
	auto result = Query(connection, "SELECT id FROM lake.default.t " + suffix + " ORDER BY id");
	REQUIRE(result->Collection().Count() == expected.size());
	for (idx_t i = 0; i < expected.size(); i++) {
		REQUIRE(result->Collection().GetValue(0, i) == Value::INTEGER(expected[i]));
	}
}

struct ReplacementTest {
	explicit ReplacementTest(int version = 2, bool populated = true)
	    : root(FileSystem::CreateLocal()->JoinPath(FileSystem::GetWorkingDirectory(), "duckdb_unittest_tempdir",
	                                               "replacement-api-" + UUID::ToString(UUID::GenerateRandomUUID()))),
	      database(nullptr), connection(database) {
		Query(connection, "CALL iceberg_test_catalog_init('" + root + "')");
		Query(connection, "ATTACH '' AS lake (TYPE ICEBERG, URI 'http://iceberg-mock.invalid', "
		                  "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', STAGE_CREATE_TABLES true)");
		Query(connection, "CREATE SCHEMA lake.default");
		Query(connection,
		      "CREATE TABLE lake.default.t (id INTEGER) WITH ('format-version' = " + std::to_string(version) +
		          ", 'commit.manifest-merge.enabled' = true, "
		          "'commit.manifest.min-count-to-merge' = 1)");
		if (populated) {
			Query(connection, "INSERT INTO lake.default.t VALUES (1), (2)");
			Query(connection,
			      "SET VARIABLE old_snapshot = (SELECT snapshot_id FROM iceberg_snapshots(lake.default.t))");
		}
	}

	~ReplacementTest() {
		connection.Query("ROLLBACK");
		FileSystem::CreateLocal()->RemoveDirectory(root);
	}

	IcebergCatalog &Catalog() {
		return duckdb::Catalog::GetCatalog(*connection.context, Identifier("lake")).Cast<IcebergCatalog>();
	}

	IcebergTable &WritableTable() {
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
		                       QualifiedName(Identifier("lake"), Identifier("default"), Identifier("t")));
		auto &table =
		    duckdb::Catalog::GetEntry(*connection.context, lookup).Cast<IcebergTableSchemaVersion>().table_info;
		auto &transaction = IcebergTransaction::Get(*connection.context, Catalog());
		auto &updated_table = transaction.GetOrCreateAlter().GetOrInitializeTable(table);
		updated_table.GetOrCreateTransactionData(transaction);
		return updated_table;
	}

	void Replace() {
		auto &table = WritableTable();
		table.transaction_data->TableReplaceContents(table.GetMetadataMutable());
	}

	void Define(const string &definition) {
		Parser parser(*connection.context);
		parser.ParseQuery("CREATE TABLE lake.default.t " + definition);
		auto &statement = parser.statements[0]->Cast<CreateStatement>();
		auto binder = Binder::CreateBinder(*connection.context);
		auto info = binder->BindCreateTableInfo(std::move(statement.info), WritableTable().schema);
		IcebergTableSet::ReplaceDefinition(*connection.context, WritableTable(), info->Base());
	}

	string root;
	DuckDB database;
	Connection connection;
};

} // namespace

TEST_CASE("Replacement detaches main while preserving historical metadata and other refs", "[iceberg_replacement]") {
	ReplacementTest test;
	Query(test.connection, "BEGIN");
	auto &table = test.WritableTable();
	auto original = table.GetMetadataOwner();
	auto &metadata = table.GetMetadataMutable();
	auto &data = *table.transaction_data;
	const auto old_id = *metadata.current_snapshot_id;
	// The mock only publishes main. Exercise preservation of other references directly.
	metadata.refs->additional_properties["branch"] = metadata.refs->additional_properties.at("main").Copy();
	metadata.refs->additional_properties["tag"] = metadata.refs->additional_properties.at("main").Copy();
	metadata.refs->additional_properties.at("tag").type = "tag";
	data.base_snapshot_id = old_id;
	data.existing_manifest_list.emplace_back(
	    IcebergManifestFile("unread.avro", 0, old_id, IcebergManifest(0, IcebergManifestContentType::DATA, 1)));
	data.TableReplaceContents(metadata);
	REQUIRE_FALSE(metadata.current_snapshot_id);
	REQUIRE(metadata.refs->additional_properties.count("main") == 0);
	REQUIRE(metadata.refs->additional_properties.size() == 2);
	REQUIRE(metadata.refs->additional_properties.at("branch").snapshot_id == old_id);
	REQUIRE(metadata.refs->additional_properties.at("tag").type == "tag");
	REQUIRE(metadata.snapshots.size() == original->snapshots.size());
	REQUIRE(metadata.snapshot_log == original->snapshot_log);
	REQUIRE(metadata.last_sequence_number == original->last_sequence_number);
	REQUIRE(metadata.table_uuid == original->table_uuid);
	REQUIRE(metadata.location == original->location);
	REQUIRE(metadata.GetCurrentSchemaId() == original->GetCurrentSchemaId());
	REQUIRE(data.initial_snapshot_id == original->current_snapshot_id);
	REQUIRE(data.existing_manifest_list.empty());
	REQUIRE_FALSE(data.base_snapshot_id);
	REQUIRE_FALSE(data.SupportsAppendRetry());
	// The metadata owner held by existing readers is unchanged.
	REQUIRE(original->current_snapshot_id);
	REQUIRE(original->refs->additional_properties.count("main") == 1);
}

TEST_CASE("Replacement commits or rolls back through the normal transaction machinery", "[iceberg_replacement]") {
	for (auto version : {2, 3}) {
		DYNAMIC_SECTION("format version " << version) {
			CAPTURE(version);
			ReplacementTest test(version);
			Connection observer(test.database);
			Query(test.connection, "BEGIN");
			test.Replace();
			REQUIRE(Scalar(test.connection, "SELECT count(*) FROM lake.default.t") == Value::BIGINT(0));
			CheckRows(test.connection, {1, 2}, "AT (VERSION => getvariable('old_snapshot'))");
			CheckRows(observer, {1, 2});
			SECTION("Commit without data leaves no current snapshot") {
				Query(test.connection, "COMMIT");
				REQUIRE(Scalar(observer,
				               "SELECT metadata.\"current-snapshot-id\" IS NULL AND metadata.refs.main IS NULL "
				               "FROM iceberg_load_table_response(lake.default.t)") == Value::BOOLEAN(true));
				REQUIRE(Scalar(observer, "SELECT count(*) FROM lake.default.t") == Value::BIGINT(0));
			}
			SECTION("Inserts start a new lineage without resetting sequence or row IDs") {
				Query(test.connection, "INSERT INTO lake.default.t VALUES (7)");
				Query(test.connection, "INSERT INTO lake.default.t VALUES (8)");
				Query(test.connection, "COMMIT");
				CheckRows(observer, {7, 8});
				REQUIRE(Scalar(observer, "SELECT parent_id IS NULL AND operation = 'append' "
				                         "FROM iceberg_snapshots(lake.default.t) WHERE sequence_number = 2") ==
				        Value::BOOLEAN(true));
				REQUIRE(Scalar(observer,
				               "SELECT tip.parent_id = base.snapshot_id FROM iceberg_snapshots(lake.default.t) tip, "
				               "iceberg_snapshots(lake.default.t) base "
				               "WHERE tip.sequence_number = 3 AND base.sequence_number = 2") == Value::BOOLEAN(true));
				if (version == 3) {
					REQUIRE(Scalar(observer, "SELECT _row_id FROM lake.default.t ORDER BY _row_id LIMIT 1") ==
					        Value::BIGINT(2));
				}
			}
			SECTION("Rollback discards replacement and its inserts") {
				Query(test.connection, "INSERT INTO lake.default.t VALUES (7)");
				Query(test.connection, "ROLLBACK");
				CheckRows(observer, {1, 2});
				REQUIRE(Scalar(observer, "SELECT count(*) FROM iceberg_snapshots(lake.default.t)") == Value::BIGINT(1));
			}
			CheckRows(test.connection, {1, 2}, "AT (VERSION => getvariable('old_snapshot'))");
		}
	}
}

TEST_CASE("Replacement asserts the original head even if it had no snapshot", "[iceberg_replacement]") {
	for (bool single_table_commit : {false, true}) {
		for (bool populated : {false, true}) {
			for (bool insert_after_replace : {false, true}) {
				CAPTURE(single_table_commit, populated, insert_after_replace);
				ReplacementTest test(2, populated);
				Connection writer(test.database);
				Query(test.connection, "BEGIN");
				test.Catalog().attach_options.disable_multi_table_commit = single_table_commit;
				test.Replace();
				if (insert_after_replace) {
					Query(test.connection, "INSERT INTO lake.default.t VALUES (7)");
				}
				Query(writer, "INSERT INTO lake.default.t VALUES (99)");
				auto commit = test.connection.Query("COMMIT");
				REQUIRE(commit->HasError());
				REQUIRE_THAT(commit->GetError(), Catch::Contains("assert-ref-snapshot-id"));
				CheckRows(writer, populated ? vector<int32_t> {1, 2, 99} : vector<int32_t> {99});
			}
		}
	}
}

TEST_CASE("Replacement rejects concurrent schema changes without partial publication", "[iceberg_replacement]") {
	ReplacementTest test;
	Connection writer(test.database);
	Query(test.connection, "CREATE TABLE lake.default.companion (id INTEGER)");
	Query(test.connection, "BEGIN");
	test.Replace();
	Query(test.connection, "INSERT INTO lake.default.companion VALUES (7)");
	Query(writer, "ALTER TABLE lake.default.t ADD COLUMN added INTEGER");
	auto commit = test.connection.Query("COMMIT");
	REQUIRE(commit->HasError());
	REQUIRE_THAT(commit->GetError(), Catch::Contains("assert-current-schema-id"));
	REQUIRE(Scalar(writer, "SELECT count(*) FROM lake.default.companion") == Value::BIGINT(0));
	CheckRows(writer, {1, 2}, "WHERE added IS NULL");
}

TEST_CASE("Replacement rejects earlier data changes and rollback without changing transaction state",
          "[iceberg_replacement]") {
	ReplacementTest test;
	Query(test.connection, "INSERT INTO lake.default.t VALUES (3)");
	Query(test.connection, "BEGIN");
	vector<int32_t> expected {1, 2, 3};
	SECTION("Insert") {
		Query(test.connection, "INSERT INTO lake.default.t VALUES (7)");
		expected = {1, 2, 3, 7};
	}
	SECTION("Delete") {
		Query(test.connection, "DELETE FROM lake.default.t WHERE id = 1");
		expected = {2, 3};
	}
	SECTION("Update") {
		Query(test.connection, "UPDATE lake.default.t SET id = 7 WHERE id = 1");
		expected = {2, 3, 7};
	}
	SECTION("Rewrite") {
		Query(test.connection, "CALL iceberg_rewrite_data_files('lake.default.t', rewrite_all = true)");
	}
	SECTION("Rollback to a snapshot") {
		Query(test.connection, "CALL iceberg_rollback_to_snapshot('lake.default.t', getvariable('old_snapshot'))");
		expected = {1, 2};
	}
	auto &table = test.WritableTable();
	auto &data = *table.transaction_data;
	// Materialize the transaction-local copy before comparing JSON: copying unordered maps can reorder it.
	auto before = table.GetMetadataMutable().ToJSON();
	auto update_count = data.updates.size();
	REQUIRE_THROWS_WITH(test.Replace(), Catch::Contains("it was already changed in this transaction"));
	REQUIRE(table.GetMetadata().ToJSON() == before);
	REQUIRE(data.updates.size() == update_count);
	REQUIRE_FALSE(data.replaces_table);
	Query(test.connection, "COMMIT");
	CheckRows(test.connection, expected);
}

TEST_CASE("Replacement supports updates and deletes of its new rows", "[iceberg_replacement]") {
	for (auto version : {2, 3}) {
		CAPTURE(version);
		ReplacementTest test(version);
		Connection observer(test.database);
		Query(test.connection, "BEGIN");
		test.Replace();
		Query(test.connection, "INSERT INTO lake.default.t VALUES (7), (8), (9)");
		Query(test.connection, "UPDATE lake.default.t SET id = 17 WHERE id = 7");
		Query(test.connection, "DELETE FROM lake.default.t WHERE id = 8");
		CheckRows(test.connection, {9, 17});
		CheckRows(observer, {1, 2});
		Query(test.connection, "COMMIT");
		CheckRows(observer, {9, 17});
		CheckRows(test.connection, {1, 2}, "AT (VERSION => getvariable('old_snapshot'))");
		REQUIRE(Scalar(observer, "SELECT count(*) FROM iceberg_snapshots(lake.default.t)") == Value::BIGINT(4));
		REQUIRE(Scalar(observer, "SELECT parent_id IS NULL FROM iceberg_snapshots(lake.default.t) "
		                         "WHERE sequence_number = 2") == Value::BOOLEAN(true));
		if (version == 3) {
			REQUIRE(Scalar(observer, "SELECT count(DISTINCT _row_id) = count(*) AND min(_row_id) >= 2 "
			                         "FROM lake.default.t") == Value::BOOLEAN(true));
		}
	}
}

TEST_CASE("Replacement preserves schema and property changes in its transaction", "[iceberg_replacement]") {
	ReplacementTest test;
	Query(test.connection, "BEGIN");
	Query(test.connection, "ALTER TABLE lake.default.t ADD COLUMN note VARCHAR");
	test.Replace();
	Query(test.connection, "CALL set_iceberg_table_properties(lake.default.t, {'replacement-probe': 'yes'})");
	Query(test.connection, "INSERT INTO lake.default.t VALUES (7, 'replacement')");
	Query(test.connection, "COMMIT");
	CheckRows(test.connection, {7});
	REQUIRE(Scalar(test.connection, "SELECT note FROM lake.default.t") == Value("replacement"));
	REQUIRE(Scalar(test.connection, "SELECT value FROM iceberg_table_properties(lake.default.t) "
	                                "WHERE key = 'replacement-probe'") == Value("yes"));
	CheckRows(test.connection, {1, 2}, "AT (VERSION => getvariable('old_snapshot'))");
}

TEST_CASE("Replacement and subsequent insertion do not open old manifests", "[iceberg_replacement]") {
	ReplacementTest test(2, false);
	REQUIRE(Scalar(test.connection, R"(
		SELECT status FROM iceberg_test_catalog_request('POST', '/v1/namespaces/default/tables/t',
		json_object('requirements', json_array(), 'updates', json_array(
			json_object('action', 'add-snapshot', 'snapshot', json_object(
				'snapshot-id', 123, 'sequence-number', 1, 'timestamp-ms', )" +
	                                    std::to_string(Timestamp::GetCurrentTimestamp().value / 1000) + R"(,
				'schema-id', 0, 'manifest-list', 'unavailable-replacement-manifest.avro',
				'summary', json_object('operation', 'append', 'total-records', '1'))),
			json_object('action', 'set-snapshot-ref', 'ref-name', 'main', 'type', 'branch', 'snapshot-id', 123))))
	)") == Value::INTEGER(200));
	auto scan = test.connection.Query("SELECT * FROM lake.default.t");
	REQUIRE(scan->HasError());
	REQUIRE_THAT(scan->GetError(), Catch::Contains("unavailable-replacement-manifest.avro"));
	Query(test.connection, "BEGIN");
	test.Replace();
	SECTION("Insert before committing the replacement") {
		Query(test.connection, "INSERT INTO lake.default.t VALUES (7)");
		Query(test.connection, "COMMIT");
	}
	SECTION("Insert after committing the replacement") {
		Query(test.connection, "COMMIT");
		Query(test.connection, "INSERT INTO lake.default.t VALUES (7)");
	}
	REQUIRE(Scalar(test.connection, "SELECT id FROM lake.default.t") == Value::INTEGER(7));
	REQUIRE(Scalar(test.connection, "SELECT count(*) FROM iceberg_snapshots(lake.default.t)") == Value::BIGINT(2));
}

TEST_CASE("Snapshotless replacement succeeds and SQL replacement remains unsupported", "[iceberg_replacement]") {
	ReplacementTest test(2, false);
	Query(test.connection, "BEGIN");
	// Older metadata may omit refs entirely.
	test.WritableTable().GetMetadataMutable().refs.reset();
	test.Replace();
	Query(test.connection, "COMMIT");
	REQUIRE(Scalar(test.connection, "SELECT count(*) FROM iceberg_snapshots(lake.default.t)") == Value::BIGINT(0));
	Query(test.connection, "INSERT INTO lake.default.t VALUES (7)");
	REQUIRE(Scalar(test.connection, "SELECT parent_id IS NULL FROM iceberg_snapshots(lake.default.t)") ==
	        Value::BOOLEAN(true));
	auto replace = test.connection.Query("CREATE OR REPLACE TABLE lake.default.t (changed VARCHAR)");
	REQUIRE(replace->HasError());
	REQUIRE_THAT(replace->GetError(), Catch::Contains("CREATE OR REPLACE not supported"));
	REQUIRE(Scalar(test.connection, "SELECT id FROM lake.default.t") == Value::INTEGER(7));
}

TEST_CASE("DELETE keeps its existing retry behavior when replacement leaves no snapshot", "[iceberg_replacement]") {
	ReplacementTest test;
	Query(test.connection, "ALTER TABLE lake.default.t SET ('write.delete.isolation-level' = 'snapshot', "
	                       "'write.delete.mode' = 'merge-on-read')");
	Connection writer(test.database);
	Query(writer, "BEGIN");
	Query(writer, "DELETE FROM lake.default.t WHERE id = 1");
	Query(test.connection, "BEGIN");
	test.Replace();
	Query(test.connection, "COMMIT");
	Query(writer, "COMMIT");
	REQUIRE(Scalar(test.connection, "SELECT count(*) FROM lake.default.t") == Value::BIGINT(0));
}

TEST_CASE("Replacement definitions retain nested field IDs above a historical allocation watermark",
          "[iceberg_replacement]") {
	ReplacementTest test;
	Query(test.connection, "ALTER TABLE lake.default.t ADD COLUMN s STRUCT(x INTEGER, y VARCHAR)");
	Query(test.connection, "ALTER TABLE lake.default.t ADD COLUMN l STRUCT(k INTEGER)[]");
	Query(test.connection, "ALTER TABLE lake.default.t ADD COLUMN m MAP(VARCHAR, STRUCT(v INTEGER))");
	Query(test.connection, "ALTER TABLE lake.default.t ADD COLUMN retired INTEGER");
	Query(test.connection, "ALTER TABLE lake.default.t DROP COLUMN retired");
	Query(test.connection, "BEGIN");
	auto original = test.WritableTable().GetMetadataOwner();
	auto last_id = original->GetLastColumnId();
	test.Define("(m MAP(VARCHAR, STRUCT(v INTEGER, extra INTEGER)), l STRUCT(k INTEGER, extra INTEGER)[], "
	            "s STRUCT(y VARCHAR, x BIGINT, extra INTEGER), id BIGINT, added INTEGER)");
	auto &metadata = test.WritableTable().GetMetadata();
	auto fields = metadata.GetLatestSchema().GetFullNames();
	for (auto &old_field : original->GetLatestSchema().GetFullNames()) {
		auto match = std::find_if(fields.begin(), fields.end(),
		                          [&](const pair<int32_t, string> &field) { return field.second == old_field.second; });
		REQUIRE(match != fields.end());
		REQUIRE(match->first == old_field.first);
	}
	REQUIRE(metadata.GetLastColumnId() == last_id + 4);
	REQUIRE(metadata.current_snapshot_id == original->current_snapshot_id);
	test.Replace();
	Query(test.connection, "INSERT INTO lake.default.t VALUES ('{key={v:1, extra:2}}', "
	                       "'[{k:3, extra:4}]', '{y:yes, x:5, extra:6}', 7, 8)");
	Query(test.connection, "COMMIT");
	REQUIRE(Scalar(test.connection, "SELECT m = '{key={v:1, extra:2}}'::MAP(VARCHAR, STRUCT(v INTEGER, extra INTEGER)) "
	                                "AND l = '[{k:3, extra:4}]'::STRUCT(k INTEGER, extra INTEGER)[] "
	                                "AND s = '{y:yes, x:5, extra:6}'::STRUCT(y VARCHAR, x BIGINT, extra INTEGER) "
	                                "AND id = 7 AND added = 8 FROM lake.default.t") == Value::BOOLEAN(true));
	CheckRows(test.connection, {1, 2}, "AT (VERSION => getvariable('old_snapshot'))");
}

TEST_CASE("Replacement definitions reuse schemas without rewriting schema properties", "[iceberg_replacement]") {
	ReplacementTest test;
	Connection observer(test.database);
	Query(test.connection, "ALTER TABLE lake.default.t SET ('write.metadata.metrics.column.id' = 'full', "
	                       "'schema.name-mapping.default' = '[{\"field-id\":1,\"names\":[\"id\",\"old_id\"]}]')");
	Query(test.connection, "BEGIN");
	test.Define("(id VARCHAR)");
	test.Replace();
	Query(test.connection, "INSERT INTO lake.default.t VALUES ('new')");
	CheckRows(test.connection, {1, 2}, "AT (VERSION => getvariable('old_snapshot'))");
	CheckRows(observer, {1, 2});
	Query(test.connection, "COMMIT");
	REQUIRE(Scalar(test.connection, "SELECT id FROM lake.default.t") == Value("new"));
	CheckRows(test.connection, {1, 2}, "AT (VERSION => getvariable('old_snapshot'))");
	Query(test.connection, "BEGIN");
	test.Define("(id INTEGER)");
	REQUIRE(test.WritableTable().GetMetadata().GetCurrentSchemaId() == 0);
	REQUIRE(test.WritableTable().GetMetadata().GetLastColumnId() == 1);
	test.Replace();
	Query(test.connection, "COMMIT");
	Query(test.connection, "BEGIN");
	test.Define("(id INTEGER)");
	REQUIRE(test.WritableTable().GetMetadata().GetCurrentSchemaId() == 0);
	REQUIRE(test.WritableTable().GetMetadata().GetLastColumnId() == 1);
	Query(test.connection, "ROLLBACK");
	Query(test.connection, "BEGIN");
	test.Define("(\"ID\" INTEGER)");
	auto &metadata = test.WritableTable().GetMetadata();
	REQUIRE(metadata.GetLatestSchema().columns[0]->id == 2);
	REQUIRE(metadata.GetTableProperty("write.metadata.metrics.column.id") == "full");
	REQUIRE(metadata.GetTableProperty("schema.name-mapping.default") ==
	        "[{\"field-id\":1,\"names\":[\"id\",\"old_id\"]}]");
	test.Replace();
	Query(test.connection, "COMMIT");
	Query(test.connection, "BEGIN");
	test.Define("(id INTEGER)");
	REQUIRE(test.WritableTable().GetMetadata().GetLatestSchema().columns[0]->id == 3);
	Query(test.connection, "ROLLBACK");
}

TEST_CASE("Replacement definitions preserve or upgrade format and bind defaults and constraints",
          "[iceberg_replacement]") {
	ReplacementTest test;
	Query(test.connection, "SET iceberg_default_format_version = 3");
	Query(test.connection, "BEGIN");
	test.Define("(id INTEGER)");
	REQUIRE(test.WritableTable().GetMetadata().iceberg_version == 2);
	Query(test.connection, "ROLLBACK");
	Query(test.connection, "BEGIN");
	test.Define("(note VARCHAR, id BIGINT DEFAULT 7 NOT NULL) WITH ('FORMAT-VERSION' = 3)");
	test.Replace();
	Query(test.connection, "INSERT INTO lake.default.t DEFAULT VALUES");
	Query(test.connection, "COMMIT");
	REQUIRE(Scalar(test.connection, "SELECT id = 7 AND note IS NULL AND _row_id IS NOT NULL FROM lake.default.t") ==
	        Value::BOOLEAN(true));
	auto invalid = test.connection.Query("INSERT INTO lake.default.t VALUES ('invalid', NULL)");
	REQUIRE(invalid->HasError());
	REQUIRE_THAT(invalid->GetError(), Catch::Contains("NOT NULL"));
	Query(test.connection, "SET iceberg_default_format_version = 2");
	Query(test.connection, "BEGIN");
	test.Define("(id TIMESTAMP_NS)");
	REQUIRE(test.WritableTable().GetMetadata().iceberg_version == 3);
	test.Replace();
	Query(test.connection, "INSERT INTO lake.default.t VALUES ('2026-01-01 00:00:00.123456789')");
	Query(test.connection, "COMMIT");
	REQUIRE(Scalar(test.connection, "SELECT id::VARCHAR FROM lake.default.t") ==
	        Value("2026-01-01 00:00:00.123456789"));
}

TEST_CASE("Replacement definitions replace reset and reuse partitioning and sorting", "[iceberg_replacement]") {
	ReplacementTest test;
	Query(test.connection, "ALTER TABLE lake.default.t ADD COLUMN part VARCHAR");
	Query(test.connection, "ALTER TABLE lake.default.t SET PARTITIONED BY (part)");
	Query(test.connection, "ALTER TABLE lake.default.t SET SORTED BY (id)");
	Query(test.connection, "BEGIN");
	auto original = test.WritableTable().GetMetadataOwner();
	test.Define("(part VARCHAR, id INTEGER) PARTITIONED BY (id) SORTED BY (part)");
	auto &metadata = test.WritableTable().GetMetadata();
	REQUIRE(metadata.GetLatestPartitionSpec().fields[0].source_id == 1);
	REQUIRE(metadata.GetLatestSortOrder().fields[0].source_id == 2);
	test.Replace();
	Query(test.connection, "INSERT INTO lake.default.t VALUES ('new', 7)");
	Query(test.connection, "COMMIT");
	REQUIRE(Scalar(test.connection, "SELECT part FROM lake.default.t") == Value("new"));
	Query(test.connection, "BEGIN");
	test.Define("(id INTEGER, part VARCHAR) PARTITIONED BY (part) SORTED BY (id)");
	auto &reused = test.WritableTable().GetMetadata();
	REQUIRE(reused.default_spec_id == original->default_spec_id);
	REQUIRE(reused.default_sort_order_id == original->default_sort_order_id);
	test.Replace();
	Query(test.connection, "COMMIT");
	Query(test.connection, "BEGIN");
	test.Define("(id INTEGER)");
	auto &reset = test.WritableTable().GetMetadata();
	REQUIRE(reset.GetLatestPartitionSpec().fields.empty());
	REQUIRE(reset.GetLatestSortOrder().fields.empty());
	REQUIRE(reset.default_sort_order_id.GetIndex() == 0);
	test.Replace();
	Query(test.connection, "INSERT INTO lake.default.t VALUES (8)");
	Query(test.connection, "COMMIT");
	CheckRows(test.connection, {8});
}

TEST_CASE("Replacement definitions merge properties and keep table identity and location", "[iceberg_replacement]") {
	ReplacementTest test;
	Query(test.connection, "ALTER TABLE lake.default.t SET ('kept' = 'old', 'overridden' = 'old')");
	Query(test.connection,
	      "SET VARIABLE base_location = (SELECT metadata.location FROM iceberg_load_table_response(lake.default.t))");
	Query(test.connection, "BEGIN");
	auto original = test.WritableTable().GetMetadataOwner();
	test.Define("(id INTEGER) WITH ('LOCATION' = getvariable('base_location')::VARCHAR || '/', "
	            "'overridden' = 'new', 'added' = 'yes')");
	auto &metadata = test.WritableTable().GetMetadata();
	REQUIRE(metadata.table_uuid == original->table_uuid);
	REQUIRE(metadata.location == original->location);
	REQUIRE(metadata.GetTableProperty("kept") == "old");
	REQUIRE(metadata.GetTableProperty("overridden") == "new");
	REQUIRE(metadata.GetTableProperty("added") == "yes");
	test.Replace();
	Query(test.connection, "COMMIT");
	REQUIRE(Scalar(test.connection, "SELECT value FROM iceberg_table_properties(lake.default.t) WHERE key = 'added'") ==
	        Value("yes"));
	Query(test.connection, "BEGIN");
	test.Define("(changed VARCHAR) WITH ('added' = 'rolled back')");
	test.Replace();
	Query(test.connection, "ROLLBACK");
	REQUIRE(Scalar(test.connection, "SELECT value FROM iceberg_table_properties(lake.default.t) WHERE key = 'added'") ==
	        Value("yes"));
	Query(test.connection, "INSERT INTO lake.default.t VALUES (7)");
	CheckRows(test.connection, {7});
}

TEST_CASE("Rejected replacement definitions leave the committed table intact", "[iceberg_replacement]") {
	ReplacementTest test;
	auto before = Scalar(test.connection, "SELECT metadata_location FROM iceberg_load_table_response(lake.default.t)");
	for (auto &entry : vector<pair<string, string>> {
	         {"(id INTEGER) WITH ('format-version' = 1)", "cannot be downgraded"},
	         {"(id INTEGER) WITH ('format-version' = 0)", "lowest supported iceberg version"},
	         {"(id INTEGER) WITH ('format-version' = 4)", "highest supported iceberg version"},
	         {"(id INTEGER) WITH ('format-version' = 'invalid')", "Can't cast 'format-version'"},
	         {"(id INTEGER) WITH ('format-version' = NULL)", "NULL is not supported"},
	         {"(id INTEGER) WITH ('location' = 'different')", "cannot change the location"},
	         {"(id INTEGER) WITH ('location' = NULL)", "NULL is not supported"},
	         {"(id INTEGER) WITH ('UUID' = 'reserved')", "reserved Iceberg table property"},
	         {"(id INTEGER) WITH ('custom' = NULL)", "NULL is not supported"},
	         {"(id INTEGER DEFAULT 7)", "non-null DEFAULT values are not supported"},
	         {"(id TIMESTAMP_NS)", "requires Iceberg format-version 3"},
	         {"(b BOOLEAN) PARTITIONED BY (bucket(4, b))", "Cannot partition by"},
	         {"(b BOOLEAN) SORTED BY (bucket(4, b))", "Cannot sort by"},
	         {"(items INTEGER[]) PARTITIONED BY (items)", "Cannot partition by"},
	         {"(items INTEGER[]) SORTED BY (items)", "Cannot sort by"},
	         {"(new_id INTEGER, b BOOLEAN) PARTITIONED BY (new_id) SORTED BY (bucket(4, b)) "
	          "WITH ('format-version' = 3, 'custom' = 'must not persist')",
	          "Cannot sort by"}}) {
		CAPTURE(entry.first);
		Query(test.connection, "BEGIN");
		REQUIRE_THROWS_WITH(test.Define(entry.first), Catch::Contains(entry.second));
		Query(test.connection, "ROLLBACK");
		REQUIRE(Scalar(test.connection, "SELECT metadata_location FROM iceberg_load_table_response(lake.default.t)") ==
		        before);
		CheckRows(test.connection, {1, 2});
	}
}

TEST_CASE("Replacement definitions protect the field ID watermark even when the current schema is reused",
          "[iceberg_replacement]") {
	ReplacementTest test;
	Connection writer(test.database);
	Query(test.connection, "BEGIN");
	test.Define("(id INTEGER, replacement VARCHAR)");
	test.Replace();
	Query(writer, "ALTER TABLE lake.default.t ADD COLUMN concurrent INTEGER");
	Query(writer, "ALTER TABLE lake.default.t DROP COLUMN concurrent");
	REQUIRE(Scalar(writer,
	               "SELECT metadata.\"current-schema-id\"::INTEGER FROM iceberg_load_table_response(lake.default.t)") ==
	        Value::INTEGER(0));
	auto commit = test.connection.Query("COMMIT");
	REQUIRE(commit->HasError());
	REQUIRE_THAT(commit->GetError(), Catch::Contains("assert-last-assigned-field-id"));
	CheckRows(writer, {1, 2});
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
