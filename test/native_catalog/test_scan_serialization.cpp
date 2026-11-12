#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "planning/snapshot/iceberg_scan_info.hpp"

using namespace duckdb;

namespace {

void Query(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
}

struct BoundScan {
	BoundTableFunction function;
	unique_ptr<FunctionData> data;
};

struct ScanSerializationTest {
	ScanSerializationTest()
	    : root(FileSystem::CreateLocal()->JoinPath(FileSystem::GetWorkingDirectory(), "duckdb_unittest_tempdir",
	                                               "scan-api-" + UUID::ToString(UUID::GenerateRandomUUID()))),
	      database(nullptr), connection(database) {
		Query(connection, "CALL iceberg_test_catalog_init('" + root + "')");
		Query(connection, "ATTACH '' AS lake (TYPE ICEBERG, URI 'http://iceberg-mock.invalid', "
		                  "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', STAGE_CREATE_TABLES true)");
		Query(connection, "CREATE SCHEMA lake.default");
		Query(connection, "CREATE TABLE lake.default.t (i INTEGER)");
		Query(connection, "INSERT INTO lake.default.t VALUES (1), (2)");
		Query(connection, "BEGIN");
		StartQuery();
	}

	~ScanSerializationTest() {
		connection.Query("ROLLBACK");
		FileSystem::CreateLocal()->RemoveDirectory(root);
	}

	void StartQuery() {
		auto &context = *connection.context;
		MetaTransaction::Get(context).SetActiveQuery(DatabaseManager::Get(context).GetNewQueryNumber());
	}

	BoundScan Bind() {
		auto &context = *connection.context;
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
		                       QualifiedName(Identifier("lake"), Identifier("default"), Identifier("t")));
		auto &entry = Catalog::GetEntry(context, lookup).Cast<IcebergTableSchemaVersion>();
		unique_ptr<FunctionData> data;
		auto function = entry.GetScanFunction(context, data, lookup);
		return {BoundTableFunction(function), std::move(data)};
	}

	void Serialize(BoundScan &scan, MemoryStream &stream) {
		BinarySerializer serializer(stream);
		serializer.Begin();
		scan.function.serialize(serializer, scan.data.get(), scan.function);
		serializer.End();
	}

	BoundScan Deserialize(BoundScan &original, MemoryStream &stream, Connection &target) {
		stream.Rewind();
		BoundScan result {original.function, nullptr};
		result.function.function_info.reset();
		BinaryDeserializer deserializer(stream);
		bound_parameter_map_t parameters;
		deserializer.Set<ClientContext &>(*target.context);
		deserializer.Set<bound_parameter_map_t &>(parameters);
		deserializer.Begin();
		result.data = result.function.deserialize(deserializer, result.function);
		deserializer.End();
		return result;
	}

	BoundScan Deserialize(BoundScan &original, MemoryStream &stream) {
		return Deserialize(original, stream, connection);
	}

	string root;
	DuckDB database;
	Connection connection;
};

} // namespace

TEST_CASE("Iceberg scan callbacks preserve repeated copies without optimizer fallback", "[iceberg_serialization]") {
	ScanSerializationTest test;
	auto original = test.Bind();
	auto &info = original.function.function_info->Cast<IcebergScanInfo>();
	auto snapshot_id = info.snapshot_info.snapshot->snapshot_id;
	MemoryStream stream;
	test.Serialize(original, stream);
	auto first = test.Deserialize(original, stream);
	auto second = test.Deserialize(original, stream);
	for (auto scan : {&original, &first, &second}) {
		auto &copy_info = scan->function.function_info->Cast<IcebergScanInfo>();
		REQUIRE(copy_info.snapshot_info.snapshot->snapshot_id == snapshot_id);
		REQUIRE(copy_info.snapshot_info.schema_id == info.snapshot_info.schema_id);
		REQUIRE(copy_info.metadata.table_uuid == info.metadata.table_uuid);
		REQUIRE(scan->function.get_table_entry(scan->data.get()) ==
		        original.function.get_table_entry(original.data.get()));
	}
	MemoryStream copy_stream;
	test.Serialize(first, copy_stream);
	REQUIRE_NOTHROW(test.Deserialize(first, copy_stream));
}

TEST_CASE("Iceberg scan serialization setting requests optimizer fallback", "[iceberg_serialization]") {
	ScanSerializationTest test;
	Query(test.connection, "SET iceberg_enable_scan_serialization = false");
	test.StartQuery();
	auto disabled_scan = test.Bind();
	MemoryStream disabled_stream;
	REQUIRE_THROWS_WITH(test.Serialize(disabled_scan, disabled_stream),
	                    Catch::Contains("disabled by iceberg_enable_scan_serialization"));

	Query(test.connection, "SET iceberg_enable_scan_serialization = true");
	test.StartQuery();
	auto enabled_scan = test.Bind();
	MemoryStream enabled_stream;
	REQUIRE_NOTHROW(test.Serialize(enabled_scan, enabled_stream));
	REQUIRE_NOTHROW(test.Deserialize(enabled_scan, enabled_stream));
}

TEST_CASE("Iceberg scan identity mismatches request optimizer fallback", "[iceberg_serialization]") {
	ScanSerializationTest test;
	auto original = test.Bind();
	auto &info = original.function.function_info->Cast<IcebergScanInfo>();
	auto &table = original.function.get_table_entry(original.data.get())->Cast<IcebergTableSchemaVersion>().table_info;
	MemoryStream stream;

	SECTION("Different table UUID") {
		test.Serialize(original, stream);
		table.GetMetadataMutable().table_uuid = "replacement-table";
		REQUIRE_THROWS_AS(test.Deserialize(original, stream), NotImplementedException);
	}
	SECTION("Missing schema") {
		auto schema_id = info.snapshot_info.schema_id;
		info.snapshot_info.schema_id = -1;
		test.Serialize(original, stream);
		info.snapshot_info.schema_id = schema_id;
		REQUIRE_THROWS_AS(test.Deserialize(original, stream), NotImplementedException);
	}
	SECTION("Missing snapshot") {
		auto snapshot = *info.snapshot_info.snapshot;
		snapshot.snapshot_id = -1;
		auto original_snapshot = info.snapshot_info.snapshot;
		info.snapshot_info.snapshot = &snapshot;
		test.Serialize(original, stream);
		info.snapshot_info.snapshot = original_snapshot;
		REQUIRE_THROWS_AS(test.Deserialize(original, stream), NotImplementedException);
	}
}

TEST_CASE("Iceberg scan serialization rejects reuse outside its binding statement", "[iceberg_serialization]") {
	ScanSerializationTest test;
	SECTION("A later statement in the same transaction") {
		auto original = test.Bind();
		MemoryStream stream;
		test.Serialize(original, stream);
		test.StartQuery();
		REQUIRE_THROWS_WITH(test.Deserialize(original, stream),
		                    Catch::Contains("within the statement that bound the scan"));
		MemoryStream delayed_stream;
		REQUIRE_THROWS_WITH(test.Serialize(original, delayed_stream),
		                    Catch::Contains("within the statement that bound the scan"));
	}
	SECTION("No active transaction") {
		auto original = test.Bind();
		MemoryStream stream;
		test.Serialize(original, stream);
		Query(test.connection, "COMMIT");
		REQUIRE_THROWS_WITH(test.Deserialize(original, stream),
		                    Catch::Contains("within the statement that bound the scan"));
		// The catalog-owned metadata has been released; serialization must decline before dereferencing it.
		MemoryStream delayed_stream;
		REQUIRE_THROWS_WITH(test.Serialize(original, delayed_stream),
		                    Catch::Contains("within the statement that bound the scan"));
	}
	SECTION("Transaction-local changes with the same base snapshot and schema") {
		Query(test.connection, "INSERT INTO lake.default.t VALUES (3)");
		test.StartQuery();
		auto original = test.Bind();
		REQUIRE(original.function.function_info->Cast<IcebergScanInfo>().transaction_data);
		MemoryStream stream;
		test.Serialize(original, stream);
		REQUIRE_NOTHROW(test.Deserialize(original, stream));
		Query(test.connection, "INSERT INTO lake.default.t VALUES (4)");
		test.StartQuery();
		REQUIRE_THROWS_WITH(test.Deserialize(original, stream),
		                    Catch::Contains("within the statement that bound the scan"));
	}
	SECTION("Another transaction even when the table did not change") {
		auto original = test.Bind();
		MemoryStream stream;
		test.Serialize(original, stream);
		original.data.reset();
		original.function.function_info.reset();
		Query(test.connection, "COMMIT");
		Query(test.connection, "BEGIN");
		test.StartQuery();
		REQUIRE_THROWS_WITH(test.Deserialize(original, stream),
		                    Catch::Contains("within the statement that bound the scan"));
	}
	SECTION("Another connection") {
		auto original = test.Bind();
		MemoryStream stream;
		test.Serialize(original, stream);
		Connection other(test.database);
		Query(other, "BEGIN");
		// Matching a query number is insufficient: the transaction identity must also match.
		MetaTransaction::Get(*other.context)
		    .SetActiveQuery(MetaTransaction::Get(*test.connection.context).GetActiveQuery());
		REQUIRE_THROWS_WITH(test.Deserialize(original, stream, other),
		                    Catch::Contains("within the statement that bound the scan"));
		Query(other, "ROLLBACK");
	}
}

TEST_CASE("Iceberg scans of empty and staged tables serialize directly", "[iceberg_serialization]") {
	ScanSerializationTest test;
	Query(test.connection, "DROP TABLE lake.default.t");
	Query(test.connection, "COMMIT");
	Query(test.connection, "BEGIN");
	Query(test.connection, "CREATE TABLE lake.default.t (i INTEGER)");
	SECTION("Committed table without a snapshot") {
		Query(test.connection, "COMMIT");
		Query(test.connection, "BEGIN");
	}
	SECTION("Staged table without a snapshot") {
	}
	SECTION("Staged table with pending rows") {
		Query(test.connection, "INSERT INTO lake.default.t VALUES (3)");
	}
	test.StartQuery();
	auto original = test.Bind();
	auto &info = original.function.function_info->Cast<IcebergScanInfo>();
	REQUIRE_FALSE(info.snapshot_info.snapshot);
	MemoryStream stream;
	test.Serialize(original, stream);
	auto copy = test.Deserialize(original, stream);
	auto &copy_info = copy.function.function_info->Cast<IcebergScanInfo>();
	REQUIRE_FALSE(copy_info.snapshot_info.snapshot);
	REQUIRE(copy_info.transaction_data == info.transaction_data);
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
