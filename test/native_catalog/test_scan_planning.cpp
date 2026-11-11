#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "function/scan_planning/iceberg_row_filter.hpp"
#include "planning/iceberg_multi_file_list.hpp"

using namespace duckdb;

namespace {

void Query(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
}

IcebergTableFilters Filters(ClientContext &context, const LogicalType &schema, const string &sql) {
	auto expression = IcebergRowFilter::Bind(context, sql, schema);
	vector<ColumnIndex> columns;
	for (idx_t i = 0; i < StructType::GetChildCount(schema); i++) {
		columns.emplace_back(i);
	}
	bool always_false = false;
	auto filters = IcebergRowFilter::TableFilters(context, *expression, columns, always_false);
	REQUIRE_FALSE(always_false);
	IcebergTableFilters result;
	for (auto &entry : filters) {
		result.PushFilter(columns[entry.GetIndex().GetIndex()],
		                  ExpressionFilter::GetExpressionFilter(entry.Filter(), "planning test").Copy());
	}
	return result;
}

struct PlanningTest {
	PlanningTest()
	    : root(FileSystem::CreateLocal()->JoinPath(FileSystem::GetWorkingDirectory(), "duckdb_unittest_tempdir",
	                                               "planning-api-" + UUID::ToString(UUID::GenerateRandomUUID()))),
	      database(nullptr), connection(database) {
		Query(connection, "CALL iceberg_test_catalog_init('" + root + "')");
		Query(connection, "ATTACH '' AS lake (TYPE ICEBERG, URI 'http://iceberg-mock.invalid', "
		                  "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', STAGE_CREATE_TABLES true)");
		Query(connection, "CREATE SCHEMA lake.default");
		Query(connection, "SET iceberg_use_server_side_scan_planning=false");
	}

	~PlanningTest() {
		connection.Query("ROLLBACK");
		FileSystem::CreateLocal()->RemoveDirectory(root);
	}

	unique_ptr<FunctionData> Bind() {
		Query(connection, "BEGIN");
		auto &context = *connection.context;
		MetaTransaction::Get(context).SetActiveQuery(DatabaseManager::Get(context).GetNewQueryNumber());
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
		                       QualifiedName(Identifier("lake"), Identifier("default"), Identifier("t")));
		auto &entry = Catalog::GetEntry(context, lookup).Cast<IcebergTableSchemaVersion>();
		unique_ptr<FunctionData> data;
		entry.GetScanFunction(context, data, lookup);
		return data;
	}

	string root;
	DuckDB database;
	Connection connection;
};

} // namespace

TEST_CASE("Iceberg partition filters borrow originals and own extracted paths", "[iceberg_planning]") {
	DuckDB database(nullptr);
	Connection connection(database);
	Query(connection, "BEGIN");
	auto &context = *connection.context;
	auto nested = LogicalType::STRUCT({{"x", LogicalType::INTEGER}, {"y", LogicalType::INTEGER}});
	auto schema = LogicalType::STRUCT({{"id", LogicalType::INTEGER}, {"s", LogicalType::STRUCT({{"child", nested}})}});
	IcebergTableFilters filters;
	filters.PushFilter(
	    ColumnIndex(0),
	    make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(
	        ExpressionFilter::CreateInExpression(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL),
	                                             {Value(1), Value(3), Value(5), Value(7), Value(9), Value(11)}),
	        LogicalType::INTEGER)));
	filters.PushFilter(ColumnIndex(1), make_uniq<ExpressionFilter>(
	                                       IcebergRowFilter::Bind(context, "s.child.x=2 AND s.child.y=4", schema)));
	auto original = filters.TryGetFilterByColumnIndex(ColumnIndex(0));
	REQUIRE(original);
	REQUIRE(filters.GetFilterForColumnIndex(ColumnIndex(0)).get() == original.get());
	auto original_copy = original->Copy();

	// Partition-source maps encode successive levels as a flat list of child indexes.
	ColumnIndex x(1);
	x.AddChildIndex(ColumnIndex(0));
	x.AddChildIndex(ColumnIndex(0));
	ColumnIndex y(1);
	y.AddChildIndex(ColumnIndex(0));
	y.AddChildIndex(ColumnIndex(1));
	auto parent = filters.TryGetFilterByColumnIndex(ColumnIndex(1));
	REQUIRE(parent);
	auto parent_copy = parent->Copy();
	auto x_filter = filters.GetFilterForColumnIndex(x);
	REQUIRE(x_filter);
	auto y_filter = filters.GetFilterForColumnIndex(y);
	REQUIRE(y_filter);
	REQUIRE(x_filter.get() != y_filter.get());
	REQUIRE(x_filter.get() != parent.get());
	REQUIRE(filters.GetFilterForColumnIndex(x).get() == x_filter.get());
	REQUIRE(filters.GetFilterForColumnIndex(y).get() == y_filter.get());
	REQUIRE(parent->Equals(*parent_copy));
	REQUIRE(original->Equals(*original_copy));

	REQUIRE(x_filter->EvaluateWithConstant(context, Value::INTEGER(2)));
	REQUIRE_FALSE(y_filter->EvaluateWithConstant(context, Value::INTEGER(2)));

	// A missing path remains conservative across repeated lookups and cache growth.
	ColumnIndex missing(1);
	missing.AddChildIndex(ColumnIndex(1));
	REQUIRE_FALSE(filters.GetFilterForColumnIndex(missing));
	REQUIRE_FALSE(filters.GetFilterForColumnIndex(missing));
	REQUIRE(filters.GetFilterForColumnIndex(x).get() == x_filter.get());

	// Optional IN filters must be extracted without modifying the original.
	IcebergTableFilters optional_filters;
	optional_filters.PushFilter(ColumnIndex(1),
	                            make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(
	                                IcebergRowFilter::Bind(context, "s.child.x IN (1,3,5,7,9,11)", schema),
	                                StructType::GetChildType(schema, 1))));
	auto optional_parent = optional_filters.TryGetFilterByColumnIndex(ColumnIndex(1));
	REQUIRE(optional_parent);
	auto optional_copy = optional_parent->Copy();
	auto optional_child = optional_filters.GetFilterForColumnIndex(x);
	REQUIRE(optional_child);
	REQUIRE_FALSE(optional_child->EvaluateWithConstant(context, Value::INTEGER(2)));
	REQUIRE(optional_filters.GetFilterForColumnIndex(x).get() == optional_child.get());
	REQUIRE(optional_parent->Equals(*optional_copy));
	Query(connection, "COMMIT");
}

TEST_CASE("Iceberg partition filter caches invalidate missing paths when filters are added", "[iceberg_planning]") {
	DuckDB database(nullptr);
	Connection connection(database);
	Query(connection, "BEGIN");
	auto &context = *connection.context;
	auto nested = LogicalType::STRUCT({{"x", LogicalType::INTEGER}, {"y", LogicalType::INTEGER}});
	auto schema = LogicalType::STRUCT({{"id", LogicalType::INTEGER}, {"s", nested}});
	ColumnIndex x(1);
	x.AddChildIndex(ColumnIndex(0));
	ColumnIndex y(1);
	y.AddChildIndex(ColumnIndex(1));

	IcebergTableFilters filters;
	REQUIRE_FALSE(filters.GetFilterForColumnIndex(x));
	REQUIRE_FALSE(filters.GetFilterForColumnIndex(y));
	filters.PushFilter(ColumnIndex(1),
	                   make_uniq<ExpressionFilter>(IcebergRowFilter::Bind(context, "s.x=2 AND s.y IS NULL", schema)));
	auto x_filter = filters.GetFilterForColumnIndex(x);
	auto y_filter = filters.GetFilterForColumnIndex(y);
	REQUIRE(x_filter);
	REQUIRE(y_filter);
	REQUIRE(x_filter->EvaluateWithConstant(context, Value::INTEGER(2)));
	REQUIRE_FALSE(x_filter->EvaluateWithConstant(context, Value::INTEGER(3)));
	REQUIRE(y_filter->EvaluateWithConstant(context, Value(LogicalType::INTEGER)));
	REQUIRE_FALSE(y_filter->EvaluateWithConstant(context, Value::INTEGER(2)));

	// Adding another column invalidates owned paths without changing their predicates.
	auto x_copy = x_filter->Copy();
	auto y_copy = y_filter->Copy();
	filters.PushFilter(ColumnIndex(0), make_uniq<ExpressionFilter>(IcebergRowFilter::Bind(context, "id=7", schema)));
	x_filter = filters.GetFilterForColumnIndex(x);
	y_filter = filters.GetFilterForColumnIndex(y);
	REQUIRE(x_filter);
	REQUIRE(y_filter);
	REQUIRE(x_filter->Equals(*x_copy));
	REQUIRE(y_filter->Equals(*y_copy));
	Query(connection, "COMMIT");
}

TEST_CASE("Iceberg filtered views recompute delete predicates on a shared provider", "[iceberg_planning]") {
	PlanningTest test;
	// Equality writes are optional in production builds.
	auto setting = test.connection.Query("SELECT value FROM duckdb_settings() "
	                                     "WHERE name='__iceberg_unsafe_enable_equality_deletes'");
	REQUIRE_FALSE(setting->HasError());
	if (!setting->RowCount()) {
		WARN("Shared-provider delete coverage requires ICEBERG_ENABLE_EQUALITY_DELETE_WRITES=1");
		return;
	}
	Query(test.connection, "SET __iceberg_unsafe_enable_equality_deletes=true");
	Query(test.connection, "CREATE TABLE lake.default.t (id INTEGER) WITH "
	                       "('format-version'=2, 'commit.manifest-merge.enabled'='false')");
	for (idx_t i = 0; i < 3; i++) {
		Query(test.connection, "INSERT INTO lake.default.t SELECT range FROM range(1,11)");
	}
	Query(test.connection, "DELETE FROM lake.default.t WHERE id=2");
	Query(test.connection, "DELETE FROM lake.default.t WHERE id=8");
	auto bound = test.Bind();
	auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
	auto schema = LogicalType::STRUCT({{"id", LogicalType::INTEGER}});
	auto low = base.CreateView(Filters(*test.connection.context, schema, "id<=3"));
	vector<string> low_deletes;
	for (idx_t i = 0; i < 3; i++) {
		auto task = low->GetScanTask(i);
		REQUIRE(task);
		REQUIRE(task->delete_files.size() == 1);
		low_deletes.push_back(task->delete_files[0].file_path);
	}
	// Create the second view after the first has populated both positive and negative results.
	auto high = low->CreateView(Filters(*test.connection.context, schema, "id>=7"));
	for (idx_t i = 0; i < 3; i++) {
		auto task = high->GetScanTask(i);
		REQUIRE(task);
		REQUIRE(task->delete_files.size() == 1);
		REQUIRE(task->delete_files[0].file_path != low_deletes[i]);
		auto revisited = low->GetScanTask(i);
		REQUIRE(revisited);
		REQUIRE(revisited->delete_files.size() == 1);
		REQUIRE(revisited->delete_files[0].file_path == low_deletes[i]);
	}
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
