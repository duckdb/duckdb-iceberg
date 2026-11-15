#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "function/scan_planning/iceberg_row_filter.hpp"
#include "planning/iceberg_multi_file_list.hpp"

#include <future>
#include <condition_variable>
#include <limits>
#include <thread>

namespace duckdb {

//! A probe of load ownership lets the test release I/O only after the overlapping
//! caller registered its shared load, without a production callback or timed race.
struct IcebergManifestStoreTest {
	static long LoadOwners(IcebergManifestStore &store, idx_t index) {
		annotated_lock_guard<annotated_mutex> guard(store.lock);
		annotated_lock_guard<annotated_mutex> delete_guard(store.delete_manifest_lock);
		return store.delete_manifest_loads[index].use_count();
	}
};

} // namespace duckdb

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

TEST_CASE("Iceberg concurrent nested filter lookups retain extracted and missing paths", "[iceberg_planning]") {
	DuckDB database(nullptr);
	Connection connection(database);
	Query(connection, "BEGIN");
	auto &context = *connection.context;
	auto schema =
	    LogicalType::STRUCT({{"s", LogicalType::STRUCT({{"x", LogicalType::INTEGER}, {"y", LogicalType::INTEGER}})}});
	IcebergTableFilters filters;
	filters.PushFilter(ColumnIndex(0),
	                   make_uniq<ExpressionFilter>(IcebergRowFilter::Bind(context, "s.x=2 AND s.y IS NULL", schema)));
	auto parent = filters.TryGetFilterByColumnIndex(ColumnIndex(0));
	REQUIRE(parent);
	auto original = parent->Copy();
	ColumnIndex x(0);
	x.AddChildIndex(ColumnIndex(0));
	ColumnIndex y(0);
	y.AddChildIndex(ColumnIndex(1));
	std::promise<void> start;
	auto ready = start.get_future().share();
	vector<std::future<pair<const ExpressionFilter *, const ExpressionFilter *>>> workers;
	for (idx_t worker = 0; worker < 8; worker++) {
		workers.push_back(std::async(std::launch::async, [&, worker] {
			ready.wait();
			auto x_filter = filters.GetFilterForColumnIndex(x);
			auto y_filter = filters.GetFilterForColumnIndex(y);
			for (idx_t i = 0; i < 64; i++) {
				ColumnIndex missing(0);
				missing.AddChildIndex(ColumnIndex(2 + worker * 64 + i));
				if (filters.GetFilterForColumnIndex(missing) ||
				    filters.GetFilterForColumnIndex(x).get() != x_filter.get() ||
				    filters.GetFilterForColumnIndex(y).get() != y_filter.get()) {
					throw InternalException("Nested filter cache changed during concurrent lookups");
				}
			}
			return make_pair(x_filter.get(), y_filter.get());
		}));
	}
	start.set_value();
	// Join every reader before asserting, including when one reader failed.
	vector<pair<const ExpressionFilter *, const ExpressionFilter *>> results;
	for (auto &worker : workers) {
		results.push_back(worker.get());
	}
	auto x_filter = filters.GetFilterForColumnIndex(x);
	auto y_filter = filters.GetFilterForColumnIndex(y);
	REQUIRE(x_filter);
	REQUIRE(y_filter);
	for (auto &result : results) {
		REQUIRE(result.first == x_filter.get());
		REQUIRE(result.second == y_filter.get());
	}
	REQUIRE(x_filter->EvaluateWithConstant(context, Value(2)));
	REQUIRE_FALSE(x_filter->EvaluateWithConstant(context, Value(3)));
	REQUIRE(y_filter->EvaluateWithConstant(context, Value(LogicalType::INTEGER)));
	REQUIRE_FALSE(y_filter->EvaluateWithConstant(context, Value(2)));
	REQUIRE(parent->Equals(*original));
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

namespace {

IcebergManifestEntry DeleteEntry(idx_t id, Value partition, optional<string> referenced = nullopt) {
	IcebergManifestEntry entry;
	entry.status = IcebergManifestEntryStatusType::ADDED;
	entry.data_file.content = IcebergManifestEntryContentType::POSITION_DELETES;
	entry.data_file.file_path = "shared.puffin";
	entry.data_file.content_offset = static_cast<int64_t>(id * 100);
	entry.data_file.file_format = "puffin";
	entry.data_file.record_count = 1;
	entry.data_file.file_size_in_bytes = 100;
	entry.data_file.partition_info = {{1000, std::move(partition)}};
	entry.data_file.referenced_data_file = std::move(referenced);
	return entry;
}

vector<idx_t> Candidates(const IcebergDeleteCandidateIndex &index, const IcebergDataFile &data, int32_t spec_id) {
	vector<idx_t> result;
	for (auto &bucket : index.Lookup(data.file_path, spec_id, IcebergFilePruner::PartitionValueMap(data))) {
		result.insert(result.end(), bucket.get().begin(), bucket.get().end());
	}
	std::sort(result.begin(), result.end());
	return result;
}

IcebergPartitionSpec PartitionSpec() {
	IcebergPartitionSpec spec(17);
	IcebergPartitionSpecField field;
	field.source_id = 1;
	field.partition_field_id = 1000;
	field.transform = IcebergTransform("identity");
	spec.fields.push_back(std::move(field));
	return spec;
}

} // namespace

TEST_CASE("Iceberg delete candidate buckets preserve paths partitions and conservative fallbacks",
          "[iceberg_planning]") {
	DuckDB database(nullptr);
	Connection connection(database);
	auto &context = *connection.context;
	auto spec = PartitionSpec();
	vector<IcebergManifestEntry> entries;
	entries.push_back(DeleteEntry(0, Value::DOUBLE(1), "original.parquet"));
	entries.push_back(DeleteEntry(1, Value::DOUBLE(1), "original.parquet"));
	entries.push_back(DeleteEntry(2, Value::DOUBLE(1), "other.parquet"));
	entries.push_back(DeleteEntry(3, Value::DOUBLE(1)));
	entries.push_back(DeleteEntry(4, Value::DOUBLE(2)));
	entries.push_back(DeleteEntry(5, Value(LogicalType::DOUBLE)));
	entries.push_back(DeleteEntry(6, Value::DOUBLE(std::numeric_limits<double>::quiet_NaN())));
	entries.push_back(DeleteEntry(7, Value::DOUBLE(-0.0)));
	entries.push_back(DeleteEntry(8, Value::DOUBLE(1)));
	entries.back().status = IcebergManifestEntryStatusType::DELETED;
	entries.push_back(DeleteEntry(9, Value::DOUBLE(1)));
	entries.back().data_file.partition_info.clear();
	entries.push_back(DeleteEntry(10, Value::INTEGER(1))); // unlike numeric types can compare equal
	entries[3].data_file.content = IcebergManifestEntryContentType::EQUALITY_DELETES;
	IcebergDeleteCandidateIndex index(context, std::move(entries), spec);
	auto data = DeleteEntry(0, Value::DOUBLE(1)).data_file;
	data.file_path = "original.parquet";
	REQUIRE(Candidates(index, data, 17) == vector<idx_t> {0, 1, 3, 9, 10});
	REQUIRE(index.Entries()[0].data_file.content_offset != index.Entries()[1].data_file.content_offset);
	// Only the original path participates in reference matching, even if a scan resolves a moved path.
	data.file_path = "resolved/original.parquet";
	REQUIRE(Candidates(index, data, 17) == vector<idx_t> {3, 9, 10});
	data.partition_info[0].value = Value(LogicalType::INTEGER);
	REQUIRE(Candidates(index, data, 17) == vector<idx_t> {5, 9, 10});
	data.partition_info[0].value = Value::DOUBLE(-std::numeric_limits<double>::quiet_NaN());
	REQUIRE(Candidates(index, data, 17) == vector<idx_t> {6, 9, 10});
	data.partition_info[0].value = Value::DOUBLE(0.0);
	REQUIRE(Candidates(index, data, 17) == vector<idx_t> {7, 9, 10});
	REQUIRE(Candidates(index, data, 18).empty());
	// Different types or missing fields cannot safely narrow the unreferenced bucket.
	data.partition_info[0].value = Value::INTEGER(1);
	REQUIRE(Candidates(index, data, 17) == vector<idx_t> {3, 4, 5, 6, 7, 9, 10});
	data.partition_info.clear();
	REQUIRE(Candidates(index, data, 17) == vector<idx_t> {3, 4, 5, 6, 7, 9, 10});

	IcebergDeleteCandidateIndex global(context, {DeleteEntry(0, Value::INTEGER(1)), DeleteEntry(1, Value::INTEGER(2))},
	                                   IcebergPartitionSpec(0));
	REQUIRE(Candidates(global, data, 18) == vector<idx_t> {0, 1});
	// A spec consisting entirely of void transforms is also global.
	spec.fields[0].transform = IcebergTransform("void");
	IcebergDeleteCandidateIndex void_spec(context, {DeleteEntry(0, Value())}, spec);
	REQUIRE(Candidates(void_spec, data, 18) == vector<idx_t> {0});

	spec = PartitionSpec();
	auto second_field = spec.fields[0];
	second_field.partition_field_id = 1001;
	spec.fields.push_back(second_field);
	auto first = DeleteEntry(0, Value::DOUBLE(1));
	first.data_file.partition_info.push_back({1001, Value("a")});
	auto second = DeleteEntry(1, Value::DOUBLE(1));
	second.data_file.partition_info.insert(second.data_file.partition_info.begin(), {1001, Value("b")});
	IcebergDeleteCandidateIndex composite(context, {first, second}, spec);
	data.partition_info = {{1001, Value("b")}, {1000, Value::DOUBLE(1)}, {9999, Value(42)}};
	REQUIRE(Candidates(composite, data, 17) == vector<idx_t> {1});
	data.partition_info[0].value = Value("c");
	REQUIRE(Candidates(composite, data, 17).empty());

	// Even NULL entries in unsupported logical types must stay conservative.
	spec = PartitionSpec();
	auto list = Value::LIST(LogicalType::INTEGER, {Value(1)});
	IcebergDeleteCandidateIndex unsupported(context, {DeleteEntry(0, Value(list.type())), DeleteEntry(1, list)}, spec);
	data.partition_info = {{1000, Value(list.type())}};
	REQUIRE(Candidates(unsupported, data, 17) == vector<idx_t> {0, 1});
}

TEST_CASE("Iceberg partition groups retain duplicate entries across growth and concurrent lookups",
          "[iceberg_planning]") {
	DuckDB database(nullptr);
	Connection connection(database);
	auto spec = PartitionSpec();
	for (uint64_t field_id : {1001, 1002}) {
		auto field = spec.fields[0];
		field.partition_field_id = field_id;
		spec.fields.push_back(field);
	}
	// More groups than one vector and the initial hash capacity; duplicate keys
	// are inserted again after growth. The last column stays SQLNULL throughout.
	constexpr idx_t group_count = 5000;
	auto entry_for = [](idx_t id) {
		auto entry = DeleteEntry(id, id % 11 ? Value::BIGINT(id) : Value());
		entry.data_file.partition_info.push_back({1001, Value(string(80, 'x') + std::to_string(id))});
		entry.data_file.partition_info.push_back({1002, Value()});
		return entry;
	};
	vector<IcebergManifestEntry> entries;
	for (idx_t pass = 0; pass < 2; pass++) {
		for (idx_t i = 0; i < group_count; i++) {
			auto entry = entry_for(i);
			entry.data_file.content_offset = static_cast<int64_t>((pass * group_count + i) * 100);
			entries.push_back(std::move(entry));
		}
	}
	IcebergDeleteCandidateIndex index(*connection.context, std::move(entries), spec);
	std::promise<void> start;
	auto ready = start.get_future().share();
	vector<std::future<bool>> workers;
	for (idx_t worker = 0; worker < 8; worker++) {
		workers.push_back(std::async(std::launch::async, [&, worker] {
			ready.wait();
			for (idx_t i = worker; i < group_count; i += 8) {
				auto data = entry_for(i).data_file;
				if (Candidates(index, data, spec.spec_id) != vector<idx_t> {i, group_count + i}) {
					return false;
				}
			}
			return Candidates(index, entry_for(group_count).data_file, spec.spec_id).empty();
		}));
	}
	start.set_value();
	for (auto &worker : workers) {
		REQUIRE(worker.get());
	}
	REQUIRE(index.Entries()[0].data_file.content_offset != index.Entries()[group_count].data_file.content_offset);
}

TEST_CASE("Iceberg partition groups preserve typed equality and boundary values", "[iceberg_planning]") {
	DuckDB database(nullptr);
	Connection connection(database);
	vector<vector<Value>> cases {
	    {Value::BOOLEAN(false), Value::BOOLEAN(true)},
	    {Value::INTEGER(NumericLimits<int32_t>::Minimum()), Value::INTEGER(NumericLimits<int32_t>::Maximum())},
	    {Value::BIGINT(NumericLimits<int64_t>::Minimum()), Value::BIGINT(NumericLimits<int64_t>::Maximum())},
	    {Value::FLOAT(-0.0f), Value::FLOAT(0.0f), Value::FLOAT(std::numeric_limits<float>::quiet_NaN()),
	     Value::FLOAT(-std::numeric_limits<float>::quiet_NaN()), Value::FLOAT(std::numeric_limits<float>::infinity()),
	     Value::FLOAT(-std::numeric_limits<float>::infinity())},
	    {Value::DOUBLE(-0.0), Value::DOUBLE(0.0), Value::DOUBLE(std::numeric_limits<double>::quiet_NaN()),
	     Value::DOUBLE(-std::numeric_limits<double>::quiet_NaN()),
	     Value::DOUBLE(std::numeric_limits<double>::infinity()),
	     Value::DOUBLE(-std::numeric_limits<double>::infinity())},
	    {Value::DATE(1969, 12, 31), Value::DATE(1970, 1, 1)},
	    {Value::TIME(0, 0, 0, 0), Value::TIME(23, 59, 59, 999999)},
	    {Value::TIMESTAMP(timestamp_t(-1)), Value::TIMESTAMP(timestamp_t(1))},
	    {Value::TIMESTAMPTZ(timestamp_tz_t(-1)), Value::TIMESTAMPTZ(timestamp_tz_t(1))},
	    {Value::TIMESTAMPNS(timestamp_ns_t(-1)), Value::TIMESTAMPNS(timestamp_ns_t(1))},
	    {Value(""), Value(string(80, 'x')), Value(string(79, 'x') + "y")},
	    {Value::BLOB_RAW(string("a\0b", 3)), Value::BLOB_RAW(string("a\0c", 3)), Value::BLOB_RAW(string())},
	    {Value::UUID("00000000-0000-0000-0000-000000000000"), Value::UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")}};
	// Exercise every physical decimal width, including values beyond int64.
	for (auto width : {4, 9, 18, 38}) {
		auto type = LogicalType::DECIMAL(width, 2);
		auto literal = string(width - 2, '9') + ".99";
		cases.push_back({Value(literal).DefaultCastAs(type), Value("-" + literal).DefaultCastAs(type)});
	}
	for (auto &values : cases) {
		INFO(values[0].type().ToString());
		// Discover the type after an initially untyped NULL; retain duplicate keys.
		vector<IcebergManifestEntry> entries {DeleteEntry(0, Value())};
		for (auto &value : values) {
			entries.push_back(DeleteEntry(entries.size(), value));
			entries.push_back(DeleteEntry(entries.size(), value));
		}
		entries.push_back(DeleteEntry(entries.size(), values[0]));
		entries.back().status = IcebergManifestEntryStatusType::DELETED;
		IcebergDeleteCandidateIndex index(*connection.context, std::move(entries), PartitionSpec());
		values.push_back(Value(values[0].type()));
		for (auto &value : values) {
			auto data = DeleteEntry(0, value).data_file;
			vector<idx_t> expected;
			for (idx_t i = 0; i < index.Entries().size(); i++) {
				auto &entry = index.Entries()[i];
				if (entry.status != IcebergManifestEntryStatusType::DELETED &&
				    Value::NotDistinctFrom(entry.data_file.partition_info[0].value, value)) {
					expected.push_back(i);
				}
			}
			REQUIRE_FALSE(expected.empty());
			REQUIRE(Candidates(index, data, 17) == expected);
		}
	}
}

TEST_CASE("Iceberg indexed candidates agree with exhaustive delete applicability", "[iceberg_planning]") {
	PlanningTest test;
	Query(test.connection, "CREATE TABLE lake.default.t (part DOUBLE)");
	auto bound = test.Bind();
	auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
	auto metadata = base.GetMetadata().Copy();
	auto spec = PartitionSpec();
	metadata.partition_specs.emplace(spec.spec_id, spec);
	metadata.partition_specs.emplace(18, IcebergPartitionSpec(18));
	IcebergTableFilters filters;
	IcebergFilePruner pruner(*test.connection.context, metadata, base.GetSchema(), filters);
	vector<Value> values {Value::DOUBLE(1),           Value::DOUBLE(2),
	                      Value(LogicalType::DOUBLE), Value::DOUBLE(std::numeric_limits<double>::quiet_NaN()),
	                      Value::DOUBLE(-0.0),        Value::INTEGER(1)};
	for (auto delete_spec_id : {0, 17, 18}) {
		vector<IcebergManifestEntry> entries;
		for (auto &value : values) {
			for (auto content : {IcebergManifestEntryContentType::POSITION_DELETES,
			                     IcebergManifestEntryContentType::EQUALITY_DELETES}) {
				auto entry = DeleteEntry(entries.size(), value);
				entry.data_file.content = content;
				entry.data_file.file_format = "parquet";
				entry.data_file.equality_ids = {1}; // missing bounds must stay conservative
				entries.push_back(entry);
				entry.data_file.referenced_data_file = "data.parquet";
				entries.push_back(std::move(entry));
			}
		}
		IcebergDeleteCandidateIndex index(*test.connection.context, std::move(entries),
		                                  metadata.partition_specs.at(delete_spec_id));
		for (auto data_spec_id : {0, 17, 18}) {
			for (auto &value : values) {
				auto data_entry = DeleteEntry(0, value);
				data_entry.data_file.file_path = "data.parquet";
				data_entry.data_file.content = IcebergManifestEntryContentType::DATA;
				for (auto sequence : {4, 5, 6}) {
					IcebergManifest data_manifest(data_spec_id, IcebergManifestContentType::DATA, sequence);
					IcebergManifest delete_manifest(delete_spec_id, IcebergManifestContentType::DELETE, 5);
					vector<idx_t> exhaustive;
					vector<idx_t> indexed;
					auto applies = [&](idx_t idx) {
						IcebergEqualityDeleteBounds bounds;
						return pruner.DeleteFileMatchesDataFile(
						    delete_manifest, index.Entries()[idx], data_manifest, data_entry,
						    IcebergFilePruner::PartitionValueMap(data_entry.data_file), &bounds);
					};
					for (idx_t i = 0; i < index.Entries().size(); i++) {
						if (applies(i)) {
							exhaustive.push_back(i);
						}
					}
					for (auto i : Candidates(index, data_entry.data_file, data_spec_id)) {
						if (applies(i)) {
							indexed.push_back(i);
						}
					}
					REQUIRE(indexed == exhaustive);
				}
			}
		}
	}
}

TEST_CASE("Iceberg concurrent filtered views share completed indexes without sharing predicates",
          "[iceberg_planning]") {
	PlanningTest test;
	auto setting = test.connection.Query("SELECT value FROM duckdb_settings() "
	                                     "WHERE name='__iceberg_unsafe_enable_equality_deletes'");
	REQUIRE_FALSE(setting->HasError());
	if (!setting->RowCount()) {
		WARN("Concurrent equality planning requires ICEBERG_ENABLE_EQUALITY_DELETE_WRITES=1");
		return;
	}
	Query(test.connection, "SET __iceberg_unsafe_enable_equality_deletes=true");
	Query(test.connection, "CREATE TABLE lake.default.t (id INTEGER) WITH "
	                       "('format-version'=2, 'commit.manifest-merge.enabled'='false')");
	for (idx_t i = 0; i < 4; i++) {
		Query(test.connection, "INSERT INTO lake.default.t SELECT range FROM range(1,11)");
	}
	Query(test.connection, "DELETE FROM lake.default.t WHERE id=2");
	Query(test.connection, "DELETE FROM lake.default.t WHERE id=8");
	auto bound = test.Bind();
	auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
	auto schema = LogicalType::STRUCT({{"id", LogicalType::INTEGER}});
	auto low = base.CreateView(Filters(*test.connection.context, schema, "id<=3"));
	auto high = base.CreateView(Filters(*test.connection.context, schema, "id>=7"));
	std::promise<void> start;
	auto ready = start.get_future().share();
	vector<std::future<vector<string>>> workers;
	for (idx_t worker = 0; worker < 8; worker++) {
		workers.push_back(std::async(std::launch::async, [&, worker] {
			ready.wait();
			vector<string> deletes;
			auto &view = worker % 2 ? *high : *low;
			for (idx_t i = 0; i < 40; i++) {
				auto task = view.GetScanTask(i % 4);
				if (!task || task->delete_files.size() != 1) {
					throw InternalException("Concurrent view returned incorrect delete candidates");
				}
				deletes.push_back(task->delete_files[0].file_path);
			}
			return deletes;
		}));
	}
	start.set_value();
	vector<vector<string>> results;
	for (auto &worker : workers) {
		results.push_back(worker.get());
	}
	REQUIRE(results[0][0] != results[1][0]);
	for (idx_t i = 0; i < results.size(); i++) {
		for (auto &path : results[i]) {
			REQUIRE(path == results[i % 2][0]);
		}
	}
}

namespace {

class GatedManifestFileSystem : public LocalFileSystem {
public:
	GatedManifestFileSystem(string path_p, int failure_p) : path(std::move(path_p)), failure(failure_p) {
	}
	string GetName() const override {
		return "GatedManifestFileSystem";
	}
	bool CanHandleFile(const string &file) override {
		return file == path;
	}
	unique_ptr<FileHandle> OpenFile(const string &file, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		unique_lock<mutex> guard(lock);
		opens++;
		entered = true;
		cv.notify_all();
		if (!cv.wait_for(guard, std::chrono::seconds(30), [&] { return released; })) {
			throw IOException("Timed out waiting for manifest test gate");
		}
		if (failure == 1) {
			throw IOException("Injected delete manifest read failure");
		}
		if (failure == 2) {
			throw InterruptException();
		}
		return LocalFileSystem::OpenFile(file, flags, opener);
	}
	bool WaitForReader() {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(10), [&] { return entered; });
	}
	void Release() {
		lock_guard<mutex> guard(lock);
		released = true;
		cv.notify_all();
	}
	idx_t OpenCount() {
		lock_guard<mutex> guard(lock);
		return opens;
	}

private:
	string path;
	int failure;
	mutex lock;
	std::condition_variable cv;
	bool entered = false;
	bool released = false;
	idx_t opens = 0;
};

} // namespace

TEST_CASE("Iceberg overlapping lazy delete loads publish atomically and propagate failures", "[iceberg_planning]") {
	int failure = 0;
	SECTION("Completed indexes are shared") {
		failure = 0;
	}
	SECTION("I/O failure reaches the owner and waiter") {
		failure = 1;
	}
	SECTION("Cancellation reaches the owner and waiter") {
		failure = 2;
	}
	PlanningTest test;
	Query(test.connection, "SET threads=1");
	Query(test.connection, "SET enable_external_file_cache=false");
	Query(test.connection, "CREATE TABLE lake.default.t (id INTEGER) WITH "
	                       "('format-version'=3, 'commit.manifest-merge.enabled'='false')");
	Query(test.connection, "INSERT INTO lake.default.t SELECT range FROM range(10)");
	Query(test.connection, "INSERT INTO lake.default.t SELECT range FROM range(10,20)");
	Query(test.connection, "DELETE FROM lake.default.t WHERE id=1");
	Query(test.connection, "DELETE FROM lake.default.t WHERE id=12");
	auto bound = test.Bind();
	auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
	auto &fs = FileSystem::GetFileSystem(*test.connection.context);
	IcebergScanPlanContext context {
	    *test.connection.context, fs,     base.GetPath(), base.GetOptions(), base.GetSnapshot(), base.GetMetadata(),
	    base.GetSchema(),         nullptr};
	annotated_mutex planning_lock;
	shared_ptr<const IcebergDeleteCandidateIndex> retained;
	{
		IcebergManifestStore store(planning_lock, context);
		string gated_path;
		{
			annotated_lock_guard<annotated_mutex> guard(store.lock);
			store.LoadManifestList();
			REQUIRE(store.DeleteManifests().size() == 2);
			gated_path = store.DeleteManifests()[1].GetFile().manifest_path;
		}
		auto gated = make_uniq<GatedManifestFileSystem>(gated_path, failure);
		auto &gate = *gated;
		FileSystem::GetFileSystem(*test.database.instance).RegisterSubSystem(std::move(gated));
		auto read = [&](vector<idx_t> indexes) {
			try {
				store.ReadDeleteManifests(indexes, 0);
				return string();
			} catch (std::exception &ex) {
				return string(ex.what());
			}
		};
		auto owner = std::async(std::launch::async, [&] { return read({0, 1}); });
		auto entered = gate.WaitForReader();
		auto owner_count = IcebergManifestStoreTest::LoadOwners(store, 0);
		auto waiter = std::async(std::launch::async, [&] { return read({0}); });
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (IcebergManifestStoreTest::LoadOwners(store, 0) <= owner_count &&
		       std::chrono::steady_clock::now() < deadline) {
			std::this_thread::yield();
		}
		auto registered = IcebergManifestStoreTest::LoadOwners(store, 0) > owner_count;
		bool partial_index = false;
		{
			// Acquiring this while I/O is blocked also proves that the owner released
			// the planning lock. Neither entry set may be published at this point.
			annotated_lock_guard<annotated_mutex> guard(store.lock);
			try {
				store.GetDeleteIndexes({0});
				partial_index = true;
			} catch (InternalException &) {
			}
		}
		// Always unblock before assertions or future destruction, including on failure.
		gate.Release();
		auto owner_error = owner.get();
		auto waiter_error = waiter.get();
		INFO(owner_error);
		REQUIRE(entered);
		REQUIRE(registered);
		REQUIRE_FALSE(partial_index);
		REQUIRE(owner_error == waiter_error);
		REQUIRE(gate.OpenCount() == 1);
		if (failure) {
			REQUIRE_FALSE(owner_error.empty());
			REQUIRE(read({0, 1}) == owner_error);
			annotated_lock_guard<annotated_mutex> guard(store.lock);
			REQUIRE_THROWS(store.GetDeleteIndexes({0, 1}));
		} else {
			REQUIRE(owner_error.empty());
			REQUIRE(read({0, 1}).empty());
			annotated_lock_guard<annotated_mutex> guard(store.lock);
			auto indexes = store.GetDeleteIndexes({0, 1});
			REQUIRE(indexes.size() == 2);
			REQUIRE(indexes[0].index.get() == store.GetDeleteIndexes({0})[0].index.get());
			retained = indexes[0].index;
		}
	}
	if (retained) {
		// The reusable portion owns the raw entries, not a pointer into the store.
		REQUIRE_FALSE(retained->Entries().empty());
	}
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
