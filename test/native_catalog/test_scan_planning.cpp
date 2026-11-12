#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "core/expression/iceberg_value.hpp"
#include "function/scan_planning/iceberg_row_filter.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/metadata_io/avro/iceberg_manifest_cache.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

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
	Query(test.connection, "SET iceberg_enable_manifest_cache=false");
	Query(test.connection, "CREATE TABLE lake.default.t (id INTEGER) WITH "
	                       "('format-version'=3, 'commit.manifest-merge.enabled'='false')");
	Query(test.connection, "INSERT INTO lake.default.t SELECT range FROM range(10)");
	Query(test.connection, "INSERT INTO lake.default.t SELECT range FROM range(10,20)");
	Query(test.connection, "DELETE FROM lake.default.t WHERE id=1");
	Query(test.connection, "DELETE FROM lake.default.t WHERE id=12");
	auto bound = test.Bind();
	Query(test.connection, "SET iceberg_enable_manifest_cache=true");
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
	// A failed or interrupted load cannot leave the successfully read prefix cached.
	// A new store retries after the transport recovers.
	fs.UnregisterSubSystem("GatedManifestFileSystem");
	IcebergManifestStore retry(planning_lock, context);
	{
		annotated_lock_guard<annotated_mutex> guard(retry.lock);
		retry.LoadManifestList();
		auto descriptors = retry.DeleteManifests();
		IcebergManifestFileScanInfo info(context.metadata, context.snapshot, descriptors, context.options, fs,
		                                 context.path, nullptr, {0, 1});
		auto &cache = ObjectCache::GetObjectCache(context.context);
		for (idx_t i = 0; i < 2; i++) {
			auto cached = cache.GetWithTypePrefix<IcebergParsedManifest>(ManifestCacheKey(info, i));
			REQUIRE(bool(cached) == !failure);
		}
	}
	retry.ReadDeleteManifests({0, 1}, 0);
	{
		annotated_lock_guard<annotated_mutex> guard(retry.lock);
		auto indexes = retry.GetDeleteIndexes({0, 1});
		REQUIRE(indexes.size() == 2);
		if (retained) {
			REQUIRE(indexes[0].index.get() == retained.get());
		}
	}
}

TEST_CASE("Iceberg parsed manifest ownership inheritance identity and eviction", "[iceberg_planning]") {
	PlanningTest test;
	Query(test.connection, "SET iceberg_enable_manifest_cache=false");
	Query(test.connection, "CREATE TABLE lake.default.t (part INTEGER, id INTEGER) PARTITIONED BY (part) "
	                       "WITH ('format-version'=3, 'commit.manifest-merge.enabled'='false')");
	Query(test.connection, "INSERT INTO lake.default.t VALUES (1,11),(1,12)");
	Query(test.connection, "INSERT INTO lake.default.t VALUES (2,21),(2,22)");
	auto bound = test.Bind();
	auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
	auto &context = *test.connection.context;
	auto &fs = FileSystem::GetFileSystem(context);
	vector<IcebergManifestListEntry> descriptors;
	IcebergManifestList::LoadManifestFiles(base.GetSnapshot(), base.GetMetadata(), context, descriptors);
	REQUIRE(descriptors.size() == 2);
	Query(test.connection, "SET iceberg_enable_manifest_cache=true");
	auto read = [&](vector<IcebergManifestListEntry> &files, vector<idx_t> indexes) {
		auto scan = AvroScan::ScanManifest(base.GetSnapshot(), files, base.GetOptions(), fs, base.GetPath(),
		                                   base.GetMetadata(), context, nullptr, std::move(indexes));
		manifest_file::ManifestReader reader(*scan);
		while (!reader.Finished()) {
			reader.Read();
		}
		scan->Complete();
		return scan->scan_info->Cast<IcebergManifestFileScanInfo>().manifest_parses.load();
	};
	auto first = descriptors;
	REQUIRE(read(first, {0}) == 1);
	REQUIRE(first[0].parsed_manifest);
	REQUIRE_FALSE(first[1].HasManifestEntries());
	auto owner = first[0].parsed_manifest;
	auto entry_address = &owner->Entries()[0];
	auto file = descriptors[0].GetFile();
	file.sequence_number = 100;
	file.added_snapshot_id = 200;
	file.first_row_id = 300;
	vector<IcebergManifestListEntry> second {IcebergManifestListEntry(file), descriptors[1]};
	REQUIRE(read(second, {0, 1}) == 1);
	REQUIRE(second[0].parsed_manifest.get() == owner.get());
	REQUIRE(&second[0].GetManifestEntries()[0] == entry_address);
	REQUIRE(second[1].GetManifestEntries().size() == 1);
	const auto &entry = second[0].GetManifestEntries()[0];
	REQUIRE_FALSE(entry.ExplicitSequenceNumber());
	REQUIRE_FALSE(entry.ExplicitFileSequenceNumber());
	REQUIRE_FALSE(entry.HasSnapshotId());
	REQUIRE_FALSE(entry.data_file.HasFirstRowId());
	REQUIRE(entry.GetSequenceNumber(file) == 100);
	REQUIRE(entry.GetFileSequenceNumber(file) == 100);
	REQUIRE(entry.GetSnapshotId(file) == 200);
	BoundIcebergManifestListEntry binding(7, second[0], IcebergRowLineageMode::NONE);
	auto rebound = binding.BindEntry(entry);
	REQUIRE(rebound.manifest_file_idx == 7);
	REQUIRE(rebound.GetFirstRowId() == 300);
	// Rewriting resolves this descriptor's inheritance in a private copy.
	auto rewritten = file.PrepareEntriesForRewrite(second[0].TakeManifestEntries());
	REQUIRE(rewritten[0].GetSnapshotId() == entry.GetSnapshotId(file));
	REQUIRE(entry.status == IcebergManifestEntryStatusType::ADDED);
	REQUIRE(rewritten[0].status == IcebergManifestEntryStatusType::EXISTING);
	REQUIRE(&first[0].GetManifestEntries()[0] == entry_address);

	IcebergManifestFileScanInfo info(base.GetMetadata(), base.GetSnapshot(), descriptors, base.GetOptions(), fs,
	                                 base.GetPath(), nullptr, {0});
	auto key = ManifestCacheKey(info, 0);
	auto &cache = ObjectCache::GetObjectCache(context);
	REQUIRE(cache.GetWithTypePrefix<IcebergParsedManifest>(key).get() == owner.get());
	auto weight = owner->GetEstimatedCacheMemory().GetIndex();
	REQUIRE(weight > sizeof(IcebergManifestEntry));
	REQUIRE(cache.GetCurrentMemory() >= weight);
	REQUIRE(cache.EvictToReduceMemory(cache.GetCurrentMemory()) >= weight);
	REQUIRE_FALSE(cache.GetWithTypePrefix<IcebergParsedManifest>(key));
	REQUIRE(&owner->Entries()[0] == entry_address);
	auto after_eviction = descriptors;
	REQUIRE(read(after_eviction, {0}) == 1);
	REQUIRE(after_eviction[0].parsed_manifest.get() != owner.get());

	auto invalid_counts = descriptors;
	invalid_counts[0].GetManifest().counts = IcebergManifestCounts::Zero();
	ManifestEntryReadState batches;
	REQUIRE_THROWS_WITH(AvroScan::ScanManifest(base.GetSnapshot(), invalid_counts, base.GetOptions(), fs,
	                                           base.GetPath(), base.GetMetadata(), context, &batches, {0}),
	                    Catch::Contains("more entries than its manifest-list counts"));
	auto invalid_content = descriptors;
	invalid_content[0].GetManifest().content = IcebergManifestContentType::DELETE;
	REQUIRE_THROWS_WITH(read(invalid_content, {0}), Catch::Contains("header does not match"));

	// Interleave two misses before either one publishes, as in concurrent scans.
	cache.DeleteWithTypePrefix<IcebergParsedManifest>(key);
	auto racing_a = descriptors;
	auto racing_b = descriptors;
	auto start = [&](vector<IcebergManifestListEntry> &files) {
		return AvroScan::ScanManifest(base.GetSnapshot(), files, base.GetOptions(), fs, base.GetPath(),
		                              base.GetMetadata(), context, nullptr, {0});
	};
	auto scan_a = start(racing_a);
	auto scan_b = start(racing_b);
	for (auto scan : {scan_a.get(), scan_b.get()}) {
		manifest_file::ManifestReader reader(*scan);
		while (!reader.Finished()) {
			reader.Read();
		}
		scan->Complete();
	}
	REQUIRE(racing_a[0].parsed_manifest.get() != racing_b[0].parsed_manifest.get());
	REQUIRE(cache.GetWithTypePrefix<IcebergParsedManifest>(key).get() == racing_b[0].parsed_manifest.get());
	REQUIRE(racing_a[0].GetManifestEntries()[0].data_file.file_path ==
	        racing_b[0].GetManifestEntries()[0].data_file.file_path);

	cache.DeleteWithTypePrefix<IcebergParsedManifest>(key);
	{
		auto partial_files = descriptors;
		auto partial = start(partial_files);
		manifest_file::ManifestReader reader(*partial);
		reader.Read();
		REQUIRE_THROWS_WITH(partial->Complete(), Catch::Contains("incomplete manifest scan"));
		REQUIRE_FALSE(cache.GetWithTypePrefix<IcebergParsedManifest>(key));
	}
	{
		auto interrupted_files = descriptors;
		auto interrupted = start(interrupted_files);
		manifest_file::ManifestReader reader(*interrupted);
		while (!reader.Finished()) {
			reader.Read();
		}
		context.Interrupt();
		REQUIRE_THROWS_AS(interrupted->Complete(), InterruptException);
		context.ClearInterrupt();
		REQUIRE_FALSE(cache.GetWithTypePrefix<IcebergParsedManifest>(key));
	}
	auto retried = descriptors;
	REQUIRE(read(retried, {0}) == 1);

	// Publication-time validation failure cannot destroy entries already exposed
	// to streaming consumers, nor publish the successful prefix of the load.
	IcebergManifestFileScanInfo both_info(base.GetMetadata(), base.GetSnapshot(), descriptors, base.GetOptions(), fs,
	                                      base.GetPath(), nullptr, {0, 1});
	auto second_key = ManifestCacheKey(both_info, 1);
	cache.DeleteWithTypePrefix<IcebergParsedManifest>(key);
	cache.DeleteWithTypePrefix<IcebergParsedManifest>(second_key);
	auto rejected_files = descriptors;
	{
		auto rejected = AvroScan::ScanManifest(base.GetSnapshot(), rejected_files, base.GetOptions(), fs,
		                                       base.GetPath(), base.GetMetadata(), context);
		manifest_file::ManifestReader reader(*rejected);
		while (!reader.Finished()) {
			reader.Read();
		}
		auto retained_entry = &rejected_files[0].GetManifestEntries()[0];
		rejected_files[1].manifest_header["content"] = Value("deletes");
		REQUIRE_THROWS_WITH(rejected->Complete(), Catch::Contains("header does not match"));
		REQUIRE(&rejected_files[0].GetManifestEntries()[0] == retained_entry);
		REQUIRE(rejected_files[0].GetManifestEntries()[0].data_file.file_path == entry_address->data_file.file_path);
		REQUIRE_FALSE(cache.GetWithTypePrefix<IcebergParsedManifest>(key));
		REQUIRE_FALSE(cache.GetWithTypePrefix<IcebergParsedManifest>(second_key));
	}
	REQUIRE(rejected_files[0].HasManifestEntries());
	REQUIRE(rejected_files[1].HasManifestEntries());
	auto publication_retry = descriptors;
	REQUIRE(read(publication_retry, {0, 1}) == 2);

	info.file_secret.cache_scope = "different-attachment";
	REQUIRE(ManifestCacheKey(info, 0) != key);
	info.file_secret.cache_scope.clear();
	info.partition_field_id_to_type[1000] = LogicalType::BIGINT;
	REQUIRE(ManifestCacheKey(info, 0) != key);
	auto metadata = base.GetMetadata().Copy();
	IcebergManifestFileScanInfo changed(metadata, base.GetSnapshot(), descriptors, base.GetOptions(), fs,
	                                    base.GetPath(), nullptr, {0});
	REQUIRE(ManifestCacheKey(changed, 0) == key);
	metadata.table_uuid += "-other";
	REQUIRE(ManifestCacheKey(changed, 0) != key);
	metadata.table_uuid = base.GetMetadata().table_uuid;
	metadata.iceberg_version = 2;
	REQUIRE(ManifestCacheKey(changed, 0) != key);
	metadata.iceberg_version = 3;
	metadata.partition_specs.begin()->second.fields[0].transform = IcebergTransform("day");
	REQUIRE(ManifestCacheKey(changed, 0) != key);
	auto moved_options = base.GetOptions();
	moved_options.allow_moved_paths = true;
	IcebergManifestFileScanInfo moved_a(base.GetMetadata(), base.GetSnapshot(), descriptors, moved_options, fs,
	                                    "/moved-a", nullptr, {0});
	IcebergManifestFileScanInfo moved_b(base.GetMetadata(), base.GetSnapshot(), descriptors, moved_options, fs,
	                                    "/moved-b", nullptr, {0});
	REQUIRE(ManifestCacheKey(moved_a, 0) != ManifestCacheKey(moved_b, 0));
	REQUIRE(ManifestCacheKey(moved_a, 0) != key);
}

TEST_CASE("Iceberg frozen partition index survives its creating connection and cache eviction", "[iceberg_planning]") {
	DuckDB database(nullptr);
	shared_ptr<IcebergParsedManifest> payload;
	auto spec = PartitionSpec();
	auto data = DeleteEntry(0, Value(string(100, 'x'))).data_file;
	{
		Connection creator(database);
		IcebergTableMetadata metadata {IcebergTableMetadataSchemas()};
		metadata.partition_specs.emplace(17, spec);
		IcebergManifestListEntry manifest(
		    IcebergManifestFile("immutable.avro", 1024, 1, IcebergManifest(17, IcebergManifestContentType::DELETE, 1)),
		    IcebergManifestMetadata(0, 17, 2, IcebergManifestContentType::DELETE),
		    {DeleteEntry(0, Value(string(100, 'x'))), DeleteEntry(1, Value()),
		     DeleteEntry(2, Value(string(100, 'x')))});
		payload = make_shared_ptr<IcebergParsedManifest>(*creator.context, manifest, metadata, 32);
		auto &cache = ObjectCache::GetObjectCache(*creator.context);
		cache.PutWithTypePrefix<IcebergParsedManifest>("lifetime", payload);
	}
	Connection reader(database);
	auto &cache = ObjectCache::GetObjectCache(*reader.context);
	REQUIRE(cache.GetWithTypePrefix<IcebergParsedManifest>("lifetime").get() == payload.get());
	REQUIRE(Candidates(*payload->DeleteIndex(), data, 17) == vector<idx_t> {0, 2});
	cache.DeleteWithTypePrefix<IcebergParsedManifest>("lifetime");
	REQUIRE(Candidates(*payload->DeleteIndex(), data, 17) == vector<idx_t> {0, 2});
	REQUIRE(&payload->Entries()[0] == &payload->DeleteIndex()->Entries()[0]);
}

namespace {

IcebergPredicateStats MetricsBounds(const Value &lower, const Value &upper) {
	IcebergPredicateStats stats;
	stats.lower_bound = lower;
	stats.upper_bound = upper;
	stats.has_null = false;
	return stats;
}

void CheckMetrics(ClientContext &context, const ExpressionFilter &filter, const IcebergPredicateStats &stats,
                  IcebergMetricsMatch expected, const IcebergTransform &transform = IcebergTransform::Identity()) {
	INFO(filter.ToString("x"));
	INFO(transform.RawType());
	REQUIRE(IcebergPredicate::MatchBounds(context, filter, stats, transform, true) == expected);
	auto inclusive =
	    expected == IcebergMetricsMatch::ROWS_MUST_MATCH ? IcebergMetricsMatch::ROWS_MIGHT_MATCH : expected;
	REQUIRE(IcebergPredicate::MatchBounds(context, filter, stats, transform) == inclusive);
}

void CheckComparison(ClientContext &context, ExpressionType comparison, const Value &constant,
                     const IcebergPredicateStats &stats, IcebergMetricsMatch expected,
                     const IcebergTransform &transform = IcebergTransform::Identity()) {
	auto filter = ExpressionFilter::CreateComparisonFilter(comparison, constant);
	CheckMetrics(context, *filter, stats, expected, transform);
	ExpressionFilter reversed(BoundComparisonExpression::Create(
	    FlipComparisonExpression(comparison), make_uniq<BoundConstantExpression>(constant),
	    make_uniq<BoundReferenceExpression>(constant.type(), 0ULL)));
	CheckMetrics(context, reversed, stats, expected, transform);
}

void SetIntegerMetrics(IcebergDataFile &file, int32_t field_id, int32_t lower, int32_t upper) {
	auto lo = IcebergValue::SerializeValue(Value(lower), LogicalType::INTEGER, SerializeBound::LOWER_BOUND);
	auto hi = IcebergValue::SerializeValue(Value(upper), LogicalType::INTEGER, SerializeBound::UPPER_BOUND);
	REQUIRE_FALSE(lo.HasError());
	REQUIRE_FALSE(hi.HasError());
	file.lower_bounds[field_id] = lo.GetValue();
	file.upper_bounds[field_id] = hi.GetValue();
	file.value_counts[field_id] = file.record_count;
	file.null_value_counts[field_id] = 0;
}

IcebergManifestEntry MetricsEntry() {
	IcebergManifestEntry entry;
	entry.status = IcebergManifestEntryStatusType::ADDED;
	entry.data_file.content = IcebergManifestEntryContentType::DATA;
	entry.data_file.file_path = "data.parquet";
	entry.data_file.file_format = "parquet";
	entry.data_file.record_count = 10;
	entry.data_file.file_size_in_bytes = 100;
	return entry;
}

} // namespace

TEST_CASE("Iceberg strict metric steps preserve resolution and reject overflow", "[iceberg_metrics]") {
	Value result;
	for (auto &type :
	     vector<LogicalType> {LogicalType::TINYINT, LogicalType::SMALLINT, LogicalType::INTEGER, LogicalType::BIGINT,
	                          LogicalType::HUGEINT, LogicalType::UTINYINT, LogicalType::USMALLINT,
	                          LogicalType::UINTEGER, LogicalType::UBIGINT, LogicalType::UHUGEINT}) {
		INFO(type.ToString());
		REQUIRE(IcebergTryIncrement(Value(0).DefaultCastAs(type), result));
		REQUIRE(result.type() == type);
		REQUIRE(result == Value(1).DefaultCastAs(type));
		REQUIRE(IcebergTryDecrement(Value(1).DefaultCastAs(type), result));
		REQUIRE(result.type() == type);
		REQUIRE(result == Value(0).DefaultCastAs(type));
		REQUIRE_FALSE(IcebergTryIncrement(Value::MaximumValue(type), result));
		REQUIRE_FALSE(IcebergTryDecrement(Value::MinimumValue(type), result));
		REQUIRE_FALSE(IcebergTryIncrement(Value(type), result));
	}
	for (auto width : {4, 9, 18, 38}) {
		auto type = LogicalType::DECIMAL(width, 2);
		INFO(type.ToString());
		REQUIRE(IcebergTryIncrement(Value("-0.01").DefaultCastAs(type), result));
		REQUIRE(result.type() == type);
		REQUIRE(result == Value("0.00").DefaultCastAs(type));
		REQUIRE(IcebergTryDecrement(Value("0.00").DefaultCastAs(type), result));
		REQUIRE(result.type() == type);
		REQUIRE(result == Value("-0.01").DefaultCastAs(type));
		auto limit = string(width - 2, '9') + ".99";
		REQUIRE_FALSE(IcebergTryIncrement(Value(limit).DefaultCastAs(type), result));
		REQUIRE_FALSE(IcebergTryDecrement(Value("-" + limit).DefaultCastAs(type), result));
	}
	for (auto &type : vector<LogicalType> {LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_NS,
	                                       LogicalType::TIMESTAMP_TZ_NS}) {
		INFO(type.ToString());
		bool nanos = type.id() == LogicalTypeId::TIMESTAMP_NS || type.id() == LogicalTypeId::TIMESTAMP_TZ_NS;
		auto before_epoch =
		    Value(nanos ? "1969-12-31 23:59:59.999999999" : "1969-12-31 23:59:59.999999").DefaultCastAs(type);
		auto epoch = Value("1970-01-01 00:00:00").DefaultCastAs(type);
		REQUIRE(IcebergTryIncrement(before_epoch, result));
		REQUIRE(result.type() == type);
		REQUIRE(result == epoch);
		REQUIRE(IcebergTryDecrement(epoch, result));
		REQUIRE(result == before_epoch);
		REQUIRE_FALSE(IcebergTryIncrement(Value::MaximumValue(type), result));
		for (auto infinity : {"infinity", "-infinity"}) {
			REQUIRE_FALSE(IcebergTryIncrement(Value(infinity).DefaultCastAs(type), result));
			REQUIRE_FALSE(IcebergTryDecrement(Value(infinity).DefaultCastAs(type), result));
		}
	}
	REQUIRE(IcebergTryIncrement(Value::DATE(2000, 2, 28), result));
	REQUIRE(result == Value::DATE(2000, 2, 29));
	REQUIRE(IcebergTryDecrement(Value::DATE(2000, 3, 1), result));
	REQUIRE(result == Value::DATE(2000, 2, 29));
	REQUIRE_FALSE(IcebergTryIncrement(Value::DATE(date_t(date_t::infinity().days - 1)), result));
	REQUIRE_FALSE(IcebergTryDecrement(Value::DATE(date_t(date_t::ninfinity().days + 1)), result));
	for (auto infinity : {"infinity", "-infinity"}) {
		REQUIRE_FALSE(IcebergTryIncrement(Value(infinity).DefaultCastAs(LogicalType::DATE), result));
		REQUIRE_FALSE(IcebergTryDecrement(Value(infinity).DefaultCastAs(LogicalType::DATE), result));
	}
	for (auto &value : vector<Value> {Value(), Value("abc"), Value::DOUBLE(1), Value::FLOAT(1), Value::BOOLEAN(true),
	                                  Value::BLOB_RAW(string("abc"))}) {
		REQUIRE_FALSE(IcebergTryIncrement(value, result));
		REQUIRE_FALSE(IcebergTryDecrement(value, result));
	}
}

TEST_CASE("Iceberg strict identity metrics distinguish all some and no matching rows", "[iceberg_metrics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	auto &context = *connection.context;
	auto stats = MetricsBounds(Value(10), Value(20));
	using Match = IcebergMetricsMatch;
	for (auto comparison :
	     {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_LESSTHAN, ExpressionType::COMPARE_LESSTHANOREQUALTO,
	      ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_GREATERTHANOREQUALTO}) {
		for (auto constant : {9, 10, 15, 20, 21}) {
			// Evaluate every possible row in this small domain independently of the metrics evaluator.
			auto filter = ExpressionFilter::CreateComparisonFilter(comparison, Value(constant));
			idx_t matches = 0;
			for (int row = 10; row <= 20; row++) {
				matches += filter->EvaluateWithConstant(context, Value(row));
			}
			auto expected = matches == 0    ? Match::ROWS_CANNOT_MATCH
			                : matches == 11 ? Match::ROWS_MUST_MATCH
			                                : Match::ROWS_MIGHT_MATCH;
			CheckComparison(context, comparison, Value(constant), stats, expected);
		}
	}
	auto singleton = MetricsBounds(Value(10), Value(10));
	CheckComparison(context, ExpressionType::COMPARE_EQUAL, Value(10), singleton, Match::ROWS_MUST_MATCH);
	singleton.has_null = true;
	CheckComparison(context, ExpressionType::COMPARE_EQUAL, Value(10), singleton, Match::ROWS_MIGHT_MATCH);
	singleton.has_null = false;
	singleton.lower_bound.reset();
	CheckComparison(context, ExpressionType::COMPARE_EQUAL, Value(10), singleton, Match::ROWS_MIGHT_MATCH);
	CheckComparison(context, ExpressionType::COMPARE_EQUAL, Value(LogicalType::INTEGER), stats,
	                Match::ROWS_CANNOT_MATCH);

	auto floating = MetricsBounds(Value::DOUBLE(10), Value::DOUBLE(20));
	CheckComparison(context, ExpressionType::COMPARE_LESSTHAN, Value::DOUBLE(30), floating, Match::ROWS_MIGHT_MATCH);
	floating.has_nan = true;
	CheckComparison(context, ExpressionType::COMPARE_LESSTHAN, Value::DOUBLE(30), floating, Match::ROWS_MIGHT_MATCH);
	CheckComparison(context, ExpressionType::COMPARE_GREATERTHAN, Value::DOUBLE(30), floating, Match::ROWS_MIGHT_MATCH);
	floating.has_nan = false;
	CheckComparison(context, ExpressionType::COMPARE_LESSTHAN, Value::DOUBLE(30), floating, Match::ROWS_MUST_MATCH);
	CheckComparison(context, ExpressionType::COMPARE_GREATERTHAN, Value::DOUBLE(30), floating,
	                Match::ROWS_CANNOT_MATCH);
}

TEST_CASE("Iceberg strict null IN conjunction and optional filters remain conservative", "[iceberg_metrics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	auto &context = *connection.context;
	using Match = IcebergMetricsMatch;
	auto stats = MetricsBounds(Value(10), Value(10));
	ExpressionFilter is_null(ExpressionFilter::CreateNullCheckExpression(
	    make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL), ExpressionType::OPERATOR_IS_NULL));
	ExpressionFilter not_null(ExpressionFilter::CreateNullCheckExpression(
	    make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL), ExpressionType::OPERATOR_IS_NOT_NULL));
	CheckMetrics(context, is_null, stats, Match::ROWS_CANNOT_MATCH);
	CheckMetrics(context, not_null, stats, Match::ROWS_MUST_MATCH);
	stats.has_null = true;
	CheckMetrics(context, is_null, stats, Match::ROWS_MIGHT_MATCH);
	CheckMetrics(context, not_null, stats, Match::ROWS_MIGHT_MATCH);
	stats.has_not_null = false;
	stats.lower_bound.reset();
	stats.upper_bound.reset();
	CheckMetrics(context, is_null, stats, Match::ROWS_MUST_MATCH);
	CheckMetrics(context, not_null, stats, Match::ROWS_CANNOT_MATCH);
	stats = MetricsBounds(Value(10), Value(10));
	ExpressionFilter in(ExpressionFilter::CreateInExpression(
	    make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0ULL), {Value(5), Value(10), Value(25)}));
	CheckMetrics(context, in, stats, Match::ROWS_MUST_MATCH);
	stats.upper_bound = Value(20);
	CheckMetrics(context, in, stats, Match::ROWS_MIGHT_MATCH);
	stats.lower_bound = Value(11);
	CheckMetrics(context, in, stats, Match::ROWS_CANNOT_MATCH);

	stats = MetricsBounds(Value(10), Value(20));
	for (auto upper : {9, 15, 20}) {
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		conjunction->GetChildrenMutable().push_back(
		    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value(10))
		        ->expr->Copy());
		conjunction->GetChildrenMutable().push_back(
		    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value(upper))
		        ->expr->Copy());
		ExpressionFilter filter(std::move(conjunction));
		CheckMetrics(context, filter, stats,
		             upper == 9    ? Match::ROWS_CANNOT_MATCH
		             : upper == 20 ? Match::ROWS_MUST_MATCH
		                           : Match::ROWS_MIGHT_MATCH);
	}
	for (bool selectivity : {false, true}) {
		for (auto constant : {5, 25}) {
			auto child = ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHAN, Value(constant));
			ExpressionFilter optional(
			    selectivity
			        ? CreateSelectivityOptionalFilterExpression(child->expr->Copy(), LogicalType::INTEGER, 0.5, 1)
			        : CreateOptionalFilterExpression(child->expr->Copy(), LogicalType::INTEGER));
			CheckMetrics(context, optional, stats, constant == 5 ? Match::ROWS_MIGHT_MATCH : Match::ROWS_CANNOT_MATCH);
		}
	}
}

TEST_CASE("Iceberg strict floating file bounds account for NaNs and missing metrics", "[iceberg_metrics]") {
	using Match = IcebergMetricsMatch;
	for (auto &type : vector<LogicalType> {LogicalType::FLOAT, LogicalType::DOUBLE}) {
		INFO(type.ToString());
		PlanningTest test;
		Query(test.connection, "CREATE TABLE lake.default.t (x " + type.ToString() + ", p INTEGER)");
		auto bound = test.Bind();
		auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
		auto metadata = base.GetMetadata().Copy();
		auto &schema = base.GetSchema();
		auto field_id = schema.columns[0]->id;
		// An evolved spec can retain a void source alongside a live partition field.
		// The void value cannot settle x's filter, so coverage must come from file metrics.
		auto spec = PartitionSpec();
		spec.fields[0].source_id = field_id;
		spec.fields[0].transform = IcebergTransform("void");
		auto live_field = spec.fields[0];
		live_field.source_id = schema.columns[1]->id;
		live_field.partition_field_id = 1001;
		live_field.transform = IcebergTransform::Identity();
		spec.fields.push_back(live_field);
		metadata.partition_specs.emplace(spec.spec_id, spec);
		IcebergManifest manifest(spec.spec_id, IcebergManifestContentType::DATA, 1);
		auto entry = MetricsEntry();
		auto &file = entry.data_file;
		file.partition_info = {{1000, Value(type)}, {1001, Value(1)}};
		file.record_count = 2;
		file.value_counts[field_id] = 2;
		file.null_value_counts[field_id] = 0;
		auto zero = Value(0).DefaultCastAs(type);
		auto one = Value(1).DefaultCastAs(type);
		auto two = Value(2).DefaultCastAs(type);
		auto nan = Value("NaN").DefaultCastAs(type);
		file.lower_bounds[field_id] = IcebergValue::SerializeValue(one, type, SerializeBound::LOWER_BOUND).GetValue();
		file.upper_bounds[field_id] = IcebergValue::SerializeValue(one, type, SerializeBound::UPPER_BOUND).GetValue();
		auto check = [&](ExpressionType comparison, const Value &constant, Match expected) {
			auto filter = ExpressionFilter::CreateComparisonFilter(comparison, constant);
			INFO(filter->ToString("x"));
			IcebergTableFilters filters;
			filters.PushFilter(ColumnIndex(0), std::move(filter));
			IcebergFilePruner pruner(*test.connection.context, metadata, schema, filters);
			CHECK(pruner.FileMatchesFilter(manifest, entry, true) == expected);
			CHECK(pruner.FileMatchesFilter(manifest, entry) ==
			      (expected == Match::ROWS_MUST_MATCH ? Match::ROWS_MIGHT_MATCH : expected));
		};
		for (auto nan_count : {-1, 0, 1}) {
			INFO(nan_count);
			if (nan_count >= 0) {
				file.nan_value_counts[field_id] = nan_count;
			}
			auto finite_only = nan_count == 0 ? Match::ROWS_MUST_MATCH : Match::ROWS_MIGHT_MATCH;
			check(ExpressionType::COMPARE_LESSTHAN, two, finite_only);
			check(ExpressionType::COMPARE_LESSTHANOREQUALTO, one, finite_only);
			check(ExpressionType::COMPARE_EQUAL, one, finite_only);
			check(ExpressionType::COMPARE_LESSTHAN, nan, finite_only);
			// NaN sorts above finite values, so it cannot invalidate these coverage proofs.
			check(ExpressionType::COMPARE_GREATERTHAN, zero, Match::ROWS_MUST_MATCH);
			check(ExpressionType::COMPARE_GREATERTHANOREQUALTO, one, Match::ROWS_MUST_MATCH);
			check(ExpressionType::COMPARE_LESSTHANOREQUALTO, nan, Match::ROWS_MUST_MATCH);
		}
		file.nan_value_counts[field_id] = 0;
		file.null_value_counts[field_id] = 1;
		check(ExpressionType::COMPARE_GREATERTHAN, zero, Match::ROWS_MIGHT_MATCH);
		file.null_value_counts.clear();
		check(ExpressionType::COMPARE_GREATERTHAN, zero, Match::ROWS_MIGHT_MATCH);
		file.null_value_counts[field_id] = 0;
		file.lower_bounds.clear();
		check(ExpressionType::COMPARE_GREATERTHAN, zero, Match::ROWS_MIGHT_MATCH);
	}
}

TEST_CASE("Iceberg strict floating identity partitions need no file metrics", "[iceberg_metrics]") {
	using Match = IcebergMetricsMatch;
	for (auto &type : vector<LogicalType> {LogicalType::FLOAT, LogicalType::DOUBLE}) {
		INFO(type.ToString());
		PlanningTest test;
		Query(test.connection, "CREATE TABLE lake.default.t (x " + type.ToString() + ")");
		auto bound = test.Bind();
		auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
		auto metadata = base.GetMetadata().Copy();
		auto spec = PartitionSpec();
		spec.fields[0].source_id = base.GetSchema().columns[0]->id;
		metadata.partition_specs.emplace(spec.spec_id, spec);
		IcebergManifest manifest(spec.spec_id, IcebergManifestContentType::DATA, 1);
		auto entry = MetricsEntry();
		auto check = [&](ExpressionType comparison, const Value &constant, Match expected) {
			auto filter = ExpressionFilter::CreateComparisonFilter(comparison, constant);
			INFO(filter->ToString("x"));
			IcebergTableFilters filters;
			filters.PushFilter(ColumnIndex(0), std::move(filter));
			IcebergFilePruner pruner(*test.connection.context, metadata, base.GetSchema(), filters);
			CHECK(pruner.FileMatchesFilter(manifest, entry, true) == expected);
			CHECK(pruner.FileMatchesFilter(manifest, entry) ==
			      (expected == Match::ROWS_MUST_MATCH ? Match::ROWS_MIGHT_MATCH : expected));
		};
		auto nan = Value("NaN").DefaultCastAs(type);
		for (auto literal : {"1", "Infinity", "NaN"}) {
			INFO(literal);
			auto partition = Value(literal).DefaultCastAs(type);
			entry.data_file.partition_info = {{1000, partition}};
			check(ExpressionType::COMPARE_EQUAL, partition, Match::ROWS_MUST_MATCH);
			check(ExpressionType::COMPARE_GREATERTHAN, Value(0).DefaultCastAs(type), Match::ROWS_MUST_MATCH);
			check(ExpressionType::COMPARE_LESSTHANOREQUALTO, nan, Match::ROWS_MUST_MATCH);
			check(ExpressionType::COMPARE_LESSTHAN, nan,
			      string(literal) == "NaN" ? Match::ROWS_CANNOT_MATCH : Match::ROWS_MUST_MATCH);
			check(ExpressionType::COMPARE_EQUAL, nan,
			      string(literal) == "NaN" ? Match::ROWS_MUST_MATCH : Match::ROWS_CANNOT_MATCH);
		}
		entry.data_file.partition_info = {{1000, Value(type)}};
		check(ExpressionType::COMPARE_EQUAL, Value(1).DefaultCastAs(type), Match::ROWS_CANNOT_MATCH);
		check(ExpressionType::COMPARE_EQUAL, nan, Match::ROWS_CANNOT_MATCH);
	}
}

TEST_CASE("Iceberg strict equality recognizes single-value truncated partitions", "[iceberg_metrics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	auto &context = *connection.context;
	using Match = IcebergMetricsMatch;
	IcebergTransform transform("truncate[1]");
	for (auto &type :
	     vector<LogicalType> {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::DECIMAL(4, 2),
	                          LogicalType::DECIMAL(9, 2), LogicalType::DECIMAL(18, 2), LogicalType::DECIMAL(38, 2)}) {
		INFO(type.ToString());
		bool decimal = type.id() == LogicalTypeId::DECIMAL;
		for (auto literal : {decimal ? "-0.01" : "-1", "0", "1"}) {
			auto value = Value(literal).DefaultCastAs(type);
			auto partition = transform.ApplyTransform(value);
			auto stats = MetricsBounds(partition, partition);
			CheckComparison(context, ExpressionType::COMPARE_EQUAL, value, stats, Match::ROWS_MUST_MATCH, transform);
			Value successor;
			REQUIRE(IcebergTryIncrement(value, successor));
			CheckComparison(context, ExpressionType::COMPARE_EQUAL, successor, stats, Match::ROWS_CANNOT_MATCH,
			                transform);
			stats.has_null = true;
			CheckComparison(context, ExpressionType::COMPARE_EQUAL, value, stats, Match::ROWS_MIGHT_MATCH, transform);
		}
	}
	// Missing predecessor/successor values must leave the strict projection conservative.
	for (auto &boundary :
	     vector<Value> {Value::MinimumValue(LogicalType::INTEGER), Value::MaximumValue(LogicalType::INTEGER)}) {
		CheckComparison(context, ExpressionType::COMPARE_EQUAL, boundary, MetricsBounds(boundary, boundary),
		                Match::ROWS_MIGHT_MATCH, transform);
	}
}

TEST_CASE("Iceberg strict partition projections respect discrete boundaries", "[iceberg_metrics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	auto &context = *connection.context;
	using Match = IcebergMetricsMatch;
	for (auto &type :
	     vector<LogicalType> {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::DECIMAL(4, 2),
	                          LogicalType::DECIMAL(9, 2), LogicalType::DECIMAL(18, 2), LogicalType::DECIMAL(38, 2)}) {
		bool decimal = type.id() == LogicalTypeId::DECIMAL;
		for (bool negative : {false, true}) {
			auto start =
			    Value(negative ? (decimal ? "-1.00" : "-100") : (decimal ? "1.00" : "100")).DefaultCastAs(type);
			auto end = Value(negative ? (decimal ? "-0.01" : "-1") : (decimal ? "1.99" : "199")).DefaultCastAs(type);
			auto stats = MetricsBounds(start, start);
			IcebergTransform transform("truncate[100]");
			CheckComparison(context, ExpressionType::COMPARE_GREATERTHANOREQUALTO, start, stats, Match::ROWS_MUST_MATCH,
			                transform);
			CheckComparison(context, ExpressionType::COMPARE_GREATERTHAN, start, stats, Match::ROWS_MIGHT_MATCH,
			                transform);
			CheckComparison(context, ExpressionType::COMPARE_LESSTHANOREQUALTO, end, stats, Match::ROWS_MUST_MATCH,
			                transform);
			CheckComparison(context, ExpressionType::COMPARE_LESSTHAN, end, stats, Match::ROWS_MIGHT_MATCH, transform);
			CheckComparison(context, ExpressionType::COMPARE_EQUAL, start, stats, Match::ROWS_MIGHT_MATCH, transform);
		}
	}
	for (auto &type : vector<LogicalType> {LogicalType::DATE, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP_TZ,
	                                       LogicalType::TIMESTAMP_NS, LogicalType::TIMESTAMP_TZ_NS}) {
		for (auto transform_name : {"year", "month", "day", "hour"}) {
			if (type.id() == LogicalTypeId::DATE && string(transform_name) == "hour") {
				continue;
			}
			INFO(type.ToString());
			INFO(transform_name);
			IcebergTransform transform(transform_name);
			auto first = Value("2024-01-01").DefaultCastAs(type);
			auto next = Value(string(transform_name) == "year"    ? "2025-01-01"
			                  : string(transform_name) == "month" ? "2024-02-01"
			                  : string(transform_name) == "day"   ? "2024-01-02"
			                                                      : "2024-01-01 01:00:00")
			                .DefaultCastAs(type);
			Value last;
			REQUIRE(IcebergTryDecrement(next, last));
			auto partition = transform.ApplyTransform(first);
			auto stats = MetricsBounds(partition, partition);
			CheckComparison(context, ExpressionType::COMPARE_GREATERTHANOREQUALTO, first, stats, Match::ROWS_MUST_MATCH,
			                transform);
			CheckComparison(context, ExpressionType::COMPARE_LESSTHANOREQUALTO, last, stats, Match::ROWS_MUST_MATCH,
			                transform);
			CheckComparison(context, ExpressionType::COMPARE_GREATERTHAN, first, stats, Match::ROWS_MIGHT_MATCH,
			                transform);
			auto equality = type.id() == LogicalTypeId::DATE && string(transform_name) == "day"
			                    ? Match::ROWS_MUST_MATCH
			                    : Match::ROWS_MIGHT_MATCH;
			CheckComparison(context, ExpressionType::COMPARE_EQUAL, first, stats, equality, transform);
			// A legacy writer can put a pre-epoch row in partition zero by truncating toward zero.
			auto epoch = Value("1970-01-01").DefaultCastAs(type);
			auto legacy = MetricsBounds(Value(0), Value(0));
			CheckComparison(context, ExpressionType::COMPARE_GREATERTHANOREQUALTO, epoch, legacy,
			                Match::ROWS_MIGHT_MATCH, transform);
			CheckComparison(context, ExpressionType::COMPARE_EQUAL, epoch, legacy, Match::ROWS_MIGHT_MATCH, transform);
		}
	}
	auto stats = MetricsBounds(Value("abc"), Value("abc"));
	CheckComparison(context, ExpressionType::COMPARE_LESSTHAN, Value("abd"), stats, Match::ROWS_MUST_MATCH,
	                IcebergTransform("truncate[3]"));
	CheckComparison(context, ExpressionType::COMPARE_EQUAL, Value("abcdef"), stats, Match::ROWS_MIGHT_MATCH,
	                IcebergTransform("truncate[3]"));
	auto bucket = IcebergTransform("bucket[4]");
	auto partition = bucket.ApplyTransform(Value(10));
	stats = MetricsBounds(partition, partition);
	CheckComparison(context, ExpressionType::COMPARE_EQUAL, Value(10), stats, Match::ROWS_MIGHT_MATCH, bucket);
	stats = MetricsBounds(Value(LogicalType::INTEGER), Value(LogicalType::INTEGER));
	stats.has_null = true;
	CheckComparison(context, ExpressionType::COMPARE_EQUAL, Value(10), stats, Match::ROWS_MIGHT_MATCH,
	                IcebergTransform("void"));
}

TEST_CASE("Iceberg strict file metrics require proof for every filter and only cover data files", "[iceberg_metrics]") {
	PlanningTest test;
	Query(test.connection, "CREATE TABLE lake.default.t (id INTEGER, other INTEGER)");
	auto bound = test.Bind();
	auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
	auto metadata = base.GetMetadata().Copy();
	auto schema = LogicalType::STRUCT({{"id", LogicalType::INTEGER}, {"other", LogicalType::INTEGER}});
	auto filters = Filters(*test.connection.context, schema, "id >= 10 AND other = 20");
	REQUIRE(filters.FilterCount() == 2);
	IcebergFilePruner pruner(*test.connection.context, metadata, base.GetSchema(), filters);
	IcebergManifest manifest(0, IcebergManifestContentType::DATA, 1);
	auto entry = MetricsEntry();
	auto id = base.GetSchema().columns[0]->id;
	auto other = base.GetSchema().columns[1]->id;
	SetIntegerMetrics(entry.data_file, id, 10, 30);
	using Match = IcebergMetricsMatch;
	REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == Match::ROWS_MIGHT_MATCH);
	SetIntegerMetrics(entry.data_file, other, 20, 20);
	REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == Match::ROWS_MUST_MATCH);
	REQUIRE(pruner.FileMatchesFilter(manifest, entry) == Match::ROWS_MIGHT_MATCH);
	entry.data_file.null_value_counts.erase(other);
	REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == Match::ROWS_MIGHT_MATCH);
	SetIntegerMetrics(entry.data_file, other, 21, 21);
	REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == Match::ROWS_CANNOT_MATCH);
	SetIntegerMetrics(entry.data_file, other, 20, 20);
	for (auto content :
	     {IcebergManifestEntryContentType::POSITION_DELETES, IcebergManifestEntryContentType::EQUALITY_DELETES}) {
		entry.data_file.content = content;
		REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == Match::ROWS_MIGHT_MATCH);
	}
	entry.data_file.content = IcebergManifestEntryContentType::DATA;
	// Both partition fields prove the same filter; neither proves "other = 20".
	auto spec = PartitionSpec();
	spec.fields[0].source_id = id;
	auto second = spec.fields[0];
	second.partition_field_id++;
	second.transform = IcebergTransform("truncate[10]");
	spec.fields.push_back(second);
	metadata.partition_specs.emplace(spec.spec_id, spec);
	manifest.partition_spec_id = spec.spec_id;
	entry.data_file.partition_info = {{1000, Value(10)}, {1001, Value(10)}};
	entry.data_file.lower_bounds.clear();
	entry.data_file.upper_bounds.clear();
	entry.data_file.null_value_counts.clear();
	REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == Match::ROWS_MIGHT_MATCH);
	SetIntegerMetrics(entry.data_file, other, 20, 20);
	REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == Match::ROWS_MUST_MATCH);
}

TEST_CASE("Iceberg strict partition metrics cannot cover a sibling struct predicate", "[iceberg_metrics]") {
	PlanningTest test;
	Query(test.connection, "CREATE TABLE lake.default.t (s STRUCT(x INTEGER, y INTEGER))");
	auto bound = test.Bind();
	auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
	auto metadata = base.GetMetadata().Copy();
	auto nested = LogicalType::STRUCT({{"x", LogicalType::INTEGER}, {"y", LogicalType::INTEGER}});
	auto schema = LogicalType::STRUCT({{"s", nested}});
	IcebergTableFilters filters;
	auto expression = IcebergRowFilter::Bind(*test.connection.context, "s.x=10 AND s.y=20", schema);
	// The scan optimizer folds literal casts before handing filters to the pruner.
	for (auto &child : expression->Cast<BoundConjunctionExpression>().GetChildrenMutable()) {
		auto &constant = BoundComparisonExpression::RightMutable(child->Cast<BoundFunctionExpression>());
		constant =
		    make_uniq<BoundConstantExpression>(ExpressionExecutor::EvaluateScalar(*test.connection.context, *constant));
	}
	filters.PushFilter(ColumnIndex(0), make_uniq<ExpressionFilter>(std::move(expression)));
	auto spec = PartitionSpec();
	spec.fields[0].source_id = base.GetSchema().columns[0]->GetChild(0)->id;
	auto path = base.GetSchema().GetSourceIdMap().at(spec.fields[0].source_id);
	auto extracted = filters.GetFilterForColumnIndex(path);
	REQUIRE(extracted);
	INFO(extracted->ToString("x"));
	CheckMetrics(*test.connection.context, *extracted, MetricsBounds(Value(10), Value(10)),
	             IcebergMetricsMatch::ROWS_MUST_MATCH);
	metadata.partition_specs.emplace(spec.spec_id, spec);
	IcebergFilePruner pruner(*test.connection.context, metadata, base.GetSchema(), filters);
	IcebergManifest manifest(spec.spec_id, IcebergManifestContentType::DATA, 1);
	auto entry = MetricsEntry();
	entry.data_file.partition_info = {{1000, Value(10)}};
	REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == IcebergMetricsMatch::ROWS_MIGHT_MATCH);
	entry.data_file.partition_info[0].value = Value(11);
	REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == IcebergMetricsMatch::ROWS_CANNOT_MATCH);
}

TEST_CASE("Iceberg missing variant metrics do not hide another column's non-match", "[iceberg_metrics]") {
	// Keep the filter keys and insertion order fixed while swapping the column types. This exercises
	// the unknown variant both before and after the integer, regardless of unordered_map traversal order.
	for (bool variant_first : {false, true}) {
		PlanningTest test;
		Query(test.connection, variant_first
		                           ? "CREATE TABLE lake.default.t (a VARIANT, b INTEGER) WITH ('format-version'=3)"
		                           : "CREATE TABLE lake.default.t (a INTEGER, b VARIANT) WITH ('format-version'=3)");
		auto bound = test.Bind();
		auto &base = bound->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
		IcebergTableFilters filters;
		for (idx_t i = 0; i < 2; i++) {
			auto type = base.GetSchema().columns[i]->type;
			filters.PushFilter(
			    ColumnIndex(i),
			    type.id() == LogicalTypeId::VARIANT
			        ? make_uniq<ExpressionFilter>(ExpressionFilter::CreateNullCheckExpression(
			              make_uniq<BoundReferenceExpression>(type, 0ULL), ExpressionType::OPERATOR_IS_NOT_NULL))
			        : ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value(20)));
		}
		IcebergFilePruner pruner(*test.connection.context, base.GetMetadata(), base.GetSchema(), filters);
		IcebergManifest manifest(0, IcebergManifestContentType::DATA, 1);
		auto entry = MetricsEntry();
		auto integer_id = base.GetSchema().columns[variant_first ? 1 : 0]->id;
		SetIntegerMetrics(entry.data_file, integer_id, 21, 21);
		REQUIRE(pruner.FileMatchesFilter(manifest, entry) == IcebergMetricsMatch::ROWS_CANNOT_MATCH);
		REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == IcebergMetricsMatch::ROWS_CANNOT_MATCH);
		SetIntegerMetrics(entry.data_file, integer_id, 20, 20);
		REQUIRE(pruner.FileMatchesFilter(manifest, entry, true) == IcebergMetricsMatch::ROWS_MIGHT_MATCH);
	}
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
