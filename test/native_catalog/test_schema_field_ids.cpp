#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "duckdb.hpp"
// Catch includes Windows headers, whose DELETE macro conflicts with Iceberg's enums.
#include "duckdb/common/windows_undefs.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"

using namespace duckdb;

namespace {

struct SchemaTest {
	SchemaTest() : database(nullptr), connection(database) {
	}

	shared_ptr<IcebergTableSchema> Schema(const child_list_t<LogicalType> &fields) {
		ColumnList columns;
		for (auto &field : fields) {
			columns.AddColumn(ColumnDefinition(Identifier(field.first), field.second));
		}
		IcebergTableMetadata metadata {IcebergTableMetadataSchemas()};
		metadata.iceberg_version = 2;
		int32_t last_id;
		auto schema =
		    IcebergCreateTableRequest::CreateIcebergSchema(*connection.context, metadata, columns, nullptr, last_id);
		schema->last_column_id = last_id;
		return schema;
	}

	IcebergTableMetadata Metadata(const shared_ptr<IcebergTableSchema> &schema, idx_t last_id) {
		IcebergTableMetadata metadata {IcebergTableMetadataSchemas({{schema->schema_id, schema}})};
		metadata.SetCurrentSchemaId(schema->schema_id);
		metadata.last_column_id = last_id;
		return metadata;
	}

	DuckDB database;
	Connection connection;
};

void CheckFields(const IcebergTableSchema &schema, const vector<pair<int32_t, string>> &expected) {
	REQUIRE(schema.GetFullNames() == expected);
	REQUIRE(schema.GetSourceIdMap().size() == expected.size());
	for (auto &field : expected) {
		REQUIRE(schema.GetFullColumnName(schema.GetColumnByFieldId(field.first).id) == field.second);
	}
}

} // namespace

TEST_CASE("Replacement field IDs follow names across reordering", "[iceberg_schema]") {
	SchemaTest test;
	auto original = test.Schema({{"id", LogicalType::INTEGER}, {"name", LogicalType::VARCHAR}});
	auto metadata = test.Metadata(original, 10);
	auto replacement = test.Schema({{"name", LogicalType::VARCHAR}, {"id", LogicalType::BIGINT}});
	REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) == 10);
	REQUIRE(replacement->last_column_id == 10);
	CheckFields(*replacement, {{2, "name"}, {1, "id"}});
	REQUIRE(replacement->columns[1]->type == LogicalType::BIGINT);
	CheckFields(metadata.GetLatestSchema(), {{1, "id"}, {2, "name"}});
	REQUIRE(metadata.GetLastColumnId() == 10);
}

TEST_CASE("Replacement field IDs match case-sensitive full paths", "[iceberg_schema]") {
	SchemaTest test;
	auto original =
	    test.Schema({{"id", LogicalType::INTEGER},
	                 {"s", LogicalType::STRUCT({{"id", LogicalType::INTEGER}, {"kept", LogicalType::INTEGER}})}});
	auto metadata = test.Metadata(original, 10);
	auto replacement =
	    test.Schema({{"ID", LogicalType::INTEGER},
	                 {"s", LogicalType::STRUCT({{"ID", LogicalType::INTEGER}, {"kept", LogicalType::INTEGER}})},
	                 {"other", LogicalType::STRUCT({{"id", LogicalType::INTEGER}})}});
	REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) == 14);
	CheckFields(*replacement, {{11, "ID"}, {2, "s"}, {13, "s.ID"}, {4, "s.kept"}, {12, "other"}, {14, "other.id"}});
}

TEST_CASE("Replacement field IDs preserve nested struct list and map fields", "[iceberg_schema]") {
	SchemaTest test;
	auto old_struct = LogicalType::STRUCT({{"x", LogicalType::INTEGER}, {"y", LogicalType::VARCHAR}});
	auto new_struct =
	    LogicalType::STRUCT({{"y", LogicalType::VARCHAR}, {"x", LogicalType::INTEGER}, {"z", LogicalType::BIGINT}});
	auto original = test.Schema({{"s", old_struct},
	                             {"l", LogicalType::LIST(old_struct)},
	                             {"m", LogicalType::MAP(LogicalType::VARCHAR, old_struct)}});
	auto metadata = test.Metadata(original, 20);
	auto replacement = test.Schema({{"m", LogicalType::MAP(LogicalType::VARCHAR, new_struct)},
	                                {"l", LogicalType::LIST(new_struct)},
	                                {"s", new_struct}});
	REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) == 23);
	REQUIRE(replacement->last_column_id == 23);
	CheckFields(*replacement, {{8, "m"},
	                           {9, "m.key"},
	                           {10, "m.value"},
	                           {12, "m.value.y"},
	                           {11, "m.value.x"},
	                           {21, "m.value.z"},
	                           {4, "l"},
	                           {5, "l.element"},
	                           {7, "l.element.y"},
	                           {6, "l.element.x"},
	                           {22, "l.element.z"},
	                           {1, "s"},
	                           {3, "s.y"},
	                           {2, "s.x"},
	                           {23, "s.z"}});
}

TEST_CASE("Replacement allocates sibling IDs before their children", "[iceberg_schema]") {
	SchemaTest test;
	auto original = test.Schema({{"old", LogicalType::INTEGER}});
	auto metadata = test.Metadata(original, 10);
	auto replacement =
	    test.Schema({{"s", LogicalType::STRUCT({{"x", LogicalType::INTEGER}, {"y", LogicalType::INTEGER}})},
	                 {"l", LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalType::INTEGER}}))},
	                 {"m", LogicalType::MAP(LogicalType::STRUCT({{"k", LogicalType::INTEGER}}),
	                                        LogicalType::STRUCT({{"v", LogicalType::INTEGER}}))}});
	REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) == 21);
	CheckFields(*replacement, {{11, "s"},
	                           {14, "s.x"},
	                           {15, "s.y"},
	                           {12, "l"},
	                           {16, "l.element"},
	                           {17, "l.element.x"},
	                           {13, "m"},
	                           {18, "m.key"},
	                           {20, "m.key.k"},
	                           {19, "m.value"},
	                           {21, "m.value.v"}});
}

TEST_CASE("Replacement never reuses IDs from historical schemas", "[iceberg_schema]") {
	SchemaTest test;
	auto historical = test.Schema({{"id", LogicalType::INTEGER}, {"removed", LogicalType::INTEGER}});
	auto current = test.Schema({{"id", LogicalType::INTEGER}});
	current->schema_id = 1;
	auto metadata = test.Metadata(historical, 20);
	metadata.GetSchemasMutable().AddSchemaOrGetExisting(current);
	metadata.SetCurrentSchemaId(1);
	auto replacement =
	    test.Schema({{"removed", LogicalType::INTEGER}, {"id", LogicalType::INTEGER}, {"new", LogicalType::INTEGER}});
	REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) == 22);
	CheckFields(*replacement, {{21, "removed"}, {1, "id"}, {22, "new"}});
	CheckFields(metadata.GetSchemaFromId(0), {{1, "id"}, {2, "removed"}});
	REQUIRE(metadata.GetLastColumnId() == 20);
}

TEST_CASE("Replacement refreshes existing field ID lookups", "[iceberg_schema]") {
	SchemaTest test;
	auto original =
	    test.Schema({{"id", LogicalType::INTEGER}, {"s", LogicalType::STRUCT({{"x", LogicalType::INTEGER}})}});
	auto metadata = test.Metadata(original, 10);
	auto replacement =
	    test.Schema({{"added", LogicalType::INTEGER},
	                 {"s", LogicalType::STRUCT({{"x", LogicalType::INTEGER}, {"added", LogicalType::INTEGER}})},
	                 {"id", LogicalType::BIGINT}});
	SECTION("Lookups were populated before assignment") {
		REQUIRE(replacement->GetSourceIdMap().size() == 5);
		REQUIRE(replacement->GetColumnByFieldId(1).name == "added");
	}
	SECTION("Parsing a schema populates its lookups") {
		replacement = IcebergTableSchema::ParseSchema(replacement->ToRESTObject());
	}
	REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) == 12);
	CheckFields(*replacement, {{11, "added"}, {2, "s"}, {3, "s.x"}, {12, "s.added"}, {1, "id"}});
	REQUIRE_FALSE(replacement->TryGetColumnByFieldId(4));
	REQUIRE_FALSE(replacement->TryGetColumnByFieldId(5));
}

TEST_CASE("Replacement identifier fields follow their reassigned IDs", "[iceberg_schema]") {
	SchemaTest test;
	auto original =
	    test.Schema({{"id", LogicalType::INTEGER}, {"s", LogicalType::STRUCT({{"key", LogicalType::INTEGER}})}});
	original->columns[0]->required = true;
	original->identifier_field_ids = {1};
	auto metadata = test.Metadata(original, 10);
	auto replacement = test.Schema({{"s", LogicalType::STRUCT({{"key", LogicalType::INTEGER}})},
	                                {"id", LogicalType::BIGINT},
	                                {"added", LogicalType::INTEGER}});
	for (auto &column : replacement->columns) {
		column->required = true;
	}
	replacement->GetMutableFromPath({Identifier("s"), Identifier("key")}, nullptr)->required = true;
	replacement->identifier_field_ids = {2, 3, 4};
	REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) == 11);
	const vector<int32_t> expected_identifiers {3, 1, 11};
	REQUIRE(replacement->identifier_field_ids == expected_identifiers);
	CheckFields(*replacement, {{2, "s"}, {3, "s.key"}, {1, "id"}, {11, "added"}});
	for (auto field_id : expected_identifiers) {
		REQUIRE(replacement->GetColumnByFieldId(field_id).required);
	}
	auto reparsed = IcebergTableSchema::ParseSchema(replacement->ToRESTObject());
	REQUIRE(reparsed->identifier_field_ids == expected_identifiers);
	CheckFields(*reparsed, {{2, "s"}, {3, "s.key"}, {1, "id"}, {11, "added"}});
	REQUIRE(metadata.GetLatestSchema().identifier_field_ids == vector<int32_t> {1});
}

TEST_CASE("Replacement rejects ambiguous full field names", "[iceberg_schema]") {
	SchemaTest test;
	auto original = test.Schema({{"a", LogicalType::STRUCT({{"b", LogicalType::INTEGER}})}});
	auto replacement = test.Schema({{"a", LogicalType::STRUCT({{"b", LogicalType::INTEGER}})}});
	auto ambiguous =
	    test.Schema({{"a.b", LogicalType::INTEGER}, {"a", LogicalType::STRUCT({{"b", LogicalType::INTEGER}})}});
	SECTION("The current schema has a literal dotted name and a matching nested path") {
		original = ambiguous;
	}
	SECTION("The replacement has a literal dotted name and a matching nested path") {
		replacement = ambiguous;
	}
	auto metadata = test.Metadata(original, 10);
	REQUIRE_THROWS_WITH(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata),
	                    Catch::Contains("multiple fields for name a.b"));
}

TEST_CASE("Replacement field IDs require a valid table high-water mark", "[iceberg_schema]") {
	SchemaTest test;
	auto original = test.Schema({{"id", LogicalType::INTEGER}});
	auto metadata = test.Metadata(original, NumericLimits<int32_t>::Maximum());
	auto replacement = test.Schema({{"id", LogicalType::INTEGER}});
	SECTION("Matching fields do not consume new IDs") {
		REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) ==
		        NumericLimits<int32_t>::Maximum());
		CheckFields(*replacement, {{1, "id"}});
	}
	SECTION("The last available ID can be assigned alongside a matching field") {
		metadata.last_column_id = NumericLimits<int32_t>::Maximum() - 1;
		replacement = test.Schema({{"new", LogicalType::INTEGER}, {"id", LogicalType::INTEGER}});
		REQUIRE(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata) ==
		        NumericLimits<int32_t>::Maximum());
		CheckFields(*replacement, {{NumericLimits<int32_t>::Maximum(), "new"}, {1, "id"}});
	}
	SECTION("New IDs cannot overflow") {
		replacement = test.Schema({{"new", LogicalType::INTEGER}});
		REQUIRE_THROWS_AS(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata),
		                  InternalException);
	}
	SECTION("Missing last-column-id cannot restart numbering") {
		metadata.last_column_id = optional_idx();
		REQUIRE_THROWS_WITH(IcebergCreateTableRequest::AssignReplacementFieldIds(*replacement, metadata),
		                    Catch::Contains("missing 'last-column-id'"));
	}
}

int main(int argc, char **argv) {
	return Catch::Session().run(argc, argv);
}
