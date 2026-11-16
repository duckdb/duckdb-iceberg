#include "catch.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/common/json_document.hpp"
#include "rest_catalog/objects/catalog_object_labels.hpp"
#include "rest_catalog/objects/create_namespace_request.hpp"
#include "rest_catalog/objects/multi_valued_map.hpp"
#include "rest_catalog/objects/snapshot.hpp"
#include "rest_catalog/objects/snapshot_references.hpp"

using namespace duckdb;

namespace {

template <class T>
T ParseObject(const string &json) {
	auto document = JSONDocument::Parse(json.c_str(), json.size());
	return T::FromJSON(document->GetRoot());
}

template <class T, class CHECK>
void CheckRoundTrips(const string &json, CHECK check) {
	auto parsed = ParseObject<T>(json);
	check(parsed);
	auto copy = parsed.Copy();
	check(copy);
	JSONWriter writer;
	writer.SetRoot(copy.ToJSON(writer));
	auto reparsed = ParseObject<T>(writer.ToString());
	check(reparsed);
}

using SnapshotSummary = decltype(rest_api_objects::Snapshot().summary);

const char *TABLE_METADATA = R"({
    "format-version": 2,
    "table-uuid": "00000000-0000-0000-0000-000000000001",
    "location": "/unused",
    "last-sequence-number": 1,
    "last-updated-ms": 10,
    "last-column-id": 1,
    "schemas": [{"type": "struct", "schema-id": 0,
                 "fields": [{"id": 1, "name": "id", "required": false, "type": "int"}]}],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 1000,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
    "properties": {"Custom": "first", "custom": "second"},
    "current-snapshot-id": 7,
    "snapshots": [{
        "snapshot-id": 7, "sequence-number": 1, "timestamp-ms": 10,
        "manifest-list": "/unused/manifest.avro", "schema-id": 0,
        "summary": {"operation": "append", "Operation": "custom operation",
                    "Custom": "first", "custom": "second",
                    "total-records": "1", "TOTAL-RECORDS": "99", "added-records": "001"}
    }],
    "refs": {"main": {"snapshot-id": 7, "type": "branch"},
             "Dev": {"snapshot-id": 7, "type": "branch"},
             "dev": {"snapshot-id": 7, "type": "tag", "max-ref-age-ms": 1234}}
})";

} // namespace

TEST_CASE("JSON additional properties preserve case through parsing copying and serialization", "[iceberg_json]") {
	CheckRoundTrips<rest_api_objects::CatalogObjectLabels>(
	    R"({"Custom":"first","custom":"second"})", [](const auto &obj) {
		    REQUIRE(obj.additional_properties.size() == 2);
		    REQUIRE(obj.additional_properties.at("Custom") == "first");
		    REQUIRE(obj.additional_properties.at("custom") == "second");
	    });
}

TEST_CASE("JSON inline property maps preserve case through parsing copying and serialization", "[iceberg_json]") {
	CheckRoundTrips<rest_api_objects::CreateNamespaceRequest>(
	    R"({"namespace":["example"],"properties":{"Custom":"first","custom":"second"}})", [](const auto &obj) {
		    REQUIRE(obj.properties);
		    REQUIRE(obj.properties->size() == 2);
		    REQUIRE(obj.properties->at("Custom") == "first");
		    REQUIRE(obj.properties->at("custom") == "second");
	    });
}

TEST_CASE("JSON declared members exclude only their exact names from additional properties", "[iceberg_json]") {
	CheckRoundTrips<SnapshotSummary>(
	    R"({"operation":"append","Operation":"custom operation","Custom":"first","custom":"second"})",
	    [](const auto &obj) {
		    REQUIRE(obj.operation == "append");
		    REQUIRE(obj.additional_properties.size() == 3);
		    REQUIRE(obj.additional_properties.at("Operation") == "custom operation");
		    REQUIRE(obj.additional_properties.at("Custom") == "first");
		    REQUIRE(obj.additional_properties.at("custom") == "second");
		    REQUIRE(obj.additional_properties.count("operation") == 0);
	    });
}

TEST_CASE("JSON declared member casing does not bypass required members or value validation", "[iceberg_json]") {
	REQUIRE_THROWS_WITH(ParseObject<SnapshotSummary>(R"({"Operation":"append"})"),
	                    Catch::Contains("required property 'operation' is missing"));
	REQUIRE_THROWS_WITH(ParseObject<SnapshotSummary>(R"({"operation":"append","Operation":42})"),
	                    Catch::Contains("not of type 'string'"));
	REQUIRE_THROWS_WITH(ParseObject<rest_api_objects::CreateNamespaceRequest>(
	                        R"({"namespace":["example"],"properties":{"Custom":"first","custom":42}})"),
	                    Catch::Contains("not of type 'string'"));
}

TEST_CASE("JSON reference maps preserve case distinct ref names and their values", "[iceberg_json]") {
	CheckRoundTrips<rest_api_objects::SnapshotReferences>(
	    R"({"Dev":{"snapshot-id":1,"type":"branch"},"dev":{"snapshot-id":2,"type":"tag","max-ref-age-ms":1234}})",
	    [](const auto &obj) {
		    REQUIRE(obj.additional_properties.size() == 2);
		    REQUIRE(obj.additional_properties.at("Dev").snapshot_id == 1);
		    REQUIRE(obj.additional_properties.at("Dev").type == "branch");
		    REQUIRE(obj.additional_properties.at("dev").snapshot_id == 2);
		    REQUIRE(obj.additional_properties.at("dev").type == "tag");
		    REQUIRE(obj.additional_properties.at("dev").max_ref_age_ms == 1234);
	    });
}

TEST_CASE("JSON multi valued maps preserve case without assuming HTTP header semantics", "[iceberg_json]") {
	CheckRoundTrips<rest_api_objects::MultiValuedMap>(
	    R"({"Custom":["First","Second"],"custom":["Third"]})", [](const auto &obj) {
		    REQUIRE(obj.additional_properties.size() == 2);
		    REQUIRE(obj.additional_properties.at("Custom").size() == 2);
		    REQUIRE(obj.additional_properties.at("Custom")[0] == "First");
		    REQUIRE(obj.additional_properties.at("Custom")[1] == "Second");
		    REQUIRE(obj.additional_properties.at("custom").size() == 1);
		    REQUIRE(obj.additional_properties.at("custom")[0] == "Third");
	    });
}

TEST_CASE("Retained metadata copies and serialization preserve raw property and summary keys", "[iceberg_json]") {
	auto source = ParseObject<rest_api_objects::TableMetadata>(TABLE_METADATA);
	auto metadata = IcebergTableMetadata::FromTableMetadata(source);
	auto copy = metadata.Copy();
	const auto &source_summary = source.snapshots->at(0).summary.additional_properties;
	for (auto &retained : {&metadata, &copy}) {
		REQUIRE(retained->GetTableProperties().size() == 2);
		REQUIRE(retained->GetTableProperties().at("Custom") == "first");
		REQUIRE(retained->GetTableProperties().at("custom") == "second");
		auto snapshot = retained->GetLatestSnapshot();
		REQUIRE(snapshot);
		REQUIRE(snapshot->summary == source_summary);
		REQUIRE(snapshot->ToRESTObject().summary.additional_properties == source_summary);
		REQUIRE(snapshot->metrics.GetMetrics().at(IcebergSnapshotMetricType::TOTAL_RECORDS) == 1);

		auto serialized = ParseObject<rest_api_objects::TableMetadata>(retained->ToJSON());
		REQUIRE(*serialized.properties == *source.properties);
		REQUIRE(serialized.snapshots->at(0).summary.additional_properties == source_summary);
		REQUIRE(serialized.refs->additional_properties.size() == 3);
		REQUIRE(serialized.refs->additional_properties.at("Dev").type == "branch");
		REQUIRE(serialized.refs->additional_properties.at("dev").type == "tag");
		REQUIRE(serialized.refs->additional_properties.at("dev").max_ref_age_ms == 1234);
	}
	copy.table_properties["Custom"] = "updated";
	REQUIRE(metadata.table_properties.at("Custom") == "first");
	REQUIRE(copy.table_properties.at("custom") == "second");
}

TEST_CASE("New snapshots inherit metric totals without inheriting custom summary keys", "[iceberg_json]") {
	auto source = ParseObject<rest_api_objects::TableMetadata>(TABLE_METADATA);
	auto metadata = IcebergTableMetadata::FromTableMetadata(source);
	IcebergSnapshot snapshot(0, 8);
	snapshot.sequence_number = 2;
	snapshot.timestamp_ms = timestamp_ms_t(20);
	snapshot.manifest_list = "/unused/new-manifest.avro";
	snapshot.operation = IcebergSnapshotOperationType::APPEND;
	snapshot.metrics = IcebergSnapshotMetrics(*metadata.GetLatestSnapshot());
	auto result = snapshot.ToRESTObject();
	REQUIRE(result.summary.operation == "append");
	REQUIRE(result.summary.additional_properties.at("total-records") == "1");
	REQUIRE(result.summary.additional_properties.count("TOTAL-RECORDS") == 0);
	REQUIRE(result.summary.additional_properties.count("Custom") == 0);
	REQUIRE(result.summary.additional_properties.count("custom") == 0);
	REQUIRE(result.summary.additional_properties.count("Operation") == 0);
}

TEST_CASE("Snapshot metric interpretation has deterministic case precedence", "[iceberg_json]") {
	unordered_map<string, string> summary {{"TOTAL-RECORDS", "99"}, {"total-records", "1"}};
	REQUIRE(IcebergSnapshotMetrics(summary).GetMetrics().at(IcebergSnapshotMetricType::TOTAL_RECORDS) == 1);
	summary.erase("total-records");
	REQUIRE(IcebergSnapshotMetrics(summary).GetMetrics().at(IcebergSnapshotMetricType::TOTAL_RECORDS) == 99);
	summary.emplace("Total-Records", "42");
	REQUIRE_THROWS_WITH(IcebergSnapshotMetrics(summary), Catch::Contains("Ambiguous Iceberg property 'total-records'"));
	summary.emplace("total-records", "invalid");
	REQUIRE(IcebergSnapshotMetrics(summary).GetMetrics().count(IcebergSnapshotMetricType::TOTAL_RECORDS) == 0);
	REQUIRE(summary.size() == 3);
}
