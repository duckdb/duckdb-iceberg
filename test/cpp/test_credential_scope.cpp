#include "catch.hpp"
#include "catalog/rest/storage/authorization/sigv4_utils.hpp"

using namespace duckdb;

// --- DetectStorageType ---

TEST_CASE("DetectStorageType - s3 schemes", "[credential_scope]") {
	REQUIRE(DetectStorageType("s3://my-bucket/warehouse") == "s3");
	REQUIRE(DetectStorageType("s3a://my-bucket/warehouse") == "s3");
}

TEST_CASE("DetectStorageType - gcs schemes", "[credential_scope]") {
	REQUIRE(DetectStorageType("gs://my-bucket/warehouse") == "gcs");
	REQUIRE(DetectStorageType("https://storage.googleapis.com/bucket") == "gcs");
}

TEST_CASE("DetectStorageType - azure schemes", "[credential_scope]") {
	REQUIRE(DetectStorageType("abfs://container@account.dfs.core.windows.net") == "azure");
	REQUIRE(DetectStorageType("abfss://container@account.dfs.core.windows.net") == "azure");
	REQUIRE(DetectStorageType("az://container/path") == "azure");
}

TEST_CASE("DetectStorageType - unknown defaults to s3", "[credential_scope]") {
	REQUIRE(DetectStorageType("oss://my-bucket/warehouse") == "s3");
	REQUIRE(DetectStorageType("https://example.com/data") == "s3");
}

// --- CredentialMatchesStorageType ---

TEST_CASE("CredentialMatchesStorageType - s3 credentials", "[credential_scope]") {
	REQUIRE(CredentialMatchesStorageType("s3://my-bucket/path", "s3"));
	REQUIRE(CredentialMatchesStorageType("s3a://my-bucket/path", "s3"));
	REQUIRE(CredentialMatchesStorageType("s3n://my-bucket/path", "s3"));
	REQUIRE_FALSE(CredentialMatchesStorageType("oss://my-bucket/path", "s3"));
}

TEST_CASE("CredentialMatchesStorageType - gcs credentials", "[credential_scope]") {
	REQUIRE(CredentialMatchesStorageType("gs://my-bucket/path", "gcs"));
	REQUIRE(CredentialMatchesStorageType("gcs://my-bucket/path", "gcs"));
	REQUIRE_FALSE(CredentialMatchesStorageType("s3://my-bucket/path", "gcs"));
}

TEST_CASE("CredentialMatchesStorageType - azure credentials", "[credential_scope]") {
	REQUIRE(CredentialMatchesStorageType("abfs://container", "azure"));
	REQUIRE(CredentialMatchesStorageType("abfss://container", "azure"));
	REQUIRE(CredentialMatchesStorageType("az://container/path", "azure"));
	REQUIRE_FALSE(CredentialMatchesStorageType("s3://bucket", "azure"));
}

TEST_CASE("CredentialMatchesStorageType - unknown type falls back to StartsWith", "[credential_scope]") {
	REQUIRE(CredentialMatchesStorageType("oss://my-bucket", "oss"));
	REQUIRE_FALSE(CredentialMatchesStorageType("s3://my-bucket", "oss"));
}

// --- Cross-scheme scope fallback logic ---

TEST_CASE("Scope fallback - table location matches credential prefix", "[credential_scope]") {
	string table_location = "s3://my-bucket/warehouse/table";
	string credential_prefix = "s3://my-bucket/";
	REQUIRE(StringUtil::StartsWith(table_location, credential_prefix));
}

TEST_CASE("Scope fallback - oss table location with s3 credential prefix", "[credential_scope]") {
	string table_location = "oss://my-bucket/warehouse/table";
	string credential_prefix = "s3://my-bucket/";
	REQUIRE_FALSE(StringUtil::StartsWith(table_location, credential_prefix));
}
