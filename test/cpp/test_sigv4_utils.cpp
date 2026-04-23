#include "catch.hpp"
#include "catalog/rest/storage/authorization/sigv4_utils.hpp"

using namespace duckdb;

// --- DecomposeHost ---

TEST_CASE("DecomposeHost - plain authority", "[sigv4_utils]") {
	auto result = DecomposeHost("s3.us-east-1.amazonaws.com");
	REQUIRE(result.authority == "s3.us-east-1.amazonaws.com");
	REQUIRE(result.path_components.empty());
}

TEST_CASE("DecomposeHost - authority with path", "[sigv4_utils]") {
	auto result = DecomposeHost("s3.us-east-1.amazonaws.com/iceberg/warehouse");
	REQUIRE(result.authority == "s3.us-east-1.amazonaws.com");
	REQUIRE(result.path_components.size() == 2);
	REQUIRE(result.path_components[0] == "iceberg");
	REQUIRE(result.path_components[1] == "warehouse");
}

TEST_CASE("DecomposeHost - trailing slash", "[sigv4_utils]") {
	auto result = DecomposeHost("example.com/");
	REQUIRE(result.authority == "example.com");
}

// --- IsAwsRegion ---

TEST_CASE("IsAwsRegion - valid regions", "[sigv4_utils]") {
	REQUIRE(IsAwsRegion("us-east-1"));
	REQUIRE(IsAwsRegion("eu-west-2"));
	REQUIRE(IsAwsRegion("ap-southeast-1"));
	REQUIRE(IsAwsRegion("sa-east-1"));
	REQUIRE(IsAwsRegion("ca-central-1"));
	REQUIRE(IsAwsRegion("me-south-1"));
	REQUIRE(IsAwsRegion("af-south-1"));
	REQUIRE(IsAwsRegion("il-central-1"));
	REQUIRE(IsAwsRegion("mx-central-1"));
}

TEST_CASE("IsAwsRegion - invalid tokens", "[sigv4_utils]") {
	REQUIRE_FALSE(IsAwsRegion("s3"));
	REQUIRE_FALSE(IsAwsRegion("amazonaws"));
	REQUIRE_FALSE(IsAwsRegion("com"));
	REQUIRE_FALSE(IsAwsRegion("us-east-"));
	REQUIRE_FALSE(IsAwsRegion(""));
	REQUIRE_FALSE(IsAwsRegion("xx-east-1"));
}

// --- GetAwsRegion ---

TEST_CASE("GetAwsRegion - standard S3 host", "[sigv4_utils]") {
	REQUIRE(GetAwsRegion("s3.us-east-1.amazonaws.com") == "us-east-1");
}

TEST_CASE("GetAwsRegion - S3 Tables host", "[sigv4_utils]") {
	REQUIRE(GetAwsRegion("s3tables.eu-west-2.amazonaws.com") == "eu-west-2");
}

TEST_CASE("GetAwsRegion - Glue host", "[sigv4_utils]") {
	REQUIRE(GetAwsRegion("glue.ap-southeast-1.amazonaws.com") == "ap-southeast-1");
}

// --- GetAwsService ---

TEST_CASE("GetAwsService - S3 host", "[sigv4_utils]") {
	REQUIRE(GetAwsService("s3.us-east-1.amazonaws.com") == "s3");
}

TEST_CASE("GetAwsService - S3 Tables host", "[sigv4_utils]") {
	REQUIRE(GetAwsService("s3tables.eu-west-2.amazonaws.com") == "s3tables");
}

TEST_CASE("GetAwsService - Glue host", "[sigv4_utils]") {
	REQUIRE(GetAwsService("glue.ap-southeast-1.amazonaws.com") == "glue");
}

