#include "catalog/rest/vendor/iceberg_s3tables_rest_backend.hpp"

#include "duckdb/common/array.hpp"

#include "catalog/iceberg_backend_registry.hpp"

namespace duckdb {

IcebergS3TablesRestBackend::IcebergS3TablesRestBackend(const Identifier &catalog_name)
    : IcebergRestBackend(catalog_name) {
}

IcebergBackendFactory IcebergS3TablesRestBackend::Factory() {
	IcebergBackendFactory factory;
	factory.catalog_type = "rest";
	factory.endpoint_type = "s3_tables";
	factory.create = [](IcebergAttachInput &input) -> unique_ptr<IcebergCatalogBackend> {
		auto backend = make_uniq<IcebergS3TablesRestBackend>(Identifier(input.name));
		backend->Attach(input);
		return std::move(backend);
	};
	return factory;
}

namespace {

struct ParsedARN {
public:
	ParsedARN(const string &arn) {
		idx_t section = 0;
		idx_t start = 0;
		//! NOTE: we can't use StringUtil::Split because it doesn't keep empty items
		for (idx_t i = 0; i < arn.size() && section < 5; i++) {
			if (arn[i] == ':') {
				sections[section++] = arn.substr(start, i - start);
				start = i + 1;
			}
		}
		if (section < 5 || sections[0] != "arn") {
			throw InvalidInputException("Expected an AWS ARN of the form "
			                            "'arn:<partition>:<service>:<region>:<account-id>[:<resource>]', got '%s'",
			                            arn);
		}
		sections[5] = arn.substr(start);
		auto &partition = Partition();
		if (partition.empty()) {
			throw InvalidInputException("Invalid PARTITION Section of ARN: '%s'", partition);
		}
		auto &service = Service();
		if (service.empty()) {
			throw InvalidInputException("Invalid SERVICE Section of ARN: '%s'", service);
		}
		auto &resource = Resource();
		if (resource.empty()) {
			throw InvalidInputException("Invalid RESOURCE Section of ARN: '%s'", resource);
		}
	}

public:
	const string &Partition() {
		return sections[1];
	}
	const string &Service() {
		return sections[2];
	}
	const string &Region() {
		return sections[3];
	}
	const string &AccountID() {
		return sections[4];
	}
	const string &Resource() {
		return sections[5];
	}

private:
	array<string, 6> sections;
};

static void S3TablesAttach(IcebergRestOptions &input) {
	ParsedARN arn(input.warehouse);

	// Populate sigv4_region so it can be used as a fallback region when creating storage secrets
	auto &region = arn.Region();
	if (region.empty()) {
		throw InvalidInputException("Can't ATTACH to S3Tables with an ARN(%s) that has an empty REGION section",
		                            input.warehouse);
	}
	input.options.emplace("sigv4_region", Value(region));
	IcebergRestBackend::UseAWSSigV4(input, "s3tables", region);
}

} // namespace

void IcebergS3TablesRestBackend::ConfigureEndpoint(IcebergAttachInput &input, IcebergRestOptions &options,
                                                   const case_insensitive_set_t &set_by_user) {
	S3TablesAttach(options);
	ApplyAWSPresets(input, options, set_by_user);
}

void IcebergS3TablesRestBackend::AddFallbackOptions(unordered_map<string, Value> &fallback_options) {
	AddAWSFallbackOptions(fallback_options);
}

void IcebergS3TablesRestBackend::AddDefaultEndpoints() {
	// insert namespaces based on REST API spec.
	// List namespaces
	supported_urls.insert("GET /v1/{prefix}/namespaces");
	// create namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces");
	// Load metadata for a Namespace
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}");
	// Drop a namespace
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}");
	// list all table identifiers
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables");
	// create table in the namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables");
	// get table from the catalog
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// commit updates to a table
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// drop table from a catalog
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// Rename a table from one identifier to another.
	supported_urls.insert("POST /v1/{prefix}/tables/rename");
	// table exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// namespace exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}");
}

} // namespace duckdb
