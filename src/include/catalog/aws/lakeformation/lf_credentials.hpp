#pragma once

#include "duckdb/main/client_context.hpp"

#ifndef EMSCRIPTEN
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#endif

namespace duckdb {

class IcebergCatalog;

struct LakeFormationClientConfig {
	string region;
#ifndef EMSCRIPTEN
	std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
	Aws::Client::ClientConfiguration aws_config;
#endif
};

LakeFormationClientConfig BuildLakeFormationClientConfig(ClientContext &context, IcebergCatalog &catalog);

} // namespace duckdb
