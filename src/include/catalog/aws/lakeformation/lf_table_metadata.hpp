#pragma once

#include "catalog/aws/lakeformation/lf_types.hpp"

#include "core/metadata/schema/iceberg_table_schema.hpp"

#ifndef EMSCRIPTEN
#include <aws/glue/model/GetUnfilteredTableMetadataResult.h>
#include <aws/glue/model/UnfilteredPartition.h>
#endif

namespace duckdb {

#ifndef EMSCRIPTEN
LakeFormationTablePolicy
ParseUnfilteredTableMetadata(const ::Aws::Glue::Model::GetUnfilteredTableMetadataResult &result);

LakeFormationPartitionPolicy
ParseUnfilteredPartitionMetadata(const ::Aws::Glue::Model::UnfilteredPartition &unfiltered);
#endif

void ValidateLakeFormationPolicyV1(const LakeFormationTablePolicy &policy, const IcebergTableSchema &schema);

} // namespace duckdb
