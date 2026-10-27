#include "function/copy/iceberg_copy_function.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"

#include "execution/operator/copy/iceberg_copy.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"

namespace duckdb {

static BoundStatement IcebergCopyPlan(Binder &binder, CopyStatement &stmt) {
	auto &copy_info = *stmt.info;
	// bind the select statement
	auto node_copy = copy_info.select_statement->Copy();
	auto child_statement = binder.Bind(*node_copy);

	// Create bind data with metadata and schema
	auto bind_data = make_uniq<CopyIcebergBindData>(copy_info, IdentifiersToStrings(child_statement.names),
	                                                std::move(child_statement.types), binder.context);

	// Create logical copy operator
	auto logical_copy = make_uniq<IcebergLogicalCopy>();
	logical_copy->bind_data = std::move(bind_data);
	logical_copy->children.push_back(std::move(child_statement.plan));

	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};
	result.plan = std::move(logical_copy);
	return result;
}

//! Before v4 the spec requires every path in the metadata to be fully qualified, with a URI scheme. Paths that have
//! a scheme (s3://, gs://, ...) are kept as they are. Local paths, which can be relative to the working directory or
//! start with '~', become absolute file:// URIs, e.g. file:///tmp/table or file:///C:/tmp/table.
static string QualifyPath(FileSystem &fs, const string &raw_path) {
	if (Path::FromString(raw_path).IsRemote()) {
		auto result = raw_path;
		StringUtil::RTrim(result, "/");
		return result;
	}
	// ExpandPath resolves '~' and strips a file: scheme the path may already have
	auto path = Path::FromString(fs.ExpandPath(raw_path));
	if (!path.IsAbsolute()) {
		path = Path::FromString(FileSystem::GetWorkingDirectory()).Join(path);
	}
	// anchor and segments without a trailing separator: "/a/b", or "C:\a\b" on Windows
	auto local_path = path.GetAnchor() + path.GetPath();
	std::replace(local_path.begin(), local_path.end(), '\\', '/');
	if (!StringUtil::StartsWith(local_path, "/")) {
		local_path = "/" + local_path;
	}
	// Windows UNC paths keep their server and share as the URI authority
	auto authority = path.GetAuthority();
	std::replace(authority.begin(), authority.end(), '\\', '/');
	return "file://" + authority + local_path;
}

static Value CastCopyOption(const string &name, const vector<Value> &values, const LogicalType &type) {
	if (values.size() != 1) {
		throw BinderException("COPY option \"%s\" expects a single value", name);
	}
	auto casted = values[0].DefaultTryCastAs(type, nullptr, true);
	if (!casted || casted->IsNull()) {
		throw InvalidInputException("Can't cast COPY option \"%s\" (%s) to %s", name, values[0].ToString(),
		                            type.ToString());
	}
	return std::move(*casted);
}

//! The "format-version" option, or 2 if it is not given
static int32_t GetFormatVersion(const CopyInfo &info) {
	int64_t format_version = 2;
	for (auto &option : info.options) {
		auto name = option.first.GetIdentifierName();
		if (StringUtil::CIEquals(name, "format-version")) {
			format_version = CastCopyOption(name, option.second, LogicalType::BIGINT).GetValue<int64_t>();
		}
	}
	if (format_version == 1) {
		throw NotImplementedException("Writing Iceberg tables with format-version 1 is not supported, use 2 or 3");
	}
	if (format_version != 2 && format_version != 3) {
		throw InvalidInputException("\"format-version\" must be 2 or 3, got %d", format_version);
	}
	return NumericCast<int32_t>(format_version);
}

CopyIcebergBindData::CopyIcebergBindData(const vector<string> &names, const vector<LogicalType> &types,
                                         const string &file_path, unique_ptr<IcebergTableMetadata> table_metadata,
                                         unique_ptr<IcebergTableSchema> table_schema)
    : names(names), types(types), file_path(file_path), table_metadata(std::move(table_metadata)),
      table_schema(std::move(table_schema)) {
}

CopyIcebergBindData::CopyIcebergBindData(const CopyInfo &info, vector<string> &&names_p, vector<LogicalType> &&types_p,
                                         ClientContext &context)
    : names(std::move(names_p)), types(std::move(types_p)) {
	file_path = info.file_path;
	auto &fs = FileSystem::GetFileSystem(context);

	// Create IcebergTableMetadata
	table_metadata = make_uniq<IcebergTableMetadata>(IcebergTableMetadataSchemas {});
	table_metadata->table_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	table_metadata->location = QualifyPath(fs, file_path);
	table_metadata->iceberg_version = GetFormatVersion(info);
	table_metadata->SetCurrentSchemaId(0);

	// Create ColumnList from query output
	ColumnList columns;
	for (idx_t i = 0; i < names.size(); i++) {
		columns.AddColumn(ColumnDefinition(Identifier(names[i]), types[i]));
	}

	int32_t last_column_id;
	table_schema =
	    IcebergCreateTableRequest::CreateIcebergSchema(context, *table_metadata, columns, nullptr, last_column_id);
	table_schema->schema_id = 0;
	auto &result_schema = table_metadata->GetSchemasMutable().AddSchemaOrGetExisting(table_schema);
	if (result_schema.schema_id != 0) {
		throw InternalException("Iceberg COPY created non-0 schema id (%d)", result_schema.schema_id);
	}
	table_metadata->SetCurrentSchemaId(0);
	//! FIXME: adapt when we have partitioning support
	table_metadata->partition_specs.emplace(0, IcebergPartitionSpec(0));
	table_metadata->default_spec_id = 0;
	table_metadata->sort_specs.emplace(0, IcebergSortOrder(0));
	table_metadata->last_column_id = last_column_id;
	table_metadata->last_partition_field_id = 0;
	table_metadata->default_sort_order_id = 0;

	// Thread COPY options (e.g. "write.target-file-size-bytes", "write.parquet.row-group-size") into the
	// table properties so IcebergInsert::GetCopyOptions applies them to the parquet writer. The parser has
	// already extracted FORMAT into info.format, so everything left here is an Iceberg write property, except
	// for "format-version".
	for (auto &option : info.options) {
		auto name = option.first.GetIdentifierName();
		if (StringUtil::CIEquals(name, "format-version")) {
			// Reserved: it selects the version of the new table (GetFormatVersion) and is not a table property
			continue;
		}
		if (option.second.empty()) {
			continue;
		}
		auto value = option.second[0].ToString();
		if (StringUtil::CIEquals(name, "write.data.path") || StringUtil::CIEquals(name, "write.metadata.path")) {
			// file paths in the metadata are built from these, so they have to be fully qualified too
			value = QualifyPath(fs, value);
		}
		table_metadata->table_properties[name] = value;
	}
	table_metadata->table_properties.emplace("write.parquet.shred-variants", "true");
}

unique_ptr<FunctionData> CopyIcebergBindData::Copy() const {
	throw NotImplementedException("Can't copy CopyIcebergBindData!");
	// return make_uniq<CopyIcebergBindData>(names, types, file_path, table_metadata->Copy(), table_schema->Copy());
}

bool CopyIcebergBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<CopyIcebergBindData>();
	if (names.size() != other.names.size()) {
		return false;
	}
	if (types.size() != other.types.size()) {
		return false;
	}
	D_ASSERT(types.size() == names.size());
	for (idx_t i = 0; i < types.size(); i++) {
		if (types[i] != other.types[i]) {
			return false;
		}
		if (names[i] != other.names[i]) {
			return false;
		}
	}

	//! TODO: compare table metadata and table schema ???
	return true;
}

CopyFunction IcebergCopyFunction::Create() {
	auto res = CopyFunction("iceberg");
	res.plan = IcebergCopyPlan;
	return res;
}

} // namespace duckdb
