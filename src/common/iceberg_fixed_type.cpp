#include "common/iceberg_fixed_type.hpp"
#include "common/iceberg_utils.hpp"

#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/function/type_constructor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/planner/operator/logical_alter.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

static constexpr const char *FIXED_TYPE_NAME = "ICEBERG_FIXED";

LogicalType IcebergFixedType::Get(int32_t length) {
	if (length <= 0) {
		throw InvalidInputException("ICEBERG_FIXED length must be a positive integer");
	}
	auto info = make_uniq<ExtensionTypeInfo>();
	info->modifiers.emplace_back(Value::INTEGER(length));
	return LogicalType(LogicalTypeId::BLOB).WithAlias(FIXED_TYPE_NAME).WithExtensionInfo(std::move(info));
}

LogicalType IcebergFixedType::Parse(const string &type) {
	if (!StringUtil::StartsWith(type, "fixed[") || type.size() < 8 || type.back() != ']') {
		throw InvalidConfigurationException("Invalid fixed type format: %s", type);
	}
	auto length_str = type.substr(6, type.size() - 7);
	int32_t length;
	if (!TryCast::Operation<string_t, int32_t>(string_t(length_str), length, true)) {
		throw InvalidConfigurationException("Invalid fixed type length: %s", type);
	}
	return Get(length);
}

bool IcebergFixedType::IsFixed(const LogicalType &type) {
	return type.id() == LogicalTypeId::BLOB && type.GetAlias() == FIXED_TYPE_NAME;
}

int32_t IcebergFixedType::GetLength(const LogicalType &type) {
	D_ASSERT(IsFixed(type));
	auto info = type.GetExtensionInfo();
	if (!info || info->modifiers.size() != 1 || info->modifiers[0].value.IsNull()) {
		throw InvalidInputException("ICEBERG_FIXED requires a length");
	}
	return info->modifiers[0].value.GetValue<int32_t>();
}

static LogicalType BindFixedType(BindLogicalTypeInput &input) {
	auto &length = input.modifiers[0].GetValue();
	if (length.IsNull()) {
		throw InvalidInputException("ICEBERG_FIXED length must be a positive integer");
	}
	return IcebergFixedType::Get(length.GetValue<int32_t>());
}

struct FixedCastData {
	FixedCastData(Vector &result, CastParameters &parameters, int32_t length_p)
	    : vector_cast_data(result, parameters), length(length_p) {
	}

	VectorTryCastData vector_cast_data;
	int32_t length;
};

struct FixedCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, FixedCastData &data) {
		if (input.GetSize() != NumericCast<idx_t>(data.length)) {
			return HandleVectorCastError::Operation<RESULT_TYPE>(
			    StringUtil::Format("Expected %d bytes for ICEBERG_FIXED(%d), got %d", data.length, data.length,
			                       input.GetSize()),
			    mask, idx, data.vector_cast_data);
		}
		return input;
	}
};

static bool BlobToFixedCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	StringVector::AddHeapReference(result, source);
	FixedCastData data(result, parameters, IcebergFixedType::GetLength(result.GetType()));
	UnaryExecutor::GenericExecute<string_t, string_t, FixedCastOperator>(source, result, count, data,
	                                                                     parameters.error_message);
	return data.vector_cast_data.all_converted;
}

struct ToFixedCastData : public BoundCastData {
	explicit ToFixedCastData(BoundCastInfo blob_cast_p) : blob_cast(std::move(blob_cast_p)) {
	}

	BoundCastInfo blob_cast;

	unique_ptr<BoundCastData> Copy() const override {
		return make_uniq<ToFixedCastData>(blob_cast.Copy());
	}
};

static unique_ptr<FunctionLocalState> InitToFixedCast(CastLocalStateParameters &parameters) {
	auto &cast = parameters.cast_data->Cast<ToFixedCastData>().blob_cast;
	if (!cast.HasInitLocalState()) {
		return nullptr;
	}
	CastLocalStateParameters child_parameters(parameters, cast.GetCastData());
	return cast.InitLocalState(child_parameters);
}

static bool ToFixedCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast = parameters.cast_data->Cast<ToFixedCastData>().blob_cast;
	CastParameters child_parameters(parameters, cast.GetCastData(), parameters.local_state);
	Vector blob(LogicalType::BLOB, count);
	auto converted = cast.Cast(source, blob, count, child_parameters);
	auto valid_length = BlobToFixedCast(blob, result, count, parameters);
	return converted && valid_length;
}

static BoundCastInfo BindToFixedCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	auto blob_cast = input.GetCastFunction(source, LogicalType::BLOB);
	return BoundCastInfo(ToFixedCast, make_uniq<ToFixedCastData>(std::move(blob_cast)), InitToFixedCast);
}

void IcebergFixedType::Register(ExtensionLoader &loader) {
	auto signature = TypeConstructor::Signature();
	signature.AddParameter("length", LogicalType::INTEGER);
	TypeConstructorSet constructors;
	constructors.AddFunction(TypeConstructor(std::move(signature), BindFixedType));
	auto type = LogicalType(LogicalTypeId::BLOB).WithAlias(FIXED_TYPE_NAME);
	loader.RegisterType(FIXED_TYPE_NAME, type, std::move(constructors));
	loader.RegisterCastFunction(LogicalType::BLOB, type, BoundCastInfo(BlobToFixedCast));
	for (auto &source : vector<LogicalType> {LogicalType::VARCHAR, LogicalType::JSON(), LogicalType::UUID,
	                                         LogicalType::BIT, LogicalType::VARIANT()}) {
		loader.RegisterCastFunction(source, type, BindToFixedCast);
	}
	loader.RegisterCastFunction(type, type, BoundCastInfo(BlobToFixedCast));
	loader.RegisterCastFunction(type, LogicalType::BLOB, BoundCastInfo(DefaultCasts::ReinterpretCast), 0);
}

static bool ContainsFixedType(const LogicalType &type) {
	return TypeVisitor::Contains(type, [](const LogicalType &child) { return IcebergFixedType::IsFixed(child); });
}

static void ThrowFixedTypeOutsideIceberg(const Identifier &name) {
	throw NotImplementedException("Cannot store \"%s\": ICEBERG_FIXED can only be used in Iceberg tables. Cast it to "
	                              "BLOB to store it in a DuckDB table.",
	                              name);
}

//! ICEBERG_FIXED is only for Iceberg tables, so reject it in any other catalog.
void IcebergFixedType::RejectOutsideIceberg(ClientContext &context, LogicalOperator &op) {
	// EXPLAIN ANALYZE executes the DDL in its child plan.
	for (auto &child : op.children) {
		RejectOutsideIceberg(context, *child);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CREATE_TABLE: {
		auto &create = op.Cast<LogicalCreateTable>();
		if (IcebergUtils::IsIcebergCatalog(create.schema.ParentCatalog())) {
			return;
		}
		for (auto &column : create.info->Base().columns.Logical()) {
			if (ContainsFixedType(column.Type())) {
				ThrowFixedTypeOutsideIceberg(column.Name());
			}
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_CREATE_TYPE: {
		auto &create = op.Cast<LogicalCreate>();
		if (create.schema && IcebergUtils::IsIcebergCatalog(create.schema->ParentCatalog())) {
			return;
		}
		auto &info = create.info->Cast<CreateTypeInfo>();
		if (ContainsFixedType(info.type)) {
			ThrowFixedTypeOutsideIceberg(info.GetQualifiedName().Name());
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_ALTER: {
		auto &alter = op.Cast<LogicalAlter>();
		auto &alter_info = *alter.info;
		if (alter_info.type != AlterType::ALTER_TABLE) {
			return;
		}
		auto catalog = Catalog::GetCatalogEntry(context, alter_info.GetQualifiedName().Catalog());
		if (!catalog || IcebergUtils::IsIcebergCatalog(*catalog)) {
			return;
		}
		auto &table_info = alter_info.Cast<AlterTableInfo>();
		switch (table_info.alter_table_type) {
		case AlterTableType::ADD_COLUMN: {
			auto &column = table_info.Cast<AddColumnInfo>().new_column;
			if (ContainsFixedType(column.Type())) {
				ThrowFixedTypeOutsideIceberg(column.Name());
			}
			break;
		}
		case AlterTableType::ADD_FIELD: {
			auto &field = table_info.Cast<AddFieldInfo>().new_field;
			if (ContainsFixedType(field.Type())) {
				ThrowFixedTypeOutsideIceberg(field.Name());
			}
			break;
		}
		case AlterTableType::ALTER_COLUMN_TYPE: {
			auto &change = table_info.Cast<ChangeColumnTypeInfo>();
			if (ContainsFixedType(change.target_type) && !change.column_path.empty()) {
				ThrowFixedTypeOutsideIceberg(change.column_path.back());
			}
			break;
		}
		default:
			break;
		}
		break;
	}
	default:
		break;
	}
}

} // namespace duckdb
