#include "common/iceberg_fixed_type.hpp"

#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/function/type_constructor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

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
	int32_t length = 0;
	for (idx_t i = 6; i < type.size() - 1; i++) {
		auto c = type[i];
		if (!StringUtil::CharacterIsDigit(c) || length > (NumericLimits<int32_t>::Maximum() - (c - '0')) / 10) {
			throw InvalidConfigurationException("Invalid fixed type length: %s", type);
		}
		length = length * 10 + (c - '0');
	}
	if (length == 0) {
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

struct FixedCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, VectorTryCastData &data) {
		auto length = IcebergFixedType::GetLength(data.result.GetType());
		if (input.GetSize() != NumericCast<idx_t>(length)) {
			return HandleVectorCastError::Operation<RESULT_TYPE>(
			    StringUtil::Format("Expected %d bytes for ICEBERG_FIXED(%d), got %d", length, length, input.GetSize()),
			    mask, idx, data);
		}
		return input;
	}
};

static bool BlobToFixedCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	StringVector::AddHeapReference(result, source);
	return VectorCastHelpers::TemplatedTryCastLoop<string_t, string_t, FixedCastOperator>(source, result, count,
	                                                                                      parameters);
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
	for (auto &source :
	     vector<LogicalType> {LogicalType::VARCHAR, LogicalType::UUID, LogicalType::BIT, LogicalType::VARIANT()}) {
		loader.RegisterCastFunction(source, type, BindToFixedCast);
	}
	loader.RegisterCastFunction(type, type, BoundCastInfo(BlobToFixedCast));
	loader.RegisterCastFunction(type, LogicalType::BLOB, BoundCastInfo(DefaultCasts::ReinterpretCast), 0);
}

} // namespace duckdb
