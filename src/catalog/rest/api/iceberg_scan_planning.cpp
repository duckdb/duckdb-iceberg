#include "catalog/rest/api/iceberg_scan_planning.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context.hpp"
#include "yyjson.hpp"

#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "core/expression/iceberg_value.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "rest_catalog/objects/data_file.hpp"
#include "rest_catalog/objects/equality_delete_file.hpp"
#include "rest_catalog/objects/load_credentials_response.hpp"
#include "rest_catalog/objects/position_delete_file.hpp"

#include <chrono>
#include <thread>

using namespace duckdb_yyjson;

namespace duckdb {
namespace {

struct PlannedContentFile {
	IcebergDataFile file;
	int32_t spec_id;
};

struct PlannedFileTask {
	PlannedContentFile data_file;
	vector<idx_t> delete_file_references;
};

struct PlanningAccumulator {
	vector<PlannedContentFile> delete_files;
	vector<PlannedFileTask> file_tasks;
	vector<string> plan_tasks;
};

static IRCEndpointBuilder TableEndpoint(IcebergTableInformation &table_info) {
	auto &catalog = table_info.catalog;
	auto result = catalog.GetBaseUrl();
	result.AddPrefixComponent(catalog.prefix, catalog.prefix_is_one_component);
	result.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	result.AddPathComponent(IRCPathComponent::NamespaceComponent(table_info.schema.namespace_items));
	result.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	result.AddPathComponent(IRCPathComponent::RegularComponent(table_info.name));
	return result;
}

static HTTPHeaders PlanningHeaders(ClientContext &context) {
	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	return headers;
}

static void ThrowResponseError(const IRCEndpointBuilder &endpoint, const HTTPResponse &response) {
	throw HTTPException(response, "Iceberg REST scan-planning request to '%s' failed (HTTP %n). Reason: %s, body: %s",
	                    endpoint.GetURLEncoded(), int(response.status), response.reason, response.body);
}

static string GetRequiredString(yyjson_val *obj, const char *key) {
	auto value = yyjson_obj_get(obj, key);
	if (!value || !yyjson_is_str(value)) {
		throw InvalidInputException("Invalid Iceberg REST scan-planning response: '%s' must be a string", key);
	}
	return yyjson_get_str(value);
}

static string AddEscapesToBlob(const string &hexadecimal_string) {
	if (hexadecimal_string.size() % 2 != 0) {
		throw InvalidInputException("Invalid odd-length hexadecimal Iceberg REST primitive value");
	}
	string result;
	result.reserve(hexadecimal_string.size() * 2);
	for (idx_t i = 0; i < hexadecimal_string.size(); i += 2) {
		result += "\\x";
		result += hexadecimal_string.substr(i, 2);
	}
	return result;
}

static Value PrimitiveValue(const rest_api_objects::PrimitiveTypeValue &value, const LogicalType &type) {
	if (value.is_null) {
		return Value(type);
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		if (value.boolean_type_value) {
			return Value::BOOLEAN(value.boolean_type_value->value);
		}
		break;
	case LogicalTypeId::INTEGER:
		if (value.integer_type_value) {
			return Value::INTEGER(value.integer_type_value->value);
		}
		break;
	case LogicalTypeId::BIGINT:
		if (value.long_type_value) {
			return Value::BIGINT(value.long_type_value->value);
		}
		break;
	case LogicalTypeId::FLOAT:
		if (value.float_type_value) {
			return Value::FLOAT(static_cast<float>(value.float_type_value->value));
		}
		break;
	case LogicalTypeId::DOUBLE:
		if (value.double_type_value) {
			return Value::DOUBLE(value.double_type_value->value);
		}
		break;
	case LogicalTypeId::DECIMAL:
		if (value.decimal_type_value) {
			return Value(value.decimal_type_value->value).DefaultCastAs(type);
		}
		break;
	case LogicalTypeId::VARCHAR:
		if (value.string_type_value) {
			return Value(value.string_type_value->value);
		}
		break;
	case LogicalTypeId::UUID:
		if (value.uuidtype_value) {
			return Value(value.uuidtype_value->value).DefaultCastAs(type);
		}
		break;
	case LogicalTypeId::DATE:
		if (value.date_type_value) {
			return Value(value.date_type_value->value).DefaultCastAs(type);
		}
		break;
	case LogicalTypeId::TIME:
		if (value.time_type_value) {
			return Value(value.time_type_value->value).DefaultCastAs(type);
		}
		break;
	case LogicalTypeId::TIMESTAMP:
		if (value.timestamp_type_value) {
			return Value(value.timestamp_type_value->value).DefaultCastAs(type);
		}
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
		if (value.timestamp_tz_type_value) {
			return Value(value.timestamp_tz_type_value->value).DefaultCastAs(type);
		}
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		if (value.timestamp_nano_type_value) {
			return Value(value.timestamp_nano_type_value->value).DefaultCastAs(type);
		}
		break;
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		if (value.timestamp_tz_nano_type_value) {
			return Value(value.timestamp_tz_nano_type_value->value).DefaultCastAs(type);
		}
		break;
	case LogicalTypeId::BLOB:
		if (value.binary_type_value) {
			return Value::BLOB(AddEscapesToBlob(value.binary_type_value->value));
		}
		if (value.fixed_type_value) {
			return Value::BLOB(AddEscapesToBlob(value.fixed_type_value->value));
		}
		break;
	default:
		break;
	}
	throw InvalidInputException("Iceberg REST scan-planning returned a primitive value incompatible with type %s",
	                            type.ToString());
}

static void CopyCountMap(const optional<rest_api_objects::CountMap> &source, unordered_map<int32_t, int64_t> &target) {
	if (!source || !source->keys || !source->values) {
		return;
	}
	if (source->keys->size() != source->values->size()) {
		throw InvalidInputException("Iceberg REST scan-planning returned a malformed CountMap");
	}
	for (idx_t i = 0; i < source->keys->size(); i++) {
		target[(*source->keys)[i].value] = (*source->values)[i].value;
	}
}

static void CopyValueMap(const optional<rest_api_objects::ValueMap> &source, const IcebergTableMetadata &metadata,
                         unordered_map<int32_t, Value> &target, SerializeBound bound) {
	if (!source || !source->keys || !source->values) {
		return;
	}
	if (source->keys->size() != source->values->size()) {
		throw InvalidInputException("Iceberg REST scan-planning returned a malformed ValueMap");
	}
	for (idx_t i = 0; i < source->keys->size(); i++) {
		auto field_id = (*source->keys)[i].value;
		auto column = metadata.FindColumnByFieldId(field_id);
		if (!column) {
			continue;
		}
		auto value = PrimitiveValue((*source->values)[i], column->type);
		if (value.IsNull()) {
			continue;
		}
		if (column->type.id() == LogicalTypeId::TIME || column->type.id() == LogicalTypeId::UUID) {
			// IcebergValue does not currently serialize bounds for these types. Omitting the
			// optional bound is conservative and leaves the remaining metrics usable.
			continue;
		}
		if (column->type.id() == LogicalTypeId::DECIMAL) {
			value = value.DefaultCastAs(LogicalType::VARCHAR);
		}
		IcebergMetricsConfig metrics;
		metrics.mode = IcebergMetricsMode::FULL;
		metrics.truncate_length = DConstants::INVALID_INDEX;
		auto serialized = IcebergValue::SerializeValue(std::move(value), column->type, bound, metrics);
		if (serialized.HasError()) {
			throw InvalidInputException("Could not encode Iceberg REST scan-planning bound for field id %d: %s",
			                            field_id, serialized.GetError());
		}
		if (serialized.HasValue()) {
			target[field_id] = serialized.GetValue();
		}
	}
}

static IcebergDataFile ConvertContentFile(const rest_api_objects::ContentFile &source,
                                          const IcebergTableMetadata &metadata,
                                          IcebergManifestEntryContentType content) {
	IcebergDataFile result;
	result.content = content;
	result.file_path = source.file_path;
	result.file_format = source.file_format.value;
	result.record_count = source.record_count;
	result.file_size_in_bytes = source.file_size_in_bytes;
	result.sort_order_id = source.sort_order_id;
	if (source.split_offsets) {
		result.split_offsets = *source.split_offsets;
	}

	auto spec_it = metadata.partition_specs.find(source.spec_id);
	if (spec_it == metadata.partition_specs.end()) {
		throw InvalidInputException("Iceberg REST scan-planning returned unknown partition spec id %d", source.spec_id);
	}
	auto &fields = spec_it->second.fields;
	if (fields.size() != source.partition.size()) {
		throw InvalidInputException("Iceberg REST scan-planning returned %d partition values for spec %d, expected %d",
		                            source.partition.size(), source.spec_id, fields.size());
	}
	for (idx_t i = 0; i < fields.size(); i++) {
		auto column = metadata.FindColumnByFieldId(NumericCast<int32_t>(fields[i].source_id));
		if (!column) {
			throw InvalidInputException("Iceberg REST scan-planning returned a partition for unknown field id %d",
			                            fields[i].source_id);
		}
		auto partition_type = fields[i].transform.GetSerializedType(column->type);
		result.partition_info.push_back(
		    IcebergPartitionInfo {fields[i].partition_field_id, PrimitiveValue(source.partition[i], partition_type)});
	}
	return result;
}

static PlannedContentFile ConvertDataFile(const rest_api_objects::DataFile &source,
                                          const IcebergTableMetadata &metadata) {
	auto result = ConvertContentFile(source.content_file, metadata, IcebergManifestEntryContentType::DATA);
	result.SetFirstRowId(source.first_row_id);
	CopyCountMap(source.column_sizes, result.column_sizes);
	CopyCountMap(source.value_counts, result.value_counts);
	CopyCountMap(source.null_value_counts, result.null_value_counts);
	CopyCountMap(source.nan_value_counts, result.nan_value_counts);
	CopyValueMap(source.lower_bounds, metadata, result.lower_bounds, SerializeBound::LOWER_BOUND);
	CopyValueMap(source.upper_bounds, metadata, result.upper_bounds, SerializeBound::UPPER_BOUND);
	return PlannedContentFile {std::move(result), source.content_file.spec_id};
}

static PlannedContentFile ConvertDeleteFile(yyjson_val *value, const IcebergTableMetadata &metadata) {
	auto content = GetRequiredString(value, "content");
	if (content == "position-deletes") {
		auto source = rest_api_objects::PositionDeleteFile::FromJSON(value);
		auto result =
		    ConvertContentFile(source.content_file, metadata, IcebergManifestEntryContentType::POSITION_DELETES);
		result.content_offset = source.content_offset;
		result.content_size_in_bytes = source.content_size_in_bytes;
		return PlannedContentFile {std::move(result), source.content_file.spec_id};
	}
	if (content == "equality-deletes") {
		auto source = rest_api_objects::EqualityDeleteFile::FromJSON(value);
		auto result =
		    ConvertContentFile(source.content_file, metadata, IcebergManifestEntryContentType::EQUALITY_DELETES);
		if (source.equality_ids) {
			result.equality_ids = *source.equality_ids;
		}
		return PlannedContentFile {std::move(result), source.content_file.spec_id};
	}
	throw InvalidInputException("Unknown Iceberg REST scan-planning delete-file content '%s'", content);
}

static void ParseTasks(yyjson_val *root, const IcebergTableMetadata &metadata, PlanningAccumulator &result) {
	vector<idx_t> local_delete_indexes;
	auto deletes = yyjson_obj_get(root, "delete-files");
	if (deletes) {
		if (!yyjson_is_arr(deletes)) {
			throw InvalidInputException("Iceberg REST scan-planning 'delete-files' must be an array");
		}
		size_t idx, count;
		yyjson_val *value;
		yyjson_arr_foreach(deletes, idx, count, value) {
			local_delete_indexes.push_back(result.delete_files.size());
			result.delete_files.push_back(ConvertDeleteFile(value, metadata));
		}
	}

	auto file_tasks = yyjson_obj_get(root, "file-scan-tasks");
	if (file_tasks) {
		if (!yyjson_is_arr(file_tasks)) {
			throw InvalidInputException("Iceberg REST scan-planning 'file-scan-tasks' must be an array");
		}
		size_t idx, count;
		yyjson_val *value;
		yyjson_arr_foreach(file_tasks, idx, count, value) {
			auto data_file_value = yyjson_obj_get(value, "data-file");
			if (!data_file_value) {
				throw InvalidInputException("Iceberg REST file scan task is missing 'data-file'");
			}
			PlannedFileTask task;
			task.data_file = ConvertDataFile(rest_api_objects::DataFile::FromJSON(data_file_value), metadata);
			auto refs = yyjson_obj_get(value, "delete-file-references");
			if (refs) {
				if (!yyjson_is_arr(refs)) {
					throw InvalidInputException("Iceberg REST 'delete-file-references' must be an array");
				}
				size_t ref_idx, ref_count;
				yyjson_val *ref;
				yyjson_arr_foreach(refs, ref_idx, ref_count, ref) {
					if (!yyjson_is_int(ref) || (yyjson_is_sint(ref) && yyjson_get_sint(ref) < 0)) {
						throw InvalidInputException("Iceberg REST delete-file reference must be an integer");
					}
					auto local_index = NumericCast<idx_t>(yyjson_get_uint(ref));
					if (local_index >= local_delete_indexes.size()) {
						throw InvalidInputException("Iceberg REST delete-file reference %d is out of range",
						                            local_index);
					}
					task.delete_file_references.push_back(local_delete_indexes[local_index]);
				}
			}
			result.file_tasks.push_back(std::move(task));
		}
	}

	auto plan_tasks = yyjson_obj_get(root, "plan-tasks");
	if (plan_tasks) {
		if (!yyjson_is_arr(plan_tasks)) {
			throw InvalidInputException("Iceberg REST scan-planning 'plan-tasks' must be an array");
		}
		size_t idx, count;
		yyjson_val *value;
		yyjson_arr_foreach(plan_tasks, idx, count, value) {
			if (!yyjson_is_str(value)) {
				throw InvalidInputException("Iceberg REST plan task must be a string");
			}
			result.plan_tasks.emplace_back(yyjson_get_str(value));
		}
	}
}

static void ParseCredentials(yyjson_val *root, IcebergRESTScanPlan &result) {
	auto credentials = yyjson_obj_get(root, "storage-credentials");
	if (!credentials) {
		return;
	}
	if (!yyjson_is_arr(credentials)) {
		throw InvalidInputException("Iceberg REST scan-planning 'storage-credentials' must be an array");
	}
	size_t idx, count;
	yyjson_val *value;
	yyjson_arr_foreach(credentials, idx, count, value) {
		result.storage_credentials.push_back(rest_api_objects::StorageCredential::FromJSON(value));
	}
}

static string SerializePlanRequest(const rest_api_objects::PlanTableScanRequest &request) {
	unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc(yyjson_mut_doc_new(nullptr));
	yyjson_mut_doc_set_root(doc.get(), request.ToJSON(doc.get()));
	return ICUtils::JsonToString(std::move(doc));
}

static void FetchPlanTasks(ClientContext &context, IcebergTableInformation &table_info,
                           PlanningAccumulator &accumulator) {
	for (idx_t task_idx = 0; task_idx < accumulator.plan_tasks.size(); task_idx++) {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		auto endpoint = TableEndpoint(table_info);
		endpoint.AddPathComponent(IRCPathComponent::RegularComponent("tasks"));
		unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc(yyjson_mut_doc_new(nullptr));
		auto root = yyjson_mut_obj(doc.get());
		yyjson_mut_doc_set_root(doc.get(), root);
		yyjson_mut_obj_add_strcpy(doc.get(), root, "plan-task", accumulator.plan_tasks[task_idx].c_str());
		auto body = ICUtils::JsonToString(std::move(doc));
		auto headers = PlanningHeaders(context);
		headers.Insert("Idempotency-Key", UUID::ToString(UUID::GenerateRandomUUID()));
		auto response =
		    table_info.catalog.auth_handler->Request(RequestType::POST_REQUEST, context, endpoint, headers, body);
		if (response->status != HTTPStatusCode::OK_200) {
			ThrowResponseError(endpoint, *response);
		}
		auto response_doc = ICUtils::APIResultToDoc(response->body);
		ParseTasks(yyjson_doc_get_root(response_doc.get()), table_info.table_metadata, accumulator);
	}
}

static void FetchCredentials(ClientContext &context, IcebergTableInformation &table_info,
                             const optional<string> &plan_id, IcebergRESTScanPlan &result) {
	if (!result.storage_credentials.empty() ||
	    table_info.catalog.supported_urls.find(IcebergScanPlanning::CREDENTIALS_ENDPOINT) ==
	        table_info.catalog.supported_urls.end()) {
		return;
	}
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	// Some services scope vended credentials to the files produced by a plan.
	auto endpoint = TableEndpoint(table_info);
	endpoint.AddPathComponent(IRCPathComponent::RegularComponent("credentials"));
	if (plan_id) {
		endpoint.SetParam("planId", IRCPathComponent::RegularComponent(*plan_id));
	}
	auto headers = PlanningHeaders(context);
	auto response = table_info.catalog.auth_handler->Request(RequestType::GET_REQUEST, context, endpoint, headers);
	if (response->status != HTTPStatusCode::OK_200) {
		ThrowResponseError(endpoint, *response);
	}
	auto doc = ICUtils::APIResultToDoc(response->body);
	auto credentials = rest_api_objects::LoadCredentialsResponse::FromJSON(yyjson_doc_get_root(doc.get()));
	result.storage_credentials = std::move(credentials.storage_credentials);
}

static idx_t GetPollDelay(const HTTPResponse &response, idx_t fallback_ms) {
	if (!response.HasHeader("Retry-After")) {
		return fallback_ms;
	}
	try {
		auto seconds = std::stoull(response.GetHeaderValue("Retry-After"));
		return NumericCast<idx_t>(MinValue<uint64_t>(seconds * 1000, 60000));
	} catch (...) {
		// HTTP-date Retry-After values are uncommon here; retain exponential polling for values we cannot parse.
		return fallback_ms;
	}
}

static void WaitForPoll(ClientContext &context, idx_t delay_ms) {
	while (delay_ms > 0) {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		auto step_ms = MinValue<idx_t>(delay_ms, 100);
		std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
		delay_ms -= step_ms;
	}
}

static vector<IcebergManifestListEntry> MakeManifests(FileSystem &fs, const IcebergTableMetadata &metadata,
                                                      vector<PlannedContentFile> &&files,
                                                      IcebergManifestContentType content,
                                                      sequence_number_t sequence_number) {
	map<int32_t, vector<IcebergManifestEntry>> by_spec;
	for (auto &planned_file : files) {
		IcebergManifestEntry entry;
		entry.status = IcebergManifestEntryStatusType::EXISTING;
		entry.SetSequenceNumber(sequence_number);
		entry.SetFileSequenceNumber(sequence_number);
		entry.data_file = std::move(planned_file.file);
		by_spec[planned_file.spec_id].push_back(std::move(entry));
	}

	vector<IcebergManifestListEntry> result;
	int64_t next_row_id = 0;
	for (auto &entry : by_spec) {
		auto manifest_metadata = IcebergManifestMetadata::FromTableMetadata(metadata, content, entry.first);
		result.push_back(IcebergManifestListEntry::CreateFromEntries(fs, sequence_number, metadata, manifest_metadata,
		                                                             std::move(entry.second), next_row_id));
	}
	return result;
}

} // namespace

bool IcebergScanPlanning::Plan(ClientContext &context, IcebergTableInformation &table_info,
                               rest_api_objects::PlanTableScanRequest request, IcebergRESTScanPlan &result) {
	auto endpoint = TableEndpoint(table_info);
	endpoint.AddPathComponent(IRCPathComponent::RegularComponent("plan"));
	auto headers = PlanningHeaders(context);
	// A fresh key makes retries of each logical planning operation idempotent on servers that support it.
	headers.Insert("Idempotency-Key", UUID::ToString(UUID::GenerateRandomUUID()));
	auto response = table_info.catalog.auth_handler->Request(RequestType::POST_REQUEST, context, endpoint, headers,
	                                                         SerializePlanRequest(request));
	if (response->status == HTTPStatusCode::NotAcceptable_406) {
		return false;
	}
	if (response->status != HTTPStatusCode::OK_200) {
		ThrowResponseError(endpoint, *response);
	}

	PlanningAccumulator accumulator;
	optional<string> active_plan_id;
	try {
		idx_t poll_delay_ms = 100;
		while (true) {
			auto doc = ICUtils::APIResultToDoc(response->body);
			auto root = yyjson_doc_get_root(doc.get());
			auto status = GetRequiredString(root, "status");
			if (status == "completed") {
				ParseTasks(root, table_info.table_metadata, accumulator);
				ParseCredentials(root, result);
				auto plan_id = yyjson_obj_get(root, "plan-id");
				if (plan_id && yyjson_is_str(plan_id)) {
					active_plan_id = string(yyjson_get_str(plan_id));
					result.plan_id = active_plan_id;
				}
				break;
			}
			if (status == "failed") {
				active_plan_id.reset();
				throw InvalidInputException("Iceberg REST scan planning failed: %s", response->body);
			}
			if (status == "cancelled") {
				active_plan_id.reset();
				throw InterruptException("Iceberg REST scan planning was cancelled by the server");
			}
			if (status != "submitted") {
				throw InvalidInputException("Unknown Iceberg REST scan-planning status '%s'", status);
			}
			active_plan_id = GetRequiredString(root, "plan-id");
			result.plan_id = active_plan_id;
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			WaitForPoll(context, GetPollDelay(*response, poll_delay_ms));
			poll_delay_ms = MinValue<idx_t>(poll_delay_ms * 2, 1000);

			endpoint = TableEndpoint(table_info);
			endpoint.AddPathComponent(IRCPathComponent::RegularComponent("plan"));
			endpoint.AddPathComponent(IRCPathComponent::RegularComponent(*active_plan_id));
			auto poll_headers = PlanningHeaders(context);
			response =
			    table_info.catalog.auth_handler->Request(RequestType::GET_REQUEST, context, endpoint, poll_headers);
			if (response->status != HTTPStatusCode::OK_200) {
				ThrowResponseError(endpoint, *response);
			}
		}

		FetchCredentials(context, table_info, active_plan_id, result);
		FetchPlanTasks(context, table_info, accumulator);

		for (auto &task : accumulator.file_tasks) {
			auto &refs = result.delete_files_by_data_file[task.data_file.file.file_path];
			for (auto delete_idx : task.delete_file_references) {
				auto &delete_file = accumulator.delete_files[delete_idx].file;
				refs.insert(delete_file.file_path);
				if (StringUtil::CIEquals(delete_file.file_format, "puffin")) {
					if (delete_file.referenced_data_file &&
					    !StringUtil::CIEquals(*delete_file.referenced_data_file, task.data_file.file.file_path)) {
						throw InvalidInputException(
						    "Iceberg REST scan plan references one Puffin deletion vector from multiple data files");
					}
					delete_file.referenced_data_file = task.data_file.file.file_path;
				}
			}
		}

		vector<PlannedContentFile> data_files;
		data_files.reserve(accumulator.file_tasks.size());
		for (auto &task : accumulator.file_tasks) {
			data_files.push_back(std::move(task.data_file));
		}
		auto &fs = FileSystem::GetFileSystem(context);
		result.data_manifests =
		    MakeManifests(fs, table_info.table_metadata, std::move(data_files), IcebergManifestContentType::DATA, 0);
		result.delete_manifests = MakeManifests(fs, table_info.table_metadata, std::move(accumulator.delete_files),
		                                        IcebergManifestContentType::DELETE, 1);
		return true;
	} catch (...) {
		if (active_plan_id) {
			try {
				auto cancel_endpoint = TableEndpoint(table_info);
				cancel_endpoint.AddPathComponent(IRCPathComponent::RegularComponent("plan"));
				cancel_endpoint.AddPathComponent(IRCPathComponent::RegularComponent(*active_plan_id));
				auto cancel_headers = PlanningHeaders(context);
				cancel_headers.Insert("Idempotency-Key", UUID::ToString(UUID::GenerateRandomUUID()));
				table_info.catalog.auth_handler->Request(RequestType::DELETE_REQUEST, context, cancel_endpoint,
				                                         cancel_headers);
			} catch (...) {
				// Best-effort cleanup must not mask the planning failure or interrupt.
			}
		}
		throw;
	}
}

} // namespace duckdb
