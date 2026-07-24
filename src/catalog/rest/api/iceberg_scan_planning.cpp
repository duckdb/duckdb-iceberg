#include "catalog/rest/api/iceberg_scan_planning.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "yyjson.hpp"

#include "iceberg_logging.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "core/expression/iceberg_value.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"
#include "rest_catalog/objects/data_file.hpp"
#include "rest_catalog/objects/fetch_scan_tasks_request.hpp"
#include "rest_catalog/objects/fetch_scan_tasks_result.hpp"
#include "rest_catalog/objects/load_credentials_response.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"

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

struct PlanTasksContainer {
public:
	void AddTask(string &&val) {
		auto res = distinct_values.emplace(val);
		if (!res.second) {
			throw InvalidConfigurationException("Server-side scan planning produced duplicate task identifier '%s'",
			                                    val);
		}
		values.emplace_back(std::move(val));
	}
	const vector<string> &Tasks() const {
		return values;
	}

private:
	unordered_set<string> distinct_values;
	vector<string> values;
};

struct PlanningAccumulator {
	vector<PlannedContentFile> delete_files;
	vector<PlannedFileTask> file_tasks;
	PlanTasksContainer plan_tasks;
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
	throw HTTPException(response,
	                    "Iceberg server-side scan-planning request to '%s' failed (HTTP %n). Reason: %s, body: %s",
	                    endpoint.GetURLEncoded(), int(response.status), response.reason, response.body);
}

static string GetRequiredString(yyjson_val *obj, const char *key) {
	auto value = yyjson_obj_get(obj, key);
	if (!value || !yyjson_is_str(value)) {
		throw InvalidInputException("Invalid Iceberg server-side scan-planning response: '%s' must be a string", key);
	}
	return yyjson_get_str(value);
}

static void CopyCountMap(const optional<rest_api_objects::CountMap> &source, unordered_map<int32_t, int64_t> &target) {
	if (!source || !source->keys || !source->values) {
		return;
	}
	if (source->keys->size() != source->values->size()) {
		throw InvalidInputException("Iceberg server-side scan-planning returned a malformed CountMap");
	}
	for (idx_t i = 0; i < source->keys->size(); i++) {
		target[(*source->keys)[i].value] = (*source->values)[i].value;
	}
}

static string DecodeHexValue(const string &value, int32_t field_id) {
	if (value.size() % 2 != 0) {
		throw InvalidInputException(
		    "Iceberg server-side scan-planning returned an invalid hexadecimal bound for field id %d", field_id);
	}

	string result(value.size() / 2, '\0');
	for (idx_t i = 0; i < value.size(); i += 2) {
		auto high = Blob::HEX_MAP[static_cast<uint8_t>(value[i])];
		auto low = Blob::HEX_MAP[static_cast<uint8_t>(value[i + 1])];
		if (high < 0 || low < 0) {
			throw InvalidInputException(
			    "Iceberg server-side scan-planning returned an invalid hexadecimal bound for field id %d", field_id);
		}
		result[i / 2] = static_cast<char>((high << 4) | low);
	}
	return result;
}

static void CopyValueMap(const optional<rest_api_objects::ValueMap> &source, const IcebergTableMetadata &metadata,
                         unordered_map<int32_t, Value> &target) {
	if (!source || !source->keys || !source->values) {
		return;
	}
	if (source->keys->size() != source->values->size()) {
		throw InvalidInputException("Iceberg server-side scan-planning returned a malformed ValueMap");
	}
	for (idx_t i = 0; i < source->keys->size(); i++) {
		auto field_id = (*source->keys)[i].value;
		auto column = metadata.FindColumnByFieldId(field_id);
		if (!column) {
			continue;
		}
		auto &serialized_value = (*source->values)[i];
		if (serialized_value.null_type_value) {
			continue;
		}
		if (!serialized_value.binary_type_value) {
			throw InvalidInputException("Iceberg server-side scan-planning returned a non-binary bound for field id %d",
			                            field_id);
		}

		// The REST API represents the Iceberg bound bytes as a hexadecimal string. Preserve the decoded bytes here;
		// consumers deserialize the BLOB using the column's Iceberg type.
		target[field_id] = Value::BLOB_RAW(DecodeHexValue(serialized_value.binary_type_value->value, field_id));
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
		throw InvalidInputException("Iceberg server-side scan-planning returned unknown partition spec id %d",
		                            source.spec_id);
	}
	auto &fields = spec_it->second.fields;
	if (fields.size() != source.partition.size()) {
		throw InvalidInputException(
		    "Iceberg server-side scan-planning returned %d partition values for spec %d, expected %d",
		    source.partition.size(), source.spec_id, fields.size());
	}
	for (idx_t i = 0; i < fields.size(); i++) {
		auto column = metadata.FindColumnByFieldId(NumericCast<int32_t>(fields[i].source_id));
		auto &field = fields[i];
		auto &transform = field.transform;
		auto &partition = source.partition[i];
		if (!column) {
			throw InvalidInputException(
			    "Iceberg server-side scan-planning returned a partition for unknown field id %d", field.source_id);
		}
		Value partition_value;
		if (transform.Type() == IcebergTransformType::DAY && partition.date_type_value) {
			//! NOTE: this is the same fix as FixSamePhysicalTypeCasts in 'iceberg_avro_multi_file_reader.cpp' is doing
			//! Compensating for Spark's DAY transform bug
			partition_value = Value::INTEGER(
			    IcebergColumnDefinition::ParsePrimitiveValue(LogicalType::DATE, partition).GetValue<date_t>().days);
		} else {
			auto serialized_type = transform.GetSerializedType(column->type);
			partition_value = IcebergColumnDefinition::ParsePrimitiveValue(serialized_type, partition);
		}
		result.partition_info.push_back(IcebergPartitionInfo {field.partition_field_id, partition_value});
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
	CopyValueMap(source.lower_bounds, metadata, result.lower_bounds);
	CopyValueMap(source.upper_bounds, metadata, result.upper_bounds);
	return PlannedContentFile {std::move(result), source.content_file.spec_id};
}

static PlannedContentFile ConvertDeleteFile(rest_api_objects::DeleteFile &source,
                                            const IcebergTableMetadata &metadata) {
	if (source.position_delete_file) {
		auto &position_delete = *source.position_delete_file;
		auto result = ConvertContentFile(position_delete.content_file, metadata,
		                                 IcebergManifestEntryContentType::POSITION_DELETES);
		result.content_offset = position_delete.content_offset;
		result.content_size_in_bytes = position_delete.content_size_in_bytes;
		return PlannedContentFile {std::move(result), position_delete.content_file.spec_id};
	}
	if (source.equality_delete_file) {
		auto &equality_delete = *source.equality_delete_file;
		auto result = ConvertContentFile(equality_delete.content_file, metadata,
		                                 IcebergManifestEntryContentType::EQUALITY_DELETES);
		if (equality_delete.equality_ids) {
			result.equality_ids = *equality_delete.equality_ids;
		}
		return PlannedContentFile {std::move(result), equality_delete.content_file.spec_id};
	}
	throw InvalidInputException("Iceberg server-side scan-planning returned an invalid delete file");
}

static void AppendTasks(rest_api_objects::ScanTasks tasks, const IcebergTableMetadata &metadata,
                        PlanningAccumulator &result) {
	vector<idx_t> local_delete_indexes;
	if (tasks.delete_files) {
		for (auto &delete_file : *tasks.delete_files) {
			local_delete_indexes.push_back(result.delete_files.size());
			result.delete_files.push_back(ConvertDeleteFile(delete_file, metadata));
		}
	}

	if (tasks.file_scan_tasks) {
		for (auto &file_task : *tasks.file_scan_tasks) {
			PlannedFileTask task;
			task.data_file = ConvertDataFile(file_task.data_file, metadata);
			if (file_task.delete_file_references) {
				for (auto reference : *file_task.delete_file_references) {
					if (reference < 0) {
						throw InvalidInputException("Iceberg REST delete-file reference must be non-negative");
					}
					auto local_index = NumericCast<idx_t>(reference);
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

	if (tasks.plan_tasks) {
		for (auto &plan_task : *tasks.plan_tasks) {
			result.plan_tasks.AddTask(std::move(plan_task.value));
		}
	}
}

static string SerializePlanRequest(const rest_api_objects::PlanTableScanRequest &request) {
	unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc(yyjson_mut_doc_new(nullptr));
	yyjson_mut_doc_set_root(doc.get(), request.ToJSON(doc.get()));
	return ICUtils::JsonToString(std::move(doc));
}

static void FetchPlanTasks(ClientContext &context, IcebergTableInformation &table_info,
                           PlanningAccumulator &accumulator) {
	for (auto &task_identifier : accumulator.plan_tasks.Tasks()) {
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		auto endpoint = TableEndpoint(table_info);
		endpoint.AddPathComponent(IRCPathComponent::RegularComponent("tasks"));
		rest_api_objects::FetchScanTasksRequest request;
		request.plan_task.value = task_identifier;
		unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc(yyjson_mut_doc_new(nullptr));
		yyjson_mut_doc_set_root(doc.get(), request.ToJSON(doc.get()));
		auto body = ICUtils::JsonToString(std::move(doc));
		auto headers = PlanningHeaders(context);
		headers.Insert("Idempotency-Key", UUID::ToString(UUID::GenerateRandomUUID()));
		auto response =
		    table_info.catalog.auth_handler->Request(RequestType::POST_REQUEST, context, endpoint, headers, body);
		if (response->status != HTTPStatusCode::OK_200) {
			ThrowResponseError(endpoint, *response);
		}
		auto response_doc = ICUtils::APIResultToDoc(response->body);
		auto tasks =
		    rest_api_objects::FetchScanTasksResult::FromJSON(yyjson_doc_get_root(response_doc.get())).scan_tasks;
		AppendTasks(std::move(tasks), table_info.table_metadata, accumulator);
	}
}

static void FetchCredentials(ClientContext &context, IcebergTableInformation &table_info,
                             const optional<string> &plan_id, IcebergServerSideScanPlan &result) {
	if (!result.storage_credentials.empty() ||
	    table_info.catalog.supported_urls.find(IcebergServerSideScanPlanning::CREDENTIALS_ENDPOINT) ==
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

static idx_t GetPollDelay(ClientContext &context, const HTTPResponse &response, idx_t fallback_ms) {
	if (!response.HasHeader("Retry-After")) {
		return fallback_ms;
	}
	auto retry_after = response.GetHeaderValue("Retry-After");
	try {
		auto seconds = std::stoull(retry_after);
		return NumericCast<idx_t>(MinValue<uint64_t>(seconds * 1000, 60000));
	} catch (std::exception &e) {
		auto error = ErrorData(e);
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg server-side scan planning ignored invalid Retry-After header '%s': %s", retry_after,
		           error.Message());
		// HTTP-date Retry-After values are uncommon here; retain exponential polling for values we cannot parse.
		return fallback_ms;
	} catch (...) { // LCOV_EXCL_START
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg server-side scan planning ignored invalid Retry-After header '%s': unknown exception",
		           retry_after);
		return fallback_ms;
	} // LCOV_EXCL_STOP
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

bool IcebergServerSideScanPlanning::Plan(ClientContext &context, IcebergTableInformation &table_info,
                                         rest_api_objects::PlanTableScanRequest request,
                                         IcebergServerSideScanPlan &result) {
	auto endpoint = TableEndpoint(table_info);
	endpoint.AddPathComponent(IRCPathComponent::RegularComponent("plan"));
	auto headers = PlanningHeaders(context);
	// A fresh key makes retries of each logical planning operation idempotent on servers that support it.
	headers.Insert("Idempotency-Key", UUID::ToString(UUID::GenerateRandomUUID()));
	auto body = SerializePlanRequest(request);
	auto response =
	    table_info.catalog.auth_handler->Request(RequestType::POST_REQUEST, context, endpoint, headers, body);
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
				auto completed = rest_api_objects::CompletedPlanningResult::FromJSON(root);
				AppendTasks(std::move(completed.scan_tasks), table_info.table_metadata, accumulator);
				if (completed.object_5.storage_credentials) {
					result.storage_credentials = std::move(*completed.object_5.storage_credentials);
				}
				auto plan_id = yyjson_obj_get(root, "plan-id");
				if (plan_id && yyjson_is_str(plan_id)) {
					active_plan_id = string(yyjson_get_str(plan_id));
					result.plan_id = active_plan_id;
				}
				break;
			}
			if (status == "failed") {
				active_plan_id.reset();
				throw InvalidInputException("Iceberg server-side scan planning failed: %s", response->body);
			}
			if (status == "cancelled") {
				active_plan_id.reset();
				throw InterruptException("Iceberg server-side scan planning was cancelled by the server");
			}
			if (status != "submitted") {
				throw InvalidInputException("Unknown Iceberg server-side scan-planning status '%s'", status);
			}
			active_plan_id = GetRequiredString(root, "plan-id");
			result.plan_id = active_plan_id;
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			WaitForPoll(context, GetPollDelay(context, *response, poll_delay_ms));
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

		vector<PlannedContentFile> data_files;
		data_files.reserve(accumulator.file_tasks.size());
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
			} catch (std::exception &e) {
				auto error = ErrorData(e);
				DUCKDB_LOG(context, IcebergLogType,
				           "Iceberg server-side scan planning failed to cancel plan '%s' during cleanup: %s",
				           *active_plan_id, error.Message());
				// Best-effort cleanup must not mask the planning failure or interrupt.
			} catch (...) { // LCOV_EXCL_START
				DUCKDB_LOG(
				    context, IcebergLogType,
				    "Iceberg server-side scan planning failed to cancel plan '%s' during cleanup: unknown exception",
				    *active_plan_id);
			} // LCOV_EXCL_STOP
		}
		throw;
	}
}

} // namespace duckdb
