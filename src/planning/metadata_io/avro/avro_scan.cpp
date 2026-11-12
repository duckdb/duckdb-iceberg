#include "planning/metadata_io/avro/avro_scan.hpp"

#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"

#include "iceberg_extension.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/metadata_io/avro/iceberg_avro_multi_file_reader.hpp"
#include "planning/metadata_io/avro/iceberg_avro_multi_file_list.hpp"
#include "duckdb/logging/logger.hpp"
#include "iceberg_logging.hpp"
#include "planning/metadata_io/avro/iceberg_manifest_cache.hpp"

namespace duckdb {

namespace {

struct ManifestFileVirtualColumn {
	column_t id;
	const char *name;
	LogicalType type;
};

} // namespace

AvroScan::AvroScan(const string &path, ClientContext &context, shared_ptr<IcebergAvroScanInfo> avro_scan_info)
    : context(context), scan_info(avro_scan_info) {
	if (scan_info->type == AvroScanInfoType::MANIFEST_FILE) {
		FindCachedManifests();
		if (scan_info->Cast<IcebergManifestFileScanInfo>().manifest_indexes.empty()) {
			finished = true;
			return;
		}
	}
	auto &instance = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "read_avro");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"read_avro\" not found!");
	}
	auto &avro_scan_entry = catalog_entry->Cast<TableFunctionCatalogEntry>();
	avro_scan = *avro_scan_entry.functions.functions[0];

	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(path));
	named_argument_map_t named_params;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;

	const bool is_manifest_list = avro_scan_info->type == AvroScanInfoType::MANIFEST_LIST;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = Identifier(is_manifest_list ? "IcebergManifestList" : "IcebergManifest");
	dummy_table_function.get_multi_file_reader = IcebergAvroMultiFileReader::CreateInstance;
	dummy_table_function.function_info = avro_scan_info;
	BoundTableFunction bound_table_function(dummy_table_function);

	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  bound_table_function, empty);
	if (avro_scan->function_info) {
		// read_avro wraps a single-file function, which it keeps in the function info we replace above
		auto &avro_info = avro_scan->function_info->Cast<TableFunctionMultiFileInfo>();
		bind_data = TableFunctionMultiFileWrapper::MultiFileBindWith(context, bind_input, return_types, return_names,
		                                                             avro_info.function, avro_info.settings);
	} else {
		bind_data = avro_scan->bind(context, bind_input, return_types, return_names);
	}

	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	global_state = avro_scan->init_global(context, input);
}

AvroScan::~AvroScan() {
	if (scan_info->type == AvroScanInfoType::MANIFEST_FILE) {
		auto &info = scan_info->Cast<IcebergManifestFileScanInfo>();
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg metadata phase=manifest_parse scope=avro_scan manifest_parses=%llu "
		           "manifest_entries_converted=%llu",
		           info.manifest_parses.load(), info.manifest_entries_converted.load());
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg metadata phase=manifest_cache scope=avro_scan parsed_manifest_hits=%llu "
		           "parsed_manifest_misses=%llu",
		           cache_hits, cache_misses.size());
	}
}

void AvroScan::FindCachedManifests() {
	auto &info = scan_info->Cast<IcebergManifestFileScanInfo>();
	Value setting;
	const bool enabled =
	    context.TryGetCurrentSetting(ENABLE_MANIFEST_CACHE_CONFIG_VARIABLE, setting) && setting.GetValue<bool>();
	vector<idx_t> to_read;
	for (auto index : info.manifest_indexes) {
		auto &manifest = info.manifest_files[index];
		if (enabled) {
			auto key = ManifestCacheKey(info, index);
			auto cached = ObjectCache::GetObjectCache(context).GetWithTypePrefix<IcebergParsedManifest>(key);
			if (cached) {
				auto metadata = cached->Validate(info, manifest);
				manifest.ResetManifestEntries();
				manifest.manifest_metadata.emplace(metadata);
				manifest.parsed_manifest = std::move(cached);
				if (info.read_state) {
					info.read_state->PushBatch({index, 0, manifest.GetManifestEntries().size()});
				}
				cache_hits++;
				continue;
			}
			cache_misses.emplace_back(index, std::move(key));
		}
		if (info.read_state) {
			auto &counts = *manifest.GetFile().counts;
			manifest.GetOrCreateManifestEntries().reserve(*counts.added_files_count + *counts.existing_files_count +
			                                              *counts.deleted_files_count);
		}
		to_read.push_back(index);
	}
	// Retain the original merged decoding types even when only some manifests missed.
	info.manifest_indexes = std::move(to_read);
}

void AvroScan::Complete() {
	if (completed || scan_info->type != AvroScanInfoType::MANIFEST_FILE) {
		return;
	}
	context.InterruptCheck();
	if (!cache_misses.empty() && (!readers_started.load() || readers_started.load() != readers_finished.load())) {
		throw InternalException("Cannot cache an incomplete manifest scan");
	}
	auto &info = scan_info->Cast<IcebergManifestFileScanInfo>();
	// Build and validate the entire load before touching the shared cache.
	vector<shared_ptr<IcebergParsedManifest>> payloads;
	for (auto &miss : cache_misses) {
		auto &manifest = info.manifest_files[miss.first];
		auto payload = make_shared_ptr<IcebergParsedManifest>(context, manifest, info.metadata, miss.second.size());
		payload->Validate(info, manifest);
		payloads.push_back(std::move(payload));
	}
	context.InterruptCheck();
	for (idx_t i = 0; i < payloads.size(); i++) {
		auto &miss = cache_misses[i];
		auto &manifest = info.manifest_files[miss.first];
		manifest.parsed_manifest = payloads[i];
		ObjectCache::GetObjectCache(context).PutWithTypePrefix<IcebergParsedManifest>(miss.second, payloads[i]);
		if (payloads[i]->DeleteIndex()) {
			DUCKDB_LOG(context, IcebergLogType,
			           "Iceberg metadata phase=delete_candidate_index scope=manifest_cache delete_index_builds=1");
		}
	}
	completed = true;
}

unique_ptr<AvroScan> AvroScan::ScanManifest(const IcebergSnapshotScanInfo &snapshot_info,
                                            vector<IcebergManifestListEntry> &manifest_files,
                                            const IcebergOptions &options, FileSystem &fs, const string &iceberg_path,
                                            const IcebergTableMetadata &metadata, ClientContext &context,
                                            optional_ptr<ManifestEntryReadState> read_state,
                                            vector<idx_t> manifest_indexes, IcebergFileSecret file_secret) {
	D_ASSERT(!manifest_files.empty());
	auto avro_scan_info = make_shared_ptr<IcebergManifestFileScanInfo>(
	    metadata, snapshot_info, manifest_files, options, fs, iceberg_path, read_state, std::move(manifest_indexes));
	avro_scan_info->file_secret = std::move(file_secret);
	return make_uniq<AvroScan>("placeholder", context, std::move(avro_scan_info));
}

unique_ptr<AvroScan> AvroScan::ScanManifestList(const IcebergSnapshotScanInfo &snapshot_info,
                                                const IcebergTableMetadata &metadata, ClientContext &context,
                                                const string &path, vector<IcebergManifestListEntry> &result,
                                                IcebergFileSecret file_secret) {
	auto avro_scan_info = make_shared_ptr<IcebergManifestListScanInfo>(metadata, snapshot_info, result);
	avro_scan_info->file_secret = std::move(file_secret);
	return make_uniq<AvroScan>(path, context, std::move(avro_scan_info));
}

void AvroScan::InitializeChunk(DataChunk &chunk) const {
	chunk.Initialize(context, return_types, STANDARD_VECTOR_SIZE);
}

bool AvroScan::Finished() const {
	return finished;
}

const vector<column_t> &AvroScan::GetColumnIds() const {
	return column_ids;
}

const idx_t AvroScan::IcebergVersion() const {
	return scan_info->IcebergVersion();
}

} // namespace duckdb
