#include "execution/operator/iceberg_insert_overwrite.hpp"

#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "iceberg_options.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/pruning/iceberg_file_pruner.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

IcebergOverwriteMode IcebergInsertOverwrite::ParseMode(const string &mode) {
	if (StringUtil::CIEquals(mode, "static")) {
		return IcebergOverwriteMode::STATIC;
	}
	if (StringUtil::CIEquals(mode, "dynamic")) {
		return IcebergOverwriteMode::DYNAMIC;
	}
	throw InvalidConfigurationException("'%s' must be 'static' or 'dynamic', got '%s'",
	                                    PARTITION_OVERWRITE_MODE_CONFIG_VARIABLE, mode);
}

IcebergOverwriteMode IcebergInsertOverwrite::GetMode(ClientContext &context) {
	Value mode;
	if (!context.TryGetCurrentSetting(PARTITION_OVERWRITE_MODE_CONFIG_VARIABLE, mode) || mode.IsNull()) {
		return IcebergOverwriteMode::STATIC;
	}
	return ParseMode(mode.GetValue<string>());
}

idx_t IcebergInsertOverwrite::ResolvePartitionColumn(const IcebergTableMetadata &metadata, const string &table_name,
                                                     const string &column_name) {
	auto &schema = metadata.GetLatestSchema();
	auto &partition_spec = metadata.GetLatestPartitionSpec();
	for (idx_t column_index = 0; column_index < schema.columns.size(); column_index++) {
		auto &column = *schema.columns[column_index];
		if (!StringUtil::CIEquals(column.name, column_name)) {
			continue;
		}
		for (auto &field : partition_spec.fields) {
			if (field.transform.Type() == IcebergTransformType::IDENTITY &&
			    field.source_id == NumericCast<uint64_t>(column.id)) {
				return column_index;
			}
		}
		break;
	}
	//! A partition value produced by a transform, such as day(ts), cannot be given for the source column
	throw BinderException("PARTITION clause names \"%s\", which is not an identity partition column of table \"%s\"",
	                      column_name, table_name);
}

namespace {

//! Whether 'text' is a plain number, [+-]digits[.digits], and how many digits it has on each side of the point
bool ParsePlainNumber(const string &text, idx_t &integer_digits, idx_t &fraction_digits) {
	idx_t position = 0;
	if (position < text.size() && (text[position] == '+' || text[position] == '-')) {
		position++;
	}
	integer_digits = 0;
	fraction_digits = 0;
	for (; position < text.size() && StringUtil::CharacterIsDigit(text[position]); position++) {
		integer_digits++;
	}
	if (position < text.size() && text[position] == '.') {
		for (position++; position < text.size() && StringUtil::CharacterIsDigit(text[position]); position++) {
			fraction_digits++;
		}
	}
	return position == text.size() && integer_digits + fraction_digits > 0;
}

} // namespace

Value IcebergInsertOverwrite::CastPartitionValue(ClientContext &context, const Value &value, const LogicalType &type,
                                                 const string &column_name) {
	const bool from_string = value.type().id() == LogicalTypeId::VARCHAR;
	//! A string for a date, a timestamp or a boolean has to be a literal of that type
	string error;
	auto result = value.TryCastAs(context, type, &error, from_string && !type.IsNumeric());
	if (!result) {
		throw BinderException("PARTITION value %s cannot be used for column \"%s\" of type %s: %s", value.ToSQLString(),
		                      column_name, type.ToString(), error);
	}
	if (value.IsNull()) {
		return std::move(*result);
	}
	//! A cast that rounds, as an INSERT does, would replace the rows of another value: the value has to round-trip
	Value source = value;
	bool exact = true;
	if (from_string) {
		if (!type.IsIntegral() && type.id() != LogicalTypeId::DECIMAL) {
			//! For every other type the cast is how the string is read
			return std::move(*result);
		}
		//! The number the text denotes, with every digit it has: a type with fewer digits would round it first
		auto text = value.GetValue<string>();
		StringUtil::Trim(text);
		idx_t integer_digits;
		idx_t fraction_digits;
		if (!ParsePlainNumber(text, integer_digits, fraction_digits)) {
			throw BinderException("PARTITION value %s cannot be used for column \"%s\" of type %s: a number for this "
			                      "column is written as digits with an optional fraction",
			                      value.ToSQLString(), column_name, type.ToString());
		}
		const auto digits = integer_digits + fraction_digits;
		optional<Value> denoted;
		if (digits <= Decimal::MAX_WIDTH_DECIMAL) {
			auto denoted_type =
			    LogicalType::DECIMAL(NumericCast<uint8_t>(digits), NumericCast<uint8_t>(fraction_digits));
			denoted = Value(text).TryCastAs(context, denoted_type);
		}
		//! With more digits than a number holds it is no value of the column
		exact = denoted.has_value();
		if (denoted) {
			source = std::move(*denoted);
		}
	}
	if (exact) {
		auto round_trip = result->TryCastAs(context, source.type());
		exact = round_trip && Value::NotDistinctFrom(*round_trip, source);
	}
	if (!exact) {
		throw BinderException(
		    "PARTITION value %s cannot be used for column \"%s\" of type %s: the value does not fit the type exactly",
		    value.ToSQLString(), column_name, type.ToString());
	}
	return std::move(*result);
}

namespace {

//! The static values as one filter per column: 'column = value', or 'column IS NULL' for a NULL value
IcebergTableFilters StaticValueFilters(const IcebergTableSchema &schema, const IcebergOverwriteSpec &spec) {
	IcebergTableFilters filters;
	for (auto &static_value : spec.static_values) {
		ColumnIndex column_index(static_value.column_index);
		if (static_value.value.IsNull()) {
			auto &column = *schema.columns[static_value.column_index];
			auto is_null = ExpressionFilter::CreateNullCheckExpression(
			    make_uniq<BoundReferenceExpression>(column.type, 0ULL), ExpressionType::OPERATOR_IS_NULL);
			filters.PushFilter(column_index, make_uniq<ExpressionFilter>(std::move(is_null)));
		} else {
			filters.PushFilter(column_index, ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL,
			                                                                          static_value.value));
		}
	}
	return filters;
}

//! The partition of a file as a key: its values for the given partition fields
string PartitionKey(const vector<IcebergPartitionInfo> &partition_info, const unordered_set<uint64_t> &key_fields) {
	vector<reference<const IcebergPartitionInfo>> fields;
	for (auto &field : partition_info) {
		if (key_fields.count(field.field_id)) {
			fields.push_back(field);
		}
	}
	std::sort(fields.begin(), fields.end(), [](const IcebergPartitionInfo &left, const IcebergPartitionInfo &right) {
		return left.field_id < right.field_id;
	});
	string key;
	for (auto &field_p : fields) {
		auto &field = field_p.get();
		key += to_string(field.field_id);
		if (field.value.IsNull()) {
			key += "=NULL;";
			continue;
		}
		//! The length keeps a value that holds the separators from reading like two fields
		auto value = field.value.ToString();
		key += ":" + to_string(value.size()) + ":" + value + ";";
	}
	return key;
}

//! Collects the files an overwrite replaces from manifests
struct ReplacedFileCollector {
	ReplacedFileCollector(ClientContext &context, const IcebergTableMetadata &metadata,
	                      optional_ptr<const IcebergTransactionData> transaction_data, const IcebergOverwriteSpec &spec,
	                      IcebergOverwriteMode mode, const vector<IcebergManifestEntry> &written_files)
	    : metadata(metadata), transaction_data(transaction_data), spec(spec),
	      filters(mode == IcebergOverwriteMode::STATIC ? StaticValueFilters(metadata.GetLatestSchema(), spec)
	                                                   : IcebergTableFilters()),
	      pruner(context, metadata, metadata.GetLatestSchema(), filters) {
		auto &partition_spec = metadata.GetLatestPartitionSpec();
		if (mode == IcebergOverwriteMode::STATIC) {
			replace_all = !filters.HasFilters();
		} else if (partition_spec.IsUnpartitioned()) {
			//! The table is one partition, which the written rows replace
			replace_all = !written_files.empty();
		} else {
			//! Written files do not list a void field, committed ones list NULL for it: leave it out of the key
			replaced_spec_id = partition_spec.spec_id;
			for (auto &field : partition_spec.fields) {
				if (field.transform.Type() != IcebergTransformType::VOID_TRANSFORM) {
					replaced_spec_fields.insert(field.partition_field_id);
				}
			}
			for (auto &written_file : written_files) {
				replaced_partitions.insert(PartitionKey(written_file.data_file.partition_info, replaced_spec_fields));
			}
		}
	}

	const IcebergTableMetadata &metadata;
	//! What the transaction has changed in the table, if anything
	optional_ptr<const IcebergTransactionData> transaction_data;
	const IcebergOverwriteSpec &spec;
	//! Every file is replaced
	bool replace_all = false;
	//! Static mode: one filter per static value
	IcebergTableFilters filters;
	IcebergFilePruner pruner;
	//! Dynamic mode: the partitions that received rows, and the partition spec they belong to
	int32_t replaced_spec_id = 0;
	unordered_set<uint64_t> replaced_spec_fields;
	unordered_set<string> replaced_partitions;
	//! The paths of the replaced data files, by which deletion vectors refer to them
	unordered_set<string> replaced_data_files;
	IcebergManifestDeletes replaced_files;

	//! Whether a manifest of the table's snapshot can hold a file that is replaced. Its entries are not read if not
	bool MayHoldReplacedFiles(const IcebergManifestListEntry &list_entry) const {
		auto &manifest = list_entry.GetManifest();
		if (manifest.counts && manifest.counts->HasNoLiveFiles()) {
			//! Only files that an earlier snapshot removed
			return false;
		}
		if (replace_all) {
			return true;
		}
		if (filters.HasFilters()) {
			return pruner.ManifestMatchesFilter(list_entry);
		}
		return manifest.partition_spec_id == replaced_spec_id;
	}

	//! Static mode: matches the identity partition values of a file against the static values. ROWS_MIGHT_MATCH
	//! when the file's partition spec does not partition by one of the columns
	IcebergMetricsMatch MatchPartitionValues(const IcebergManifest &manifest, const IcebergDataFile &file) const {
		//! The spec was looked up before, when the manifest was selected or made from the transaction's files
		auto partition_spec = metadata.partition_specs.find(manifest.partition_spec_id);
		D_ASSERT(partition_spec != metadata.partition_specs.end());
		auto &schema = metadata.GetLatestSchema();
		bool undecided = false;
		for (auto &static_value : spec.static_values) {
			auto &column = *schema.columns[static_value.column_index];
			optional_ptr<const Value> partition_value;
			for (auto &field : partition_spec->second.fields) {
				if (field.transform.Type() != IcebergTransformType::IDENTITY ||
				    field.source_id != NumericCast<uint64_t>(column.id)) {
					continue;
				}
				for (auto &file_value : file.partition_info) {
					if (file_value.field_id == field.partition_field_id) {
						partition_value = file_value.value;
					}
				}
			}
			if (!partition_value) {
				undecided = true;
			} else if (!Value::NotDistinctFrom(*partition_value, static_value.value)) {
				return IcebergMetricsMatch::ROWS_CANNOT_MATCH;
			}
		}
		return undecided ? IcebergMetricsMatch::ROWS_MIGHT_MATCH : IcebergMetricsMatch::ROWS_MUST_MATCH;
	}

	//! Whether a data file is replaced: by its partition values, and where they do not decide by its column bounds
	bool ReplacesDataFile(const IcebergManifest &manifest, const IcebergManifestEntry &entry) const {
		if (replace_all) {
			return true;
		}
		if (!filters.HasFilters()) {
			//! A file written under another partition spec is not in a partition of the current one
			return manifest.partition_spec_id == replaced_spec_id &&
			       replaced_partitions.count(PartitionKey(entry.data_file.partition_info, replaced_spec_fields));
		}
		auto match = MatchPartitionValues(manifest, entry.data_file);
		if (match == IcebergMetricsMatch::ROWS_MIGHT_MATCH) {
			match = pruner.FileMatchesFilter(manifest, entry, true);
		}
		switch (match) {
		case IcebergMetricsMatch::ROWS_MUST_MATCH:
			return true;
		case IcebergMetricsMatch::ROWS_CANNOT_MATCH:
			return false;
		default:
			throw InvalidInputException(
			    "Cannot replace the rows that match the PARTITION values: some, but not all, rows of data file "
			    "\"%s\" may match. The file was written under a partition spec that does not separate these rows.",
			    entry.data_file.file_path);
		}
	}

	//! A deletion vector goes with its data file, another delete file when its partition is replaced. With static
	//! values only its partition values count: its column bounds describe the deletes, not the rows they delete
	bool ReplacesDeleteFile(const IcebergManifest &manifest, const IcebergManifestEntry &entry) const {
		auto &delete_file = entry.data_file;
		if (delete_file.IsDeletionVector() && delete_file.referenced_data_file &&
		    replaced_data_files.count(*delete_file.referenced_data_file)) {
			return true;
		}
		if (replace_all) {
			return true;
		}
		if (filters.HasFilters()) {
			return MatchPartitionValues(manifest, delete_file) == IcebergMetricsMatch::ROWS_MUST_MATCH;
		}
		return manifest.partition_spec_id == replaced_spec_id &&
		       replaced_partitions.count(PartitionKey(delete_file.partition_info, replaced_spec_fields));
	}

	void AddManifest(const IcebergManifestListEntry &list_entry) {
		//! A manifest of the transaction has no file yet, only GetManifest() serves both kinds
		auto &manifest = list_entry.GetManifest();
		const bool is_data = manifest.content == IcebergManifestContentType::DATA;
		for (const auto &entry : list_entry.GetManifestEntries()) {
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			auto &file = entry.data_file;
			IcebergFileIdentity identity(file.file_path, file.content_offset);
			//! Removed by an earlier statement of this transaction
			if (transaction_data && transaction_data->IsFileInvalidated(identity)) {
				continue;
			}
			if (is_data) {
				if (file.content != IcebergManifestEntryContentType::DATA || !ReplacesDataFile(manifest, entry)) {
					continue;
				}
				replaced_data_files.insert(file.file_path);
			} else if (!ReplacesDeleteFile(manifest, entry)) {
				continue;
			}
			replaced_files.InvalidateFile(identity);
		}
	}

	//! The manifest list of the snapshot the transaction reads, and the manifests that can hold a replaced file
	shared_ptr<const IcebergOverwriteManifests> LoadManifests(ClientContext &context, const IcebergTable &table) const {
		auto snapshot = metadata.GetLatestSnapshot();
		if (!snapshot) {
			return nullptr;
		}
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = snapshot;
		snapshot_info.schema_id = metadata.GetCurrentSchemaId();
		//! Install vended storage secrets before reading manifest lists from object storage
		table.LoadCredentials(context);

		auto result = make_shared_ptr<IcebergOverwriteManifests>();
		result->snapshot_id = snapshot->snapshot_id;
		if (transaction_data && !transaction_data->alters.empty()) {
			//! The transaction kept the manifest list when it added its first snapshot. The entries it loaded for
			//! manifests without file counts are left out: those of the selected manifests are read below
			for (auto &list_entry : transaction_data->existing_manifest_list) {
				result->manifest_list.emplace_back(list_entry.GetFile());
			}
		} else {
			IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context, result->manifest_list);
		}
		for (auto &list_entry : result->manifest_list) {
			if (MayHoldReplacedFiles(list_entry)) {
				result->manifests.push_back(list_entry);
			}
		}
		if (result->manifests.empty()) {
			return std::move(result);
		}
		IcebergOptions options;
		auto scan =
		    AvroScan::ScanManifest(snapshot_info, result->manifests, options, FileSystem::GetFileSystem(context),
		                           metadata.GetLocation(), metadata, context);
		manifest_file::ManifestReader reader(*scan);
		while (!reader.Finished()) {
			reader.Read();
		}
		scan->Complete();
		return std::move(result);
	}

	//! Collects from 'manifests' and from the files written earlier in the transaction. Data files first: which
	//! delete files go depends on them
	void Collect(optional_ptr<const IcebergOverwriteManifests> manifests) {
		for (auto content : {IcebergManifestContentType::DATA, IcebergManifestContentType::DELETE}) {
			if (manifests) {
				for (const auto &list_entry : manifests->manifests) {
					if (list_entry.GetManifest().content == content) {
						AddManifest(list_entry);
					}
				}
			}
			if (!transaction_data) {
				continue;
			}
			auto sequence_number = transaction_data->scan_sequence_number;
			for (auto &alter : transaction_data->alters) {
				for (const auto &pending_manifest : alter.get().GetPendingManifests()) {
					if (pending_manifest.GetMetadata().content == content) {
						AddManifest(pending_manifest.CreateScanEntry(metadata, sequence_number));
					}
				}
				sequence_number++;
			}
		}
	}
};

} // namespace

void IcebergInsertOverwrite::PlanStaticOverwrite(ClientContext &context, const IcebergTable &table,
                                                 IcebergOverwriteSpec &spec) {
	if (spec.static_values.empty() || spec.missing_static_values || GetMode(context) != IcebergOverwriteMode::STATIC) {
		//! Every file is replaced, or the files that are written decide which are
		return;
	}
	ReplacedFileCollector collector(context, table.GetMetadata(), table.transaction_data.get(), spec,
	                                IcebergOverwriteMode::STATIC, {});
	spec.planned_manifests = collector.LoadManifests(context, table);
	collector.Collect(spec.planned_manifests.get());
}

void IcebergInsertOverwrite::Stage(ClientContext &context, const IcebergTable &table,
                                   IcebergTransactionData &transaction_data, const IcebergOverwriteSpec &spec,
                                   vector<IcebergManifestEntry> &&written_files) {
	if (spec.missing_static_values) {
		throw InternalException("INSERT OVERWRITE was executed without the values of its PARTITION clause");
	}
	//! The mode of the session that runs the statement, also when it was prepared under another
	auto mode = GetMode(context);
	if (mode == IcebergOverwriteMode::DYNAMIC && written_files.empty()) {
		//! No partition received rows
		return;
	}
	auto &metadata = table.GetMetadata();
	ReplacedFileCollector collector(context, metadata, transaction_data, spec, mode, written_files);

	//! The manifests read when the statement was planned serve while the transaction reads the same snapshot
	auto manifests = spec.planned_manifests;
	auto snapshot = metadata.GetLatestSnapshot();
	if (!collector.filters.HasFilters() || !manifests || !snapshot || manifests->snapshot_id != snapshot->snapshot_id) {
		manifests = collector.LoadManifests(context, table);
	}
	collector.Collect(manifests.get());
	if (written_files.empty() && collector.replaced_files.IsEmpty()) {
		//! Nothing to stage, but that depended on the files of the table the transaction reads
		lock_guard<mutex> guard(transaction_data.lock);
		transaction_data.contains_overwrite = true;
		return;
	}
	//! The manifest list handed over is the one of the snapshot the transaction reads
	D_ASSERT(!manifests || manifests->snapshot_id == snapshot->snapshot_id);
	transaction_data.AddOverwriteSnapshot(std::move(written_files), std::move(collector.replaced_files),
	                                      mode == IcebergOverwriteMode::DYNAMIC,
	                                      manifests ? &manifests->manifest_list : nullptr);
}

} // namespace duckdb
