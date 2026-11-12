#include "catalog_model.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/set.hpp"
#include <algorithm>
#include <chrono>
#include <thread>

namespace duckdb {
namespace iceberg_test {

int64_t NowMillis() {
	return Timestamp::GetCurrentTimestamp().value / 1000;
}

static string NewUUID() {
	return UUID::ToString(UUID::GenerateRandomUUID());
}

Namespace ParseNamespace(const Json &value) {
	Namespace result;
	for (auto &part : value.Items()) {
		auto name = part.String();
		if (name.empty()) {
			Invalid("Namespace must contain nonempty strings");
		}
		result.push_back(std::move(name));
	}
	if (result.empty()) {
		Invalid("Namespace must contain nonempty strings");
	}
	return result;
}

Json NamespaceJSON(const Namespace &ns) {
	auto result = Json::Array();
	for (auto &part : ns) {
		result.Push(part);
	}
	return result;
}

Identifier ParseIdentifier(const Json &value) {
	auto name = value.At("name").String();
	if (name.empty()) {
		Invalid("Table identifier must contain a nonempty name");
	}
	return {ParseNamespace(value.At("namespace")), name};
}

static Json Properties(const Json &value) {
	for (auto &entry : value.Members()) {
		entry.second.String();
	}
	return value;
}

static void FieldIDs(const Json &type, set<int64_t> &ids) {
	if (!type.IsObject()) {
		return;
	}
	auto name = type.At("type").String();
	if (name == "struct") {
		for (auto &field : type.At("fields").Items()) {
			ids.insert(field.At("id").Integer());
			FieldIDs(field.At("type"), ids);
		}
	} else if (name == "list") {
		ids.insert(type.At("element-id").Integer());
		FieldIDs(type.At("element"), ids);
	} else if (name == "map") {
		ids.insert(type.At("key-id").Integer());
		ids.insert(type.At("value-id").Integer());
		FieldIDs(type.At("key"), ids);
		FieldIDs(type.At("value"), ids);
	}
}

static int64_t MaxFieldID(const Json &schema) {
	set<int64_t> ids;
	FieldIDs(schema, ids);
	return ids.empty() ? 0 : *ids.rbegin();
}

static int64_t MaxPartitionID(const Json &spec) {
	int64_t result = 999;
	for (auto &field : spec.At("fields").Items()) {
		result = MaxValue(result, field.At("field-id").Integer());
	}
	return result;
}

static void ValidateType(const Json &value, int64_t version) {
	if (value.IsString()) {
		auto base = StringUtil::Split(value.String(), '(')[0];
		if (version < 3 && (base == "unknown" || base == "variant" || base == "timestamp_ns" ||
		                    base == "timestamptz_ns" || base == "geometry" || base == "geography")) {
			Invalid("Type " + base + " requires format-version 3");
		}
		return;
	}
	auto type = value.At("type").String();
	if (type == "struct") {
		for (auto &field : value.At("fields").Items()) {
			ValidateType(field.At("type"), version);
			bool has_default = !field.Get("initial-default").IsNull() || !field.Get("write-default").IsNull();
			if (version < 3 && has_default) {
				Invalid("Non-null column defaults require format-version 3");
			}
			if (field.At("type").IsString() && has_default) {
				auto base = StringUtil::Split(field.At("type").String(), '(')[0];
				if (base == "unknown" || base == "variant" || base == "geometry" || base == "geography") {
					Invalid("Non-null defaults are not supported for " + base);
				}
			}
		}
	} else if (type == "list") {
		ValidateType(value.At("element"), version);
	} else if (type == "map") {
		ValidateType(value.At("key"), version);
		ValidateType(value.At("value"), version);
	}
}

static int64_t RowID(const Json &value) {
	auto result = value.Integer();
	if (result < 0) {
		Invalid("Row IDs must be nonnegative 64-bit integers");
	}
	return result;
}

static const Json &FindID(const Json &values, const string &key, int64_t identifier) {
	for (auto &value : values.Items()) {
		if (value.At(key).Integer() == identifier) {
			return value;
		}
	}
	Invalid("Unknown " + key + ": " + std::to_string(identifier));
}

Catalog::Catalog(FileSystem &fs_p, string root_p) : root(std::move(root_p)), fs(fs_p) {
	directory = fs.JoinPath(root, NewUUID());
	warehouse = fs.JoinPath(directory, "warehouse");
	fs.CreateDirectoriesRecursive(warehouse);
}

void Catalog::WriteFile(const string &path, const string &content, bool append) {
	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;
	flags |= append ? FileFlags::FILE_FLAGS_APPEND : FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
	auto handle = fs.OpenFile(path, flags);
	idx_t offset = 0;
	while (offset < content.size()) {
		auto written = handle->Write(const_cast<char *>(content.data() + offset), content.size() - offset);
		if (written <= 0) {
			throw IOException("Could not complete native catalog file '%s'", path);
		}
		offset += written;
	}
	handle->Close();
}

Json Catalog::GetNamespace(const Namespace &ns) const {
	auto entry = namespaces.find(ns);
	if (entry == namespaces.end()) {
		throw CatalogError(404, "NoSuchNamespaceException", "Namespace does not exist");
	}
	return Json::Object({{"namespace", NamespaceJSON(ns)}, {"properties", entry->second}});
}

Json Catalog::CreateNamespace(const Json &body) {
	auto ns = ParseNamespace(body.At("namespace"));
	if (namespaces.count(ns)) {
		throw CatalogError(409, "AlreadyExistsException", "Namespace already exists");
	}
	auto properties = body.Has("properties") ? Properties(body.At("properties")) : Json::Object();
	if (!properties.Has("location")) {
		properties["location"] = warehouse;
	}
	namespaces.emplace(ns, std::move(properties));
	return GetNamespace(ns);
}

Json Catalog::UpdateNamespace(const Namespace &ns, const Json &body) {
	auto properties = GetNamespace(ns).At("properties");
	auto updates = body.Has("updates") ? Properties(body.At("updates")) : Json::Object();
	auto removals = body.Has("removals") ? body.At("removals") : Json::Array();
	auto removed = Json::Array();
	auto missing = Json::Array();
	auto updated = Json::Array();
	set<string> seen;
	for (auto &value : removals.Items()) {
		auto key = value.String();
		if (!seen.insert(key).second) {
			Invalid("Property removals must be unique");
		}
		if (updates.Has(key)) {
			throw CatalogError(422, "UnprocessableEntityException",
			                   "A property cannot be updated and removed together");
		}
		(properties.Has(key) ? removed : missing).Push(key);
		properties.Erase(key);
	}
	for (auto &entry : updates.Members()) {
		properties[entry.first] = entry.second;
		updated.Push(entry.first);
	}
	namespaces.at(ns) = std::move(properties);
	return Json::Object({{"updated", updated}, {"removed", removed}, {"missing", missing}});
}

void Catalog::DropNamespace(const Namespace &ns) {
	GetNamespace(ns);
	for (auto &entry : tables) {
		if (entry.first.first == ns) {
			throw CatalogError(409, "NamespaceNotEmptyException", "Namespace is not empty");
		}
	}
	for (auto &entry : namespaces) {
		if (entry.first.size() > ns.size() && std::equal(ns.begin(), ns.end(), entry.first.begin())) {
			throw CatalogError(409, "NamespaceNotEmptyException", "Namespace is not empty");
		}
	}
	namespaces.erase(ns);
}

Json Catalog::Load(const Identifier &key) const {
	GetNamespace(key.first);
	auto entry = tables.find(key);
	if (entry == tables.end()) {
		throw CatalogError(404, "NoSuchTableException", "Table does not exist: " + key.second);
	}
	return entry->second;
}

void Catalog::Rename(const Json &body) {
	auto source = ParseIdentifier(body.At("source"));
	auto destination = ParseIdentifier(body.At("destination"));
	auto table = Load(source);
	GetNamespace(destination.first);
	if (tables.count(destination)) {
		throw CatalogError(409, "AlreadyExistsException", "Table already exists: " + destination.second);
	}
	tables.emplace(destination, std::move(table));
	tables.erase(source);
}

Json Catalog::Create(const Namespace &ns, const Json &body) {
	GetNamespace(ns);
	Identifier key {ns, body.At("name").String()};
	if (key.second.empty()) {
		Invalid("Table name must be a nonempty string");
	}
	if (tables.count(key)) {
		throw CatalogError(409, "AlreadyExistsException", "Table already exists");
	}
	auto properties = body.Has("properties") ? Properties(body.At("properties")) : Json::Object();
	int64_t version = 2;
	if (properties.Has("format-version")) {
		auto text = properties.At("format-version").String();
		if (text != "2" && text != "3") {
			Unsupported("The mock implements format-version 2 and 3");
		}
		version = text == "3" ? 3 : 2;
		properties.Erase("format-version");
	}
	if (!body.Get("location").IsNull() && !body.At("location").String().empty()) {
		Unsupported("Explicit table locations are outside the mock's temporary warehouse contract");
	}
	auto uuid = NewUUID();
	auto location = fs.JoinPath(warehouse, uuid);
	auto schema = body.At("schema");
	if (!schema.Has("schema-id")) {
		schema["schema-id"] = int64_t(0);
	}
	auto spec = body.Get("partition-spec").IsNull() ? Json::Object({{"spec-id", int64_t(0)}, {"fields", Json::Array()}})
	                                                : body.At("partition-spec");
	spec.Erase("type");
	if (!spec.Has("spec-id")) {
		spec["spec-id"] = int64_t(0);
	}
	auto order = body.Get("write-order").IsNull() ? Json::Object({{"order-id", int64_t(0)}, {"fields", Json::Array()}})
	                                              : body.At("write-order");
	if (!order.Has("order-id")) {
		order["order-id"] = int64_t(order.At("fields").Items().empty() ? 0 : 1);
	}
	auto metadata = Json::Object({{"format-version", version},
	                              {"table-uuid", uuid},
	                              {"location", location},
	                              {"last-updated-ms", NowMillis()},
	                              {"last-column-id", MaxFieldID(schema)},
	                              {"schemas", Json::Array({schema})},
	                              {"current-schema-id", schema.At("schema-id")},
	                              {"partition-specs", Json::Array({spec})},
	                              {"default-spec-id", spec.At("spec-id")},
	                              {"last-partition-id", MaxPartitionID(spec)},
	                              {"sort-orders", Json::Array({order})},
	                              {"default-sort-order-id", order.At("order-id")},
	                              {"properties", properties},
	                              {"snapshots", Json::Array()},
	                              {"refs", Json::Object()},
	                              {"last-sequence-number", int64_t(0)},
	                              {"snapshot-log", Json::Array()},
	                              {"metadata-log", Json::Array()}});
	if (version == 3) {
		metadata["next-row-id"] = int64_t(0);
	}
	Validate(metadata);
	fs.CreateDirectoriesRecursive(fs.JoinPath(location, "metadata"));
	if (body.Has("stage-create") && body.At("stage-create").Bool()) {
		staged.emplace(uuid, std::make_pair(key, metadata));
		return Json::Object({{"metadata", metadata}, {"config", Json::Object()}});
	}
	return Publish({{key, metadata}}).at(key);
}

void Catalog::Validate(const Json &metadata) const {
	auto version = metadata.At("format-version").Integer();
	for (auto &schema : metadata.At("schemas").Items()) {
		ValidateType(schema, version);
	}
	if (version == 3) {
		RowID(metadata.At("next-row-id"));
	}
	auto &schema = FindID(metadata.At("schemas"), "schema-id", metadata.At("current-schema-id").Integer());
	set<int64_t> fields;
	FieldIDs(schema, fields);
	for (auto &layout : {std::make_pair("partition-specs", std::make_pair("spec-id", "default-spec-id")),
	                     std::make_pair("sort-orders", std::make_pair("order-id", "default-sort-order-id"))}) {
		auto &current =
		    FindID(metadata.At(layout.first), layout.second.first, metadata.At(layout.second.second).Integer());
		for (auto &field : current.At("fields").Items()) {
			if (field.At("transform").String() != "void" && !fields.count(field.At("source-id").Integer())) {
				Invalid("Current layout references missing schema field");
			}
		}
	}
}

Json Catalog::WriteMetadata(const Identifier &key, Json metadata, int64_t timestamp) {
	metadata["last-updated-ms"] = timestamp;
	auto previous = tables.find(key);
	if (previous != tables.end()) {
		metadata.At("metadata-log")
		    .Push(Json::Object({{"metadata-file", previous->second.At("metadata-location")},
		                        {"timestamp-ms", previous->second.At("metadata").At("last-updated-ms")}}));
	}
	auto path = fs.JoinPath(metadata.At("location").String(), "metadata", NewUUID() + ".metadata.json");
	WriteFile(path, metadata.Dump());
	return Json::Object({{"metadata", metadata}, {"metadata-location", path}, {"config", Json::Object()}});
}

map<Identifier, Json> Catalog::Publish(const map<Identifier, Json> &candidates) {
	for (auto &entry : candidates) {
		Validate(entry.second);
	}
	auto timestamp = NowMillis();
	// Metadata and the client's transaction-start cutoff have millisecond precision.
	// A synchronous commit can otherwise appear to precede a transaction that began
	// before this request. Cross the request's starting tick using the real clock:
	// a logical counter alone would put fast commits in the future.
	if (timestamp == request_start_ms) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		timestamp = NowMillis();
	}
	if (timestamp <= request_start_ms) {
		throw IOException("System clock did not advance during native catalog publication");
	}
	map<Identifier, Json> results;
	for (auto &entry : candidates) {
		results.emplace(entry.first, WriteMetadata(entry.first, entry.second, timestamp));
	}
	// Prepare the entire replacement before changing any visible table pointer.
	auto published = tables;
	for (auto &entry : results) {
		published[entry.first] = entry.second;
	}
	tables.swap(published);
	for (auto &entry : candidates) {
		staged.erase(entry.second.At("table-uuid").String());
	}
	return results;
}

void Catalog::CheckRequirement(const Json &metadata, const Json &requirement) const {
	auto kind = requirement.At("type").String();
	bool valid = false;
	const map<string, std::pair<string, string>> fields {
	    {"assert-table-uuid", {"table-uuid", "uuid"}},
	    {"assert-current-schema-id", {"current-schema-id", "current-schema-id"}},
	    {"assert-last-assigned-field-id", {"last-column-id", "last-assigned-field-id"}},
	    {"assert-last-assigned-partition-id", {"last-partition-id", "last-assigned-partition-id"}},
	    {"assert-default-spec-id", {"default-spec-id", "default-spec-id"}},
	    {"assert-default-sort-order-id", {"default-sort-order-id", "default-sort-order-id"}}};
	if (kind == "assert-create") {
		valid = metadata.IsNull();
	} else if (kind == "assert-ref-snapshot-id") {
		if (!metadata.IsNull()) {
			auto &ref = metadata.At("refs").Get(requirement.At("ref").String());
			valid = ref.Get("snapshot-id") == requirement.Get("snapshot-id");
		}
	} else if (fields.count(kind)) {
		auto &field = fields.at(kind);
		valid = !metadata.IsNull() && metadata.At(field.first) == requirement.At(field.second);
	} else {
		Invalid("Unknown requirement: " + kind);
	}
	if (!valid) {
		throw CatalogError(409, "CommitFailedException", "Requirement failed: " + kind);
	}
}

Json Catalog::PrepareCommit(const Identifier &key, const Json &body) {
	GetNamespace(key.first);
	auto &requirements = body.At("requirements").Items();
	auto &updates = body.At("updates").Items();
	auto old = tables.find(key);
	Json candidate;
	if (old != tables.end()) {
		candidate = old->second.At("metadata");
	}
	bool assert_create = false;
	for (auto &requirement : requirements) {
		CheckRequirement(candidate, requirement);
		assert_create |= requirement.At("type").String() == "assert-create";
	}
	if (candidate.IsNull()) {
		if (!assert_create) {
			throw CatalogError(404, "NoSuchTableException", "Create commit requires assert-create");
		}
		vector<string> uuids;
		for (auto &update : updates) {
			if (update.At("action").String() == "assign-uuid") {
				uuids.push_back(update.At("uuid").String());
			}
		}
		if (uuids.size() != 1 || !staged.count(uuids[0])) {
			Invalid("Create commit must identify a staged UUID");
		}
		auto &stage = staged.at(uuids[0]);
		if (stage.first != key) {
			Invalid("Staged UUID belongs to another table");
		}
		candidate = stage.second;
	}
	map<string, int64_t> last_added;
	vector<int64_t> added_snapshots;
	for (auto &update : updates) {
		Apply(candidate, update, last_added, added_snapshots);
	}
	return candidate;
}

void Catalog::Apply(Json &metadata, const Json &update, map<string, int64_t> &last_added,
                    vector<int64_t> &added_snapshots) {
	auto action = update.At("action").String();
	if (action == "assign-uuid") {
		if (update.At("uuid") != metadata.At("table-uuid")) {
			Invalid("Cannot replace table UUID");
		}
	} else if (action == "upgrade-format-version") {
		auto version = update.At("format-version").Integer();
		if (version != 2 && version != 3) {
			Unsupported("The mock implements format-version 2 and 3");
		}
		auto old = metadata.At("format-version").Integer();
		if (version < old) {
			Invalid("Cannot downgrade a table's format-version");
		}
		if (old == 2 && version == 3) {
			metadata["next-row-id"] = int64_t(0);
		}
		metadata["format-version"] = version;
	} else if (action == "set-location") {
		if (update.At("location") != metadata.At("location")) {
			Unsupported("Relocating tables is not implemented");
		}
	} else if (action == "add-schema" || action == "add-spec" || action == "add-sort-order") {
		auto argument = action == "add-schema" ? "schema" : action == "add-spec" ? "spec" : "sort-order";
		auto collection = action == "add-schema" ? "schemas" : action == "add-spec" ? "partition-specs" : "sort-orders";
		auto id_key = action == "add-schema" ? "schema-id" : action == "add-spec" ? "spec-id" : "order-id";
		auto &item = update.At(argument);
		auto identifier = item.At(id_key).Integer();
		if (identifier < 0) {
			Invalid("Invalid metadata ID");
		}
		bool found = false;
		for (auto &existing : metadata.At(collection).Items()) {
			if (existing.At(id_key).Integer() == identifier) {
				if (existing != item) {
					Invalid("Conflicting " + string(id_key));
				}
				found = true;
			}
		}
		if (!found) {
			metadata.At(collection).Push(item);
		}
		last_added[collection] = identifier;
		if (action == "add-schema") {
			auto last_column = update.Has("last-column-id") ? update.At("last-column-id").Integer() : 0;
			metadata["last-column-id"] =
			    MaxValue(metadata.At("last-column-id").Integer(), MaxValue(MaxFieldID(item), last_column));
		} else if (action == "add-spec") {
			metadata["last-partition-id"] = MaxValue(metadata.At("last-partition-id").Integer(), MaxPartitionID(item));
		}
	} else if (action == "set-current-schema" || action == "set-default-spec" || action == "set-default-sort-order") {
		bool schema = action == "set-current-schema";
		bool spec = action == "set-default-spec";
		auto argument = schema ? "schema-id" : spec ? "spec-id" : "sort-order-id";
		auto collection = schema ? "schemas" : spec ? "partition-specs" : "sort-orders";
		auto target = schema ? "current-schema-id" : spec ? "default-spec-id" : "default-sort-order-id";
		auto id_key = schema ? "schema-id" : spec ? "spec-id" : "order-id";
		auto identifier = update.At(argument).Integer();
		if (identifier == -1) {
			if (!last_added.count(collection)) {
				Invalid("No last added " + string(collection) + " in this commit");
			}
			identifier = last_added.at(collection);
		}
		FindID(metadata.At(collection), id_key, identifier);
		metadata[target] = identifier;
	} else if (action == "set-properties") {
		if (update.At("updates").Has("format-version")) {
			Invalid("Use upgrade-format-version instead of a property update");
		}
		auto properties = Properties(update.At("updates"));
		for (auto &entry : properties.Members()) {
			metadata.At("properties")[entry.first] = entry.second;
		}
	} else if (action == "remove-properties") {
		for (auto &entry : update.At("removals").Items()) {
			metadata.At("properties").Erase(entry.String());
		}
	} else if (action == "add-snapshot") {
		auto snapshot = update.At("snapshot");
		auto identifier = snapshot.At("snapshot-id").Integer();
		for (auto &existing : metadata.At("snapshots").Items()) {
			if (existing.At("snapshot-id").Integer() == identifier) {
				Invalid("Snapshot ID already exists");
			}
		}
		if (snapshot.At("sequence-number").Integer() <= metadata.At("last-sequence-number").Integer()) {
			Invalid("Snapshot sequence number must increase");
		}
		if (!snapshot.Get("parent-snapshot-id").IsNull()) {
			FindID(metadata.At("snapshots"), "snapshot-id", snapshot.At("parent-snapshot-id").Integer());
		}
		FindID(metadata.At("schemas"), "schema-id", snapshot.At("schema-id").Integer());
		auto operation = snapshot.At("summary").At("operation").String();
		if (operation != "append" && operation != "replace" && operation != "overwrite" && operation != "delete") {
			Invalid("Unknown snapshot operation");
		}
		snapshot.At("manifest-list").String();
		snapshot.At("timestamp-ms").Integer();
		if (metadata.At("format-version").Integer() == 3) {
			auto first = RowID(snapshot.At("first-row-id"));
			auto added = RowID(snapshot.At("added-rows"));
			if (first < RowID(metadata.At("next-row-id"))) {
				throw CatalogError(409, "CommitFailedException", "Snapshot row IDs overlap a committed allocation");
			}
			if (added > NumericLimits<int64_t>::Maximum() - first) {
				Invalid("Row ID allocation exceeds signed 64-bit range");
			}
			metadata["next-row-id"] = first + added;
		} else if (snapshot.Has("first-row-id") || snapshot.Has("added-rows")) {
			Invalid("Snapshot row lineage requires format-version 3");
		}
		metadata["last-sequence-number"] = snapshot.At("sequence-number");
		metadata.At("snapshots").Push(std::move(snapshot));
		added_snapshots.push_back(identifier);
	} else if (action == "set-snapshot-ref") {
		auto identifier = update.At("snapshot-id").Integer();
		auto &snapshot = FindID(metadata.At("snapshots"), "snapshot-id", identifier);
		if (update.At("ref-name").String() != "main" || update.At("type").String() != "branch") {
			Unsupported("Only the main branch is implemented");
		}
		auto ref = update;
		ref.Erase("action");
		ref.Erase("ref-name");
		metadata.At("refs")["main"] = std::move(ref);
		if (metadata.Get("current-snapshot-id") != update.At("snapshot-id")) {
			// Publication waits for this tick before exposing the candidate.
			auto timestamp = MaxValue(NowMillis(), request_start_ms + 1);
			if (std::find(added_snapshots.begin(), added_snapshots.end(), identifier) != added_snapshots.end()) {
				timestamp = snapshot.At("timestamp-ms").Integer();
			}
			metadata.At("snapshot-log").Push(Json::Object({{"snapshot-id", identifier}, {"timestamp-ms", timestamp}}));
			metadata["current-snapshot-id"] = identifier;
		}
	} else {
		Invalid("Unknown update action: " + action);
	}
}

} // namespace iceberg_test
} // namespace duckdb
