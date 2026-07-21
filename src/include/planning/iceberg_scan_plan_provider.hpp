#pragma once

#include "planning/iceberg_multi_file_list.hpp"
#include "catalog/rest/api/iceberg_scan_planning.hpp"

namespace duckdb {

class IcebergScanPlanProvider {
public:
	virtual ~IcebergScanPlanProvider() = default;

	virtual bool IsServerSide() const = 0;
	virtual vector<IcebergManifestListEntry> &DataManifests() = 0;
	virtual vector<IcebergManifestListEntry> &DeleteManifests() = 0;
	virtual ManifestEntryReadState &ReadState() = 0;
	virtual bool &DataManifestScanStarted() = 0;
	virtual bool &DeleteEntriesEnumerated() = 0;
	virtual idx_t &NextDeleteEntryToProcess() = 0;
	virtual vector<BoundIcebergManifestEntry> &DeleteManifestEntries() = 0;
	virtual case_insensitive_map_t<shared_ptr<IcebergDeleteData>> &PositionalDeleteData() = 0;
	virtual map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> &EqualityDeleteData() = 0;
	virtual const case_insensitive_map_t<unordered_set<string>> &DeleteFilesByDataFile() const = 0;
};

class ClientSideScanPlanProvider final : public IcebergScanPlanProvider {
public:
	explicit ClientSideScanPlanProvider(IcebergMultiFileListSharedState &shared_state);

	bool IsServerSide() const override;
	vector<IcebergManifestListEntry> &DataManifests() override;
	vector<IcebergManifestListEntry> &DeleteManifests() override;
	ManifestEntryReadState &ReadState() override;
	bool &DataManifestScanStarted() override;
	bool &DeleteEntriesEnumerated() override;
	idx_t &NextDeleteEntryToProcess() override;
	vector<BoundIcebergManifestEntry> &DeleteManifestEntries() override;
	case_insensitive_map_t<shared_ptr<IcebergDeleteData>> &PositionalDeleteData() override;
	map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> &EqualityDeleteData() override;
	const case_insensitive_map_t<unordered_set<string>> &DeleteFilesByDataFile() const override;

private:
	IcebergMultiFileListSharedState &shared_state;
	case_insensitive_map_t<unordered_set<string>> empty_delete_files_by_data_file;
};

class ServerSideScanPlanProvider final : public IcebergScanPlanProvider {
public:
	explicit ServerSideScanPlanProvider(IcebergServerSideScanPlan plan);

	bool IsServerSide() const override;
	vector<IcebergManifestListEntry> &DataManifests() override;
	vector<IcebergManifestListEntry> &DeleteManifests() override;
	ManifestEntryReadState &ReadState() override;
	bool &DataManifestScanStarted() override;
	bool &DeleteEntriesEnumerated() override;
	idx_t &NextDeleteEntryToProcess() override;
	vector<BoundIcebergManifestEntry> &DeleteManifestEntries() override;
	case_insensitive_map_t<shared_ptr<IcebergDeleteData>> &PositionalDeleteData() override;
	map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> &EqualityDeleteData() override;
	const case_insensitive_map_t<unordered_set<string>> &DeleteFilesByDataFile() const override;

private:
	//! Declared before bound entries and parsed delete data so their references are destroyed first.
	IcebergServerSideScanPlan plan;
	ManifestEntryReadState read_state;
	bool data_manifest_scan_started = false;
	bool delete_entries_enumerated = false;
	idx_t next_delete_entry_to_process = 0;
	vector<BoundIcebergManifestEntry> delete_manifest_entries;
	case_insensitive_map_t<shared_ptr<IcebergDeleteData>> positional_delete_data;
	map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> equality_delete_data;
};

} // namespace duckdb
