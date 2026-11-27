#include "maintenance/expire_snapshots_planner.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

#include <algorithm>
#include <stdexcept>

namespace duckdb {

namespace {

constexpr int64_t DEFAULT_MAX_SNAPSHOT_AGE_MS = 5LL * 24 * 60 * 60 * 1000;
constexpr int64_t DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 1;
constexpr int64_t DEFAULT_MAX_REF_AGE_MS = NumericLimits<int64_t>::Maximum();
constexpr const char *MAX_SNAPSHOT_AGE_PROPERTY = "history.expire.max-snapshot-age-ms";
constexpr const char *MIN_SNAPSHOTS_TO_KEEP_PROPERTY = "history.expire.min-snapshots-to-keep";
constexpr const char *MAX_REF_AGE_PROPERTY = "history.expire.max-ref-age-ms";

struct SnapshotRetentionPolicy {
	timestamp_ms_t cutoff;
	int64_t min_snapshots_to_keep;
	int64_t max_ref_age_ms;
};

//! A validated branch or tag, including the implicit main of metadata without refs.
struct SnapshotRef {
	string name;
	bool is_branch;
	int64_t snapshot_id;
	optional<int64_t> max_ref_age_ms;
	optional<int64_t> max_snapshot_age_ms;
	optional<int32_t> min_snapshots_to_keep;
};

int64_t GetBoundedIntegerTableProperty(const IcebergTableMetadata &metadata, const string &name, int64_t default_value,
                                       int64_t minimum_value, int64_t maximum_value) {
	const auto &properties = metadata.GetTableProperties();
	const auto entry = properties.find(name);
	if (entry == properties.end()) {
		return default_value;
	}

	const auto &value = entry->second;
	if (value.empty() || StringUtil::CharacterIsSpace(value.front())) {
		throw InvalidInputException("Invalid value '%s' for table property '%s': expected an integer", value, name);
	}

	int64_t result;
	size_t parsed_characters = 0;
	try {
		result = std::stoll(value, &parsed_characters);
	} catch (const std::invalid_argument &) {
		throw InvalidInputException("Invalid value '%s' for table property '%s': expected an integer", value, name);
	} catch (const std::out_of_range &) {
		throw InvalidInputException("Invalid value '%s' for table property '%s': expected an integer", value, name);
	}
	if (parsed_characters != value.size()) {
		throw InvalidInputException("Invalid value '%s' for table property '%s': expected an integer", value, name);
	}
	if (result < minimum_value) {
		throw InvalidInputException("Invalid value '%s' for table property '%s': expected a value >= %lld", value, name,
		                            minimum_value);
	}
	if (result > maximum_value) {
		throw InvalidInputException("Invalid value '%s' for table property '%s': expected a value <= %lld", value, name,
		                            maximum_value);
	}
	return result;
}

timestamp_ms_t SubtractAge(timestamp_ms_t now, int64_t age_ms) {
	D_ASSERT(age_ms >= 0);
	if (now.value < timestamp_ms_t::ninfinity().value + age_ms) {
		return timestamp_ms_t::ninfinity();
	}
	return timestamp_ms_t(now.value - age_ms);
}

constexpr int64_t CeilMicrosecondsToMilliseconds(int64_t microseconds) {
	return microseconds / Interval::MICROS_PER_MSEC + (microseconds % Interval::MICROS_PER_MSEC > 0 ? 1 : 0);
}

static_assert(CeilMicrosecondsToMilliseconds(-1001) == -1 && CeilMicrosecondsToMilliseconds(-1000) == -1 &&
                  CeilMicrosecondsToMilliseconds(-999) == 0 && CeilMicrosecondsToMilliseconds(999) == 1 &&
                  CeilMicrosecondsToMilliseconds(1000) == 1 && CeilMicrosecondsToMilliseconds(1001) == 2,
              "timestamp cutoffs must use mathematical ceil");

timestamp_ms_t GetExpirationCutoff(timestamp_t timestamp) {
	//! Snapshot timestamps have millisecond precision. Round the SQL timestamp up
	//! so `snapshot_time < older_than` stays strict for sub-millisecond inputs.
	return timestamp_ms_t(CeilMicrosecondsToMilliseconds(timestamp.value));
}

SnapshotRetentionPolicy ResolveGlobalPolicy(const IcebergTableMetadata &metadata, const ExpireSnapshotsPlanInput &input,
                                            timestamp_ms_t now) {
	SnapshotRetentionPolicy result;
	if (input.older_than) {
		result.cutoff = GetExpirationCutoff(*input.older_than);
	} else {
		const auto max_snapshot_age_ms = GetBoundedIntegerTableProperty(
		    metadata, MAX_SNAPSHOT_AGE_PROPERTY, DEFAULT_MAX_SNAPSHOT_AGE_MS, 0, NumericLimits<int64_t>::Maximum());
		result.cutoff = SubtractAge(now, max_snapshot_age_ms);
	}

	if (input.retain_last) {
		result.min_snapshots_to_keep = *input.retain_last;
	} else {
		result.min_snapshots_to_keep =
		    GetBoundedIntegerTableProperty(metadata, MIN_SNAPSHOTS_TO_KEEP_PROPERTY, DEFAULT_MIN_SNAPSHOTS_TO_KEEP, 1,
		                                   NumericLimits<int32_t>::Maximum());
	}
	result.max_ref_age_ms = GetBoundedIntegerTableProperty(metadata, MAX_REF_AGE_PROPERTY, DEFAULT_MAX_REF_AGE_MS, 0,
	                                                       NumericLimits<int64_t>::Maximum());
	return result;
}

//! Retain the branch's ancestors while the minimum count or the age rule keeps them; every visited
//! ancestor counts as referenced.
void RetainBranchAncestry(const IcebergTableMetadata &metadata, int64_t head_snapshot_id,
                          const SnapshotRetentionPolicy &policy, unordered_set<int64_t> &to_retain,
                          unordered_set<int64_t> &referenced) {
	int64_t depth = 0;
	bool retention_prefix_open = true;
	unordered_set<int64_t> seen;
	seen.reserve(metadata.snapshots.size());
	auto snapshot_id = head_snapshot_id;
	while (true) {
		const auto snapshot = metadata.FindSnapshotByIdInternal(snapshot_id);
		if (!snapshot) {
			break;
		}
		if (!seen.insert(snapshot_id).second) {
			throw InvalidConfigurationException("Cycle detected in snapshot ancestry at snapshot %lld", snapshot_id);
		}
		referenced.insert(snapshot_id);
		if (retention_prefix_open &&
		    (depth < policy.min_snapshots_to_keep || snapshot->timestamp_ms >= policy.cutoff)) {
			to_retain.insert(snapshot_id);
		} else {
			//! Keep walking after the prefix closes so newer timestamps cannot reopen it
			//! and cycles in older ancestry are still detected.
			retention_prefix_open = false;
		}
		depth++;
		if (!snapshot->parent_snapshot_id) {
			break;
		}
		//! A previous expiration may have removed the parent.
		snapshot_id = *snapshot->parent_snapshot_id;
	}
}

vector<SnapshotRef> ValidateSnapshotRefs(const IcebergTableMetadata &metadata) {
	if (metadata.current_snapshot_id && !metadata.FindSnapshotByIdInternal(*metadata.current_snapshot_id)) {
		throw InvalidConfigurationException("current-snapshot-id points to unknown snapshot %lld",
		                                    *metadata.current_snapshot_id);
	}

	vector<SnapshotRef> result;
	if (!metadata.refs) {
		//! Metadata without refs: the current snapshot is the implicit main branch.
		if (metadata.current_snapshot_id) {
			SnapshotRef main_ref;
			main_ref.name = "main";
			main_ref.is_branch = true;
			main_ref.snapshot_id = *metadata.current_snapshot_id;
			result.push_back(std::move(main_ref));
		}
		return result;
	}

	bool has_main = false;
	for (const auto &entry : metadata.refs->additional_properties) {
		const auto &name = entry.first;
		const auto &ref = entry.second;
		SnapshotRef resolved;
		resolved.name = name;
		resolved.snapshot_id = ref.snapshot_id;
		resolved.max_ref_age_ms = ref.max_ref_age_ms;
		resolved.max_snapshot_age_ms = ref.max_snapshot_age_ms;
		resolved.min_snapshots_to_keep = ref.min_snapshots_to_keep;
		if (ref.type == "branch") {
			resolved.is_branch = true;
		} else if (ref.type == "tag") {
			resolved.is_branch = false;
		} else {
			throw InvalidConfigurationException("Snapshot ref '%s' has unknown type '%s'", name, ref.type);
		}

		if (name == "main") {
			has_main = true;
			if (!resolved.is_branch) {
				throw InvalidConfigurationException("Snapshot ref 'main' must be a branch");
			}
			if (!metadata.current_snapshot_id) {
				throw InvalidConfigurationException("Snapshot ref 'main' requires current-snapshot-id");
			}
			if (ref.snapshot_id != *metadata.current_snapshot_id) {
				throw InvalidConfigurationException("Snapshot ref 'main' does not match current-snapshot-id");
			}
		}
		if (ref.max_ref_age_ms && *ref.max_ref_age_ms < 1) {
			throw InvalidConfigurationException(
			    "Invalid max-ref-age-ms for snapshot ref '%s': expected a positive integer", name);
		}
		if (ref.max_snapshot_age_ms && *ref.max_snapshot_age_ms < 1) {
			throw InvalidConfigurationException(
			    "Invalid max-snapshot-age-ms for snapshot ref '%s': expected a positive integer", name);
		}
		if (ref.min_snapshots_to_keep && *ref.min_snapshots_to_keep < 1) {
			throw InvalidConfigurationException(
			    "Invalid min-snapshots-to-keep for snapshot ref '%s': expected a positive integer", name);
		}
		result.push_back(std::move(resolved));
	}
	if (!has_main && metadata.current_snapshot_id) {
		throw InvalidConfigurationException("Snapshot refs must contain 'main' when current-snapshot-id is set");
	}
	//! The refs map has no order; keep plans and messages deterministic.
	std::sort(result.begin(), result.end(),
	          [](const SnapshotRef &lhs, const SnapshotRef &rhs) { return lhs.name < rhs.name; });
	return result;
}

//! Main is always retained. Another ref is removed when its snapshot is older than its
//! max-ref-age-ms (else the table property), or no longer exists.
void ComputeRetainedRefs(const IcebergTableMetadata &metadata, const vector<SnapshotRef> &refs,
                         const SnapshotRetentionPolicy &global_policy, timestamp_ms_t now,
                         vector<SnapshotRef> &retained, vector<string> &removed) {
	for (const auto &ref : refs) {
		if (ref.name == "main") {
			retained.push_back(ref);
			continue;
		}
		const auto snapshot = metadata.FindSnapshotByIdInternal(ref.snapshot_id);
		if (!snapshot) {
			removed.push_back(ref.name);
			continue;
		}
		const auto max_ref_age_ms = ref.max_ref_age_ms ? *ref.max_ref_age_ms : global_policy.max_ref_age_ms;
		if (snapshot->timestamp_ms >= SubtractAge(now, max_ref_age_ms)) {
			retained.push_back(ref);
		} else {
			removed.push_back(ref.name);
		}
	}
}

SnapshotRetentionPolicy ResolveBranchPolicy(const SnapshotRef &ref, const SnapshotRetentionPolicy &global_policy,
                                            timestamp_ms_t now) {
	auto result = global_policy;
	if (ref.min_snapshots_to_keep) {
		result.min_snapshots_to_keep = *ref.min_snapshots_to_keep;
	}
	if (ref.max_snapshot_age_ms) {
		result.cutoff = SubtractAge(now, *ref.max_snapshot_age_ms);
	}
	return result;
}

void ValidateExplicitSnapshotIds(const IcebergTableMetadata &metadata, const vector<int64_t> &snapshot_ids,
                                 const vector<SnapshotRef> &retained_refs) {
	//! Explicit IDs override age and count retention, but never the snapshot a retained ref points at.
	for (auto snapshot_id : snapshot_ids) {
		if (!metadata.FindSnapshotByIdInternal(snapshot_id)) {
			throw InvalidInputException("iceberg_expire_snapshots: snapshot %lld does not exist", snapshot_id);
		}
		vector<string> referencing_refs;
		for (const auto &ref : retained_refs) {
			if (ref.snapshot_id == snapshot_id) {
				referencing_refs.push_back(ref.name);
			}
		}
		if (!referencing_refs.empty()) {
			throw InvalidInputException("iceberg_expire_snapshots: cannot expire snapshot %lld because it is still "
			                            "referenced by %s",
			                            snapshot_id, StringUtil::Join(referencing_refs, ", "));
		}
	}
}

} // namespace

ExpireSnapshotsPlan PlanSnapshotExpiration(const IcebergTableMetadata &metadata, const ExpireSnapshotsPlanInput &input,
                                           timestamp_ms_t now) {
	const auto refs = ValidateSnapshotRefs(metadata);
	const auto global_policy = ResolveGlobalPolicy(metadata, input, now);

	ExpireSnapshotsPlan result;
	vector<SnapshotRef> retained_refs;
	ComputeRetainedRefs(metadata, refs, global_policy, now, retained_refs, result.refs_to_remove);
	ValidateExplicitSnapshotIds(metadata, input.snapshot_ids, retained_refs);

	//! Retain what the refs reach under their policies, then unreferenced snapshots that are
	//! still younger than the global cutoff.
	unordered_set<int64_t> to_retain;
	unordered_set<int64_t> referenced;
	to_retain.reserve(metadata.snapshots.size());
	referenced.reserve(metadata.snapshots.size());
	for (const auto &ref : retained_refs) {
		if (ref.is_branch) {
			RetainBranchAncestry(metadata, ref.snapshot_id, ResolveBranchPolicy(ref, global_policy, now), to_retain,
			                     referenced);
		} else {
			to_retain.insert(ref.snapshot_id);
			referenced.insert(ref.snapshot_id);
		}
	}
	for (const auto &entry : metadata.snapshots) {
		if (!referenced.count(entry.first) && entry.second.timestamp_ms >= global_policy.cutoff) {
			to_retain.insert(entry.first);
		}
	}

	unordered_set<int64_t> to_expire;
	for (const auto &entry : metadata.snapshots) {
		if (!to_retain.count(entry.first)) {
			to_expire.insert(entry.first);
		}
	}
	for (auto snapshot_id : input.snapshot_ids) {
		to_expire.insert(snapshot_id);
	}

	vector<int64_t> result_ids(to_expire.begin(), to_expire.end());
	std::sort(result_ids.begin(), result_ids.end());
	D_ASSERT(result_ids.size() <= metadata.snapshots.size());

	result.retained_snapshots =
	    static_cast<int64_t>(metadata.snapshots.size()) - static_cast<int64_t>(result_ids.size());
	result.to_expire = std::move(result_ids);
	return result;
}

} // namespace duckdb
