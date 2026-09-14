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
constexpr const char *MAX_SNAPSHOT_AGE_PROPERTY = "history.expire.max-snapshot-age-ms";
constexpr const char *MIN_SNAPSHOTS_TO_KEEP_PROPERTY = "history.expire.min-snapshots-to-keep";

struct SnapshotRetentionPolicy {
	timestamp_ms_t cutoff;
	int64_t min_snapshots_to_keep;
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
	return result;
}

void ApplyMainRetention(const IcebergTableMetadata &metadata, int64_t main_snapshot_id,
                        const SnapshotRetentionPolicy &policy, unordered_set<int64_t> &to_expire) {
	int64_t depth = 0;
	bool retention_prefix_open = true;
	unordered_set<int64_t> seen;
	seen.reserve(metadata.snapshots.size());
	auto snapshot_id = main_snapshot_id;
	while (true) {
		const auto snapshot = metadata.FindSnapshotByIdInternal(snapshot_id);
		if (!snapshot) {
			break;
		}
		if (!seen.insert(snapshot_id).second) {
			throw InvalidConfigurationException("Cycle detected in snapshot ancestry at snapshot %lld", snapshot_id);
		}
		const bool retain =
		    retention_prefix_open && (depth < policy.min_snapshots_to_keep || snapshot->timestamp_ms >= policy.cutoff);
		if (retain) {
			to_expire.erase(snapshot_id);
		} else {
			//! Keep walking after the prefix closes so newer timestamps cannot reopen it
			//! and cycles in older ancestry are still detected.
			retention_prefix_open = false;
			to_expire.insert(snapshot_id);
		}
		depth++;
		if (!snapshot->parent_snapshot_id) {
			break;
		}
		//! A previous expiration may have removed the parent.
		snapshot_id = *snapshot->parent_snapshot_id;
	}
}

void ValidateMainOnlyMetadata(const IcebergTableMetadata &metadata) {
	if (metadata.unsupported_snapshot_ref) {
		throw NotImplementedException(
		    "iceberg_expire_snapshots currently supports only the main branch; found unsupported snapshot reference "
		    "\"%s\"",
		    *metadata.unsupported_snapshot_ref);
	}

	if (metadata.current_snapshot_id && !metadata.FindSnapshotByIdInternal(*metadata.current_snapshot_id)) {
		throw InvalidConfigurationException("current-snapshot-id points to unknown snapshot %lld",
		                                    *metadata.current_snapshot_id);
	}
	if (!metadata.main_snapshot_ref) {
		if (metadata.snapshot_refs_present && metadata.current_snapshot_id) {
			throw InvalidConfigurationException("Snapshot refs must contain 'main' when current-snapshot-id is set");
		}
		return;
	}

	const auto &main_ref = *metadata.main_snapshot_ref;
	if (main_ref.type != "branch") {
		throw InvalidConfigurationException("Snapshot ref 'main' must be a branch");
	}
	if (!metadata.current_snapshot_id) {
		throw InvalidConfigurationException("Snapshot ref 'main' requires current-snapshot-id");
	}
	if (main_ref.snapshot_id != *metadata.current_snapshot_id) {
		throw InvalidConfigurationException("Snapshot ref 'main' does not match current-snapshot-id");
	}
	if (main_ref.max_snapshot_age_ms && *main_ref.max_snapshot_age_ms < 1) {
		throw InvalidConfigurationException("Invalid max-snapshot-age-ms for snapshot ref 'main': expected a positive "
		                                    "integer");
	}
	if (main_ref.min_snapshots_to_keep && *main_ref.min_snapshots_to_keep < 1) {
		throw InvalidConfigurationException(
		    "Invalid min-snapshots-to-keep for snapshot ref 'main': expected a positive "
		    "integer");
	}
}

SnapshotRetentionPolicy ResolveMainPolicy(const IcebergTableMetadata &metadata,
                                          const SnapshotRetentionPolicy &global_policy, timestamp_ms_t now) {
	auto result = global_policy;
	if (!metadata.main_snapshot_ref) {
		return result;
	}

	const auto &main_ref = *metadata.main_snapshot_ref;
	if (main_ref.min_snapshots_to_keep) {
		result.min_snapshots_to_keep = *main_ref.min_snapshots_to_keep;
	}
	if (main_ref.max_snapshot_age_ms) {
		result.cutoff = SubtractAge(now, *main_ref.max_snapshot_age_ms);
	}
	return result;
}

void ApplyGlobalAgePolicy(const IcebergTableMetadata &metadata, timestamp_ms_t cutoff,
                          unordered_set<int64_t> &to_expire) {
	//! Apply the global age policy as if snapshots were unreferenced.
	//! Main ancestry retention below overrides this initial classification.
	for (const auto &entry : metadata.snapshots) {
		if (entry.second.timestamp_ms < cutoff) {
			to_expire.insert(entry.first);
		}
	}
}

void ApplyExplicitSnapshotIds(const IcebergTableMetadata &metadata, const vector<int64_t> &snapshot_ids,
                              unordered_set<int64_t> &to_expire) {
	//! Explicit IDs override age and count retention, but never current main.
	for (auto snapshot_id : snapshot_ids) {
		if (!metadata.FindSnapshotByIdInternal(snapshot_id)) {
			throw InvalidInputException("iceberg_expire_snapshots: snapshot %lld does not exist", snapshot_id);
		}
		if (metadata.current_snapshot_id && snapshot_id == *metadata.current_snapshot_id) {
			throw InvalidInputException("iceberg_expire_snapshots: cannot expire snapshot %lld because it is still "
			                            "referenced",
			                            snapshot_id);
		}
		to_expire.insert(snapshot_id);
	}
}

} // namespace

ExpireSnapshotsPlan PlanSnapshotExpiration(const IcebergTableMetadata &metadata, const ExpireSnapshotsPlanInput &input,
                                           timestamp_ms_t now) {
	ValidateMainOnlyMetadata(metadata);

	const auto global_policy = ResolveGlobalPolicy(metadata, input, now);
	const auto main_policy = ResolveMainPolicy(metadata, global_policy, now);

	unordered_set<int64_t> to_expire;
	to_expire.reserve(metadata.snapshots.size());
	ApplyGlobalAgePolicy(metadata, global_policy.cutoff, to_expire);
	if (metadata.current_snapshot_id) {
		//! Validation guarantees that an explicit refs.main targets current;
		//! otherwise current is the implicit main head.
		ApplyMainRetention(metadata, *metadata.current_snapshot_id, main_policy, to_expire);
	}
	ApplyExplicitSnapshotIds(metadata, input.snapshot_ids, to_expire);

	vector<int64_t> result_ids(to_expire.begin(), to_expire.end());
	std::sort(result_ids.begin(), result_ids.end());
	D_ASSERT(result_ids.size() <= metadata.snapshots.size());

	ExpireSnapshotsPlan result;
	result.retained_snapshots =
	    static_cast<int64_t>(metadata.snapshots.size()) - static_cast<int64_t>(result_ids.size());
	result.to_expire = std::move(result_ids);
	return result;
}

} // namespace duckdb
