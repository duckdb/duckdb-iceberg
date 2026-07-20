#!/usr/bin/env bash

active_catalog_test_config() {
	local script_dir repo_root active_catalog_file active_catalog config_path

	script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
	repo_root="$(cd -- "$script_dir/.." && pwd)"
	active_catalog_file="$repo_root/.catalogs/.active_catalog"

	if [[ ! -f "$active_catalog_file" ]]; then
		echo "No active catalog is set. Expected '$active_catalog_file' to exist. Start a catalog via the make targets first." >&2
		return 1
	fi

	active_catalog="$(<"$active_catalog_file")"
	active_catalog="${active_catalog//$'\r'/}"
	active_catalog="${active_catalog//$'\n'/}"

	if [[ -z "$active_catalog" ]]; then
		echo "The active catalog file '$active_catalog_file' is empty." >&2
		return 1
	fi

	case "$active_catalog" in
		fixture|fixture-latest|gravitino|lakekeeper|polaris|nessie)
			config_path="$repo_root/test/configs/${active_catalog}.json"
			;;
		*)
			echo "Active catalog '$active_catalog' does not have a REST catalog unittest config." >&2
			return 1
			;;
	esac

	if [[ ! -f "$config_path" ]]; then
		echo "Resolved test config '$config_path' does not exist for active catalog '$active_catalog'." >&2
		return 1
	fi

	printf '%s\n' "$config_path"
}
