#!/usr/bin/env bash
# Create and destroy a throwaway S3 Tables table bucket, used by the
# "Test S3 Tables Catalog" workflow to give every CI run its own catalog.
#
# Usage:
#   scripts/s3tables_table_bucket.sh create  <bucket-name>   # prints the ARN
#   scripts/s3tables_table_bucket.sh destroy <bucket-name>   # idempotent
#
# Both subcommands take the bucket *name* rather than the ARN so that teardown
# never depends on the create step having succeeded: the name is derived from
# the run id, so a run that dies halfway through creation can still clean up.
#
# Credentials/region come from the usual AWS_* environment variables.

set -euo pipefail

# Give up rather than loop forever if the catalog keeps handing back entries we
# cannot delete (permissions, a concurrent writer, ...).
readonly MAX_DRAIN_ROUNDS=10

usage() {
	cat >&2 <<-EOF
		usage: $(basename "$0") create|destroy <bucket-name>
	EOF
	exit 2
}

require_tools() {
	local tool
	for tool in aws jq; do
		if ! command -v "$tool" > /dev/null 2>&1; then
			echo "$(basename "$0"): required tool '$tool' is not installed" >&2
			exit 1
		fi
	done
}

# Prints the ARN of the table bucket with the given name, or nothing if no such
# bucket exists.
resolve_bucket_arn() {
	local name="$1"
	aws s3tables list-table-buckets --output json \
		| jq -r --arg name "$name" '.tableBuckets[]? | select(.name == $name) | .arn'
}

create_bucket() {
	local name="$1"
	aws s3tables create-table-bucket --name "$name" --output json | jq -r '.arn'
}

# S3 Tables refuses to delete a namespace that still holds tables, so empty it
# first. Individual failures are tolerated: the round loop re-lists and retries,
# and a genuinely undeletable table surfaces as a delete-namespace failure.
drain_namespace() {
	local arn="$1" ns="$2" round table
	local -a tables

	for ((round = 0; round < MAX_DRAIN_ROUNDS; round++)); do
		mapfile -t tables < <(
			aws s3tables list-tables --table-bucket-arn "$arn" --namespace "$ns" --output json \
				| jq -r '.tables[]?.name'
		)
		if ((${#tables[@]} == 0)); then
			return 0
		fi
		for table in "${tables[@]}"; do
			[[ -n $table ]] || continue
			echo "  dropping table ${ns}.${table}"
			aws s3tables delete-table --table-bucket-arn "$arn" --namespace "$ns" --name "$table" || true
		done
	done

	echo "  warning: namespace '$ns' still has tables after $MAX_DRAIN_ROUNDS rounds" >&2
	return 0
}

# Same story one level up: the bucket cannot be deleted while it has namespaces.
drain_bucket() {
	local arn="$1" round ns
	local -a namespaces

	for ((round = 0; round < MAX_DRAIN_ROUNDS; round++)); do
		mapfile -t namespaces < <(
			aws s3tables list-namespaces --table-bucket-arn "$arn" --output json \
				| jq -r '.namespaces[]?.namespace[0]'
		)
		if ((${#namespaces[@]} == 0)); then
			return 0
		fi
		for ns in "${namespaces[@]}"; do
			[[ -n $ns ]] || continue
			drain_namespace "$arn" "$ns"
			echo "  dropping namespace ${ns}"
			aws s3tables delete-namespace --table-bucket-arn "$arn" --namespace "$ns" || true
		done
	done

	echo "  warning: bucket still has namespaces after $MAX_DRAIN_ROUNDS rounds" >&2
	return 0
}

destroy_bucket() {
	local name="$1" arn

	arn="$(resolve_bucket_arn "$name")"
	if [[ -z $arn ]]; then
		echo "No table bucket named '$name' found, nothing to tear down."
		return 0
	fi

	echo "Tearing down table bucket $arn"
	drain_bucket "$arn"
	aws s3tables delete-table-bucket --table-bucket-arn "$arn"
	echo "Deleted table bucket $arn"
}

main() {
	[[ $# -eq 2 ]] || usage
	require_tools

	local command="$1" name="$2"
	case "$command" in
		create) create_bucket "$name" ;;
		destroy) destroy_bucket "$name" ;;
		*) usage ;;
	esac
}

main "$@"
