#!/usr/bin/env bash
# Setup for the credential-refresh SigV4 test.
#
# Starts the combined Iceberg-REST-catalog + STS mock server and exports the
# environment the DuckDB aws extension needs to resolve `sts`-chain credentials
# against the mock (instead of real AWS STS).
#
# The test uses CHAIN 'sts' (not 'web_identity') because the sts chain builds a
# regular Aws::STS::STSClient, which honors AWS_ENDPOINT_URL_STS and uses the
# endpoint's http scheme. The web_identity provider is CRT-based and hardcodes
# https://sts.<region>.amazonaws.com, so it can't be pointed at a local mock.
#
# Usage:
#   source scripts/credential_refresh_test_setup.sh [port]
#   CREDENTIAL_REFRESH_MOCK_AVAILABLE=1 build/release/test/unittest \
#       test/sql/local/catalog_custom_setup/sigv4_mock/test_credential_refresh.test
#   # when done:
#   kill "$CREDENTIAL_REFRESH_MOCK_PID"
#
# NOTE: this script is meant to be `source`d, so it deliberately does NOT use
# `set -euo pipefail` — those options would leak into the caller's interactive
# shell and cause it to exit on the first non-zero command (e.g. the test's
# exit code), closing the terminal.

PORT="${1:-19140}"
# Resolve the repo's scripts/ dir without relying on ${BASH_SOURCE} — this
# script is sourced, and under zsh (1-indexed arrays) ${BASH_SOURCE[0]} is empty.
# git rev-parse works in both bash and zsh as long as we're inside the repo.
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
SCRIPT_DIR="${REPO_ROOT}/scripts"

python3 "${SCRIPT_DIR}/sigv4_mock_server.py" "${PORT}" &
MOCK_PID=$!
sleep 1

export CREDENTIAL_REFRESH_MOCK_AVAILABLE=1
export CREDENTIAL_REFRESH_MOCK_PORT="${PORT}"
export CREDENTIAL_REFRESH_MOCK_PID="${MOCK_PID}"
# Point the STS client at the mock endpoint (http scheme is honored from the URL).
export AWS_ENDPOINT_URL_STS="http://127.0.0.1:${PORT}"
# Base credentials the STSClient uses to sign the AssumeRole call. The mock does
# not validate the signature, so any non-empty values work.
export AWS_ACCESS_KEY_ID="mock-base-key"
export AWS_SECRET_ACCESS_KEY="mock-base-secret"
export AWS_REGION="us-east-1"

echo "Mock credential-refresh server running on http://127.0.0.1:${PORT} (pid ${MOCK_PID})"
echo "Env exported: CREDENTIAL_REFRESH_MOCK_AVAILABLE, AWS_ENDPOINT_URL_STS, AWS_ACCESS_KEY_ID, AWS_REGION"
