"""mitmproxy addon: deterministic fault injection for the Iceberg commit-retry tests.

It sits between DuckDB and the REST catalog and rewrites the *response* of a commit request so we can
exercise the optimistic-concurrency path that is otherwise impossible to trigger deterministically
from SQL:

  UNKNOWN-then-committed: the upstream catalog actually applies the commit, but we replace its
  successful response with a 502. DuckDB must NOT treat this as a hard failure, must NOT blindly
  retry, and must NOT clean up data files; its status-check has to re-load the table, find the
  committed snapshot, and resolve the commit as a success.

Because the rewrite happens in the `response` hook (after the upstream has answered), for a 2xx
commit the change is already durably applied on the catalog when we turn the response into a 502 --
that is exactly the "committed but the client could not tell" case.

The injection is keyed off the *table name* in the request path, so it only affects tables created by
the fault-injection test and leaves every other table/test untouched. Each rule fires a bounded
number of times (so the eventual real attempt/state-check succeeds).

Enable in CI with:  mitmdump --mode regular@<port> -s scripts/fault_injection_addon.py

Markers (substring of the table name in the commit path):
  inject_unknown_once  -> the first commit response for this table becomes 502 (upstream still
                          applied it); subsequent commits pass through unchanged. Exercises
                          UNKNOWN -> CheckCommitStatus -> ALL_COMMITTED (commit landed, response lost).
  inject_fail_before_apply_once -> the first commit POST for this table is answered with a 502 in the
                          REQUEST hook *without forwarding to the catalog*, so the commit provably did
                          NOT apply; subsequent commits pass through. Exercises
                          UNKNOWN -> CheckCommitStatus -> NONE_COMMITTED -> retry-to-success (a
                          transient 5xx that never reached/applied at the catalog).
"""

from mitmproxy import http

import logging

# marker -> max number of times to inject (per distinct request path)
_LIMITS = {
    "inject_unknown_once": 1,
    "inject_fail_before_apply_once": 1,
}

# How many times each (marker, path) has fired. Shared across the request and response hooks so each
# marker's budget is counted once regardless of which hook performs the injection.
_fired = {}

# 502 is classified UNKNOWN by the extension (ClassifyCommitStatus), which is the ambiguous-outcome
# path we want to exercise.
_INJECT_STATUS = 502


def _is_commit_request(flow: http.HTTPFlow) -> bool:
    # Iceberg REST commit endpoints: single-table update (POST .../tables/<name>) and the multi-table
    # transaction commit (POST .../transactions/commit).
    if flow.request.method != "POST":
        return False
    path = flow.request.path
    return ("/tables/" in path) or path.endswith("/transactions/commit")


def _marker_for(flow: http.HTTPFlow):
    # The table name appears in the request path for single-table commits, and in the body for the
    # multi-table transaction endpoint; check both.
    body = ""
    try:
        body = flow.request.get_text() or ""
    except Exception:
        body = ""
    haystack = flow.request.path + " " + body
    for marker in _LIMITS:
        if marker in haystack:
            return marker
    return None


def _inject_502(marker: str, path: str, hook: str) -> http.Response:
    _fired[(marker, path)] = _fired.get((marker, path), 0) + 1
    # Distinctive log line so the test can assert the fault was actually injected (otherwise a
    # mis-wired proxy / unmatched marker would let a normal response through and the test would pass
    # without ever exercising the targeted path).
    logging.warning("ICEBERG_FAULT_INJECTED marker=%s status=%d hook=%s path=%s", marker, _INJECT_STATUS, hook, path)
    return http.Response.make(
        _INJECT_STATUS,
        b'{"error": {"message": "injected ambiguous commit outcome", "type": "ServerError", "code": 502}}',
        {"Content-Type": "application/json"},
    )


def request(flow: http.HTTPFlow) -> None:
    # NONE_COMMITTED path: short-circuit the commit POST *before* it reaches the catalog, so the
    # commit provably never applies. Setting flow.response in the request hook prevents forwarding
    # upstream entirely. Only the inject_fail_before_apply_once marker uses this hook.
    if not _is_commit_request(flow):
        return
    marker = _marker_for(flow)
    if marker != "inject_fail_before_apply_once":
        return
    key = (marker, flow.request.path)
    if _fired.get(key, 0) >= _LIMITS[marker]:
        return  # budget exhausted: let the request reach the catalog and succeed
    flow.response = _inject_502(marker, flow.request.path, "request")


def response(flow: http.HTTPFlow) -> None:
    if not _is_commit_request(flow):
        return
    if flow.response is None or flow.response.status_code not in (200, 204):
        return  # only convert a *successful* commit into an ambiguous one
    marker = _marker_for(flow)
    if marker != "inject_unknown_once":
        return  # the before-apply marker is handled in the request hook
    key = (marker, flow.request.path)
    if _fired.get(key, 0) >= _LIMITS[marker]:
        return  # budget exhausted: let the real (successful) response through
    flow.response = _inject_502(marker, flow.request.path, "response")
