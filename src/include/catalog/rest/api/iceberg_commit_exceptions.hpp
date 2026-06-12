#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/http_status_code.hpp"

#include <string>

namespace duckdb {

//! Classification of a failed commit request, used to drive the retry loop (design doc B4/B13).
enum class IcebergCommitOutcome : uint8_t {
	//! Optimistic-concurrency conflict (HTTP 409). The table moved under us; refresh and retry.
	CONFLICT,
	//! We cannot tell whether the commit landed (timeout / connection reset / 5xx). Must NOT clean
	//! up files; resolve via a status-check before deciding success/failure.
	UNKNOWN,
	//! Definite, non-retryable failure (e.g. 4xx other than 409, validation error).
	FATAL
};

//! Map an HTTP status code to a commit outcome. Network-level failures that never produced a status
//! code should be treated as UNKNOWN by the caller before reaching here.
inline IcebergCommitOutcome ClassifyCommitStatus(HTTPStatusCode status) {
	switch (status) {
	case HTTPStatusCode::Conflict_409:
		return IcebergCommitOutcome::CONFLICT;
	case HTTPStatusCode::TooManyRequests_429:
		//! Rate-limited: the server rejected the request before applying it (the commit definitely did
		//! NOT land), so it is safe to retry directly like a conflict -- no status-check round-trip.
		//! Treating it as UNKNOWN would add a GetTable call per 429, which under heavy concurrency (the
		//! exact case that produces 429s) only worsens the rate limiting. The backoff+jitter before the
		//! next attempt is what relieves the pressure. Mirrors Java's REST client (429 = retryable).
		return IcebergCommitOutcome::CONFLICT;
	case HTTPStatusCode::RequestTimeout_408:
	case HTTPStatusCode::InternalServerError_500:
	case HTTPStatusCode::BadGateway_502:
	case HTTPStatusCode::ServiceUnavailable_503:
	case HTTPStatusCode::GatewayTimeout_504:
		//! Genuinely ambiguous: the request may have reached the server and been applied before the
		//! failure surfaced. Retrying blindly could double-apply (e.g. append the same batch twice),
		//! and cleaning up could delete a committed snapshot's files. Resolve via a status-check; never
		//! clean up. (503 stays here: a gateway can return it after the backend already committed.)
		return IcebergCommitOutcome::UNKNOWN;
	default:
		return IcebergCommitOutcome::FATAL;
	}
}

//! Exception thrown by the commit functions carrying its retry classification. The retry driver
//! catches this and decides whether to retry (CONFLICT), status-check (UNKNOWN), or abort (FATAL).
class IcebergCommitException : public Exception {
public:
	IcebergCommitException(IcebergCommitOutcome outcome, const string &msg)
	    : Exception(ExceptionType::TRANSACTION, msg), outcome(outcome) {
	}
	IcebergCommitException(IcebergCommitOutcome outcome, const string &msg, int64_t retry_after_ms)
	    : Exception(ExceptionType::TRANSACTION, msg), outcome(outcome), retry_after_ms(retry_after_ms) {
	}

	IcebergCommitOutcome outcome;
	//! Server-requested wait before retrying, in ms, parsed from a `Retry-After` response header.
	//! -1 means the server gave no guidance (use the client's exponential backoff). When set, the
	//! retry loop honors it (capped to the configured max wait) so a rate-limited server (e.g. S3
	//! Tables under a herd of writers) is not hammered earlier than it asked for.
	int64_t retry_after_ms = -1;
};

//! Parse a `Retry-After` header value into milliseconds. The HTTP spec allows either a number of
//! seconds ("120") or an HTTP-date; we support the numeric form (what catalogs/load balancers send)
//! and return -1 for an absent/unparseable/date value (caller falls back to exponential backoff).
inline int64_t ParseRetryAfterMs(const string &value) {
	if (value.empty()) {
		return -1;
	}
	try {
		size_t consumed = 0;
		auto seconds = std::stoll(value, &consumed);
		//! Require the whole value to be the integer (reject "Wed, 21 Oct..." HTTP-date form).
		if (consumed != value.size() || seconds < 0) {
			return -1;
		}
		//! Guard the *1000 against int64 overflow: Retry-After is untrusted server input, and a huge
		//! value would wrap to a negative/tiny wait. Clamp to a day in ms (far above any sane
		//! max-wait, which the retry loop caps it to anyway) instead of overflowing.
		constexpr int64_t MAX_RETRY_AFTER_MS = 24LL * 60 * 60 * 1000;
		if (seconds > MAX_RETRY_AFTER_MS / 1000) {
			return MAX_RETRY_AFTER_MS;
		}
		return seconds * 1000;
	} catch (...) {
		return -1;
	}
}

} // namespace duckdb
