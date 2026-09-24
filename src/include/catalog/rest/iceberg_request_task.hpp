#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <condition_variable>

namespace duckdb {

class IcebergCatalog;

//! One task publishes this result and one caller consumes it. Completion is independent of executor draining.
//! The task retains the result holder. The execution context and catalog must outlive the executor's tasks.
template <class RESULT>
class IcebergRequestResult {
public:
	bool IsReady() const {
		lock_guard<mutex> guard(lock);
		return ready;
	}

	RESULT TakeResult() {
		lock_guard<mutex> guard(lock);
		if (!ready) {
			throw InternalException("Iceberg request result is not ready");
		}
		if (error.HasError()) {
			error.Throw();
		}
		if (!result) {
			throw InternalException("Iceberg request result was already consumed");
		}
		auto value = std::move(*result);
		result.reset();
		return value;
	}

private:
	friend class IcebergRequestExecutor;
	template <class REQUEST>
	friend class IcebergRequestTask;

	void SetResult(RESULT value) {
		lock_guard<mutex> guard(lock);
		D_ASSERT(!ready);
		result.emplace(std::move(value));
		ready = true;
		completion.notify_all();
	}

	void SetError(ErrorData value) {
		lock_guard<mutex> guard(lock);
		D_ASSERT(!ready);
		error = std::move(value);
		ready = true;
		completion.notify_all();
	}

	mutable mutex lock;
	std::condition_variable completion;
	bool ready = false;
	//! An engaged outer optional can contain an empty RESULT optional (a successfully refused listing).
	optional<RESULT> result;
	ErrorData error;
};

//! Executes an owned request without retaining catalog entries or publishing its response.
//! Scheduling, draining, and handling request failures remain the caller's responsibility.
template <class REQUEST>
class IcebergRequestTask : public BaseExecutorTask {
public:
	using Result = typename REQUEST::Result;

	IcebergRequestTask(TaskExecutor &executor, ClientContext &context, IcebergCatalog &catalog, REQUEST request,
	                   shared_ptr<IcebergRequestResult<Result>> result)
	    : BaseExecutorTask(executor), context(context), catalog(catalog), request(std::move(request)),
	      result(std::move(result)) {
	}

	void ExecuteTask() override {
		if (context.IsInterrupted()) {
			result->SetError(ErrorData(InterruptException()));
			throw InterruptException();
		}
		try {
			result->SetResult(request.Execute(context, catalog));
		} catch (std::exception &ex) {
			// Let the caller decide whether a request failure aborts the operation or is only a warning.
			result->SetError(ErrorData(ex));
		} catch (...) {
			result->SetError(ErrorData("Unknown exception while executing an Iceberg catalog request"));
			throw;
		}
	}

	void Cancel() override {
		result->SetError(ErrorData(ExceptionType::INTERRUPT, "Iceberg catalog request was cancelled"));
	}

	string TaskType() const override {
		return "IcebergRequestTask";
	}

private:
	ClientContext &context;
	IcebergCatalog &catalog;
	REQUEST request;
	shared_ptr<IcebergRequestResult<Result>> result;
};

} // namespace duckdb
