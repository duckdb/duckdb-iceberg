#pragma once

#include "catalog/rest/iceberg_request_task.hpp"

#include <chrono>

namespace duckdb {

//! Schedules owned requests on one producer and drains them when this scope ends.
//! The caller must keep the context and catalog alive; publication remains the caller's responsibility.
class IcebergRequestExecutor {
public:
	IcebergRequestExecutor(ClientContext &context, IcebergCatalog &catalog)
	    : context(context), catalog(catalog), executor(context, TaskSchedulerType::ASYNC) {
	}

	template <class REQUEST>
	shared_ptr<IcebergRequestResult<typename REQUEST::Result>> Schedule(REQUEST request) {
		auto result = make_shared_ptr<IcebergRequestResult<typename REQUEST::Result>>();
		executor.ScheduleTask(
		    make_uniq<IcebergRequestTask<REQUEST>>(executor, context, catalog, std::move(request), result));
		return result;
	}

	//! Consume a result scheduled by this executor, leaving the caller available to refill its request window.
	//! Other requests and task cleanup may still be running when this returns.
	template <class RESULT>
	RESULT WaitAndTakeResult(IcebergRequestResult<RESULT> &result) {
		WaitUntilReady(result);
		return result.TakeResult();
	}

	//! Wait without consuming, so callers can discard responses superseded by transaction-local state.
	template <class RESULT>
	void WaitUntilReady(IcebergRequestResult<RESULT> &result) {
		auto &scheduler = TaskScheduler::GetScheduler(context);
		while (true) {
			context.InterruptCheck();
			if (executor.HasError()) {
				executor.ThrowError();
			}
			if (result.IsReady()) {
				return;
			}
			shared_ptr<Task> task;
			// With dedicated async workers, borrowing a request can block the caller after its awaited result
			// is ready, leaving completed window slots unfilled. Help only when no async workers are available.
			// Recheck on every iteration so reducing the pool to zero cannot strand queued requests.
			if (scheduler.NumberOfAsyncThreads() == 0 && executor.GetTask(task)) {
				const auto task_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
				D_ASSERT(task_result != TaskExecutionResult::TASK_NOT_FINISHED);
			} else {
				unique_lock<mutex> guard(result.lock);
				// Interruption does not notify this condition variable, so periodically check it on the caller.
				result.completion.wait_for(guard, std::chrono::milliseconds(50), [&]() { return result.ready; });
			}
		}
	}

	//! Executor failures abort the operation; individual request failures are handled by the caller.
	bool HasError() {
		return executor.HasError();
	}

	//! Join at the operation boundary and surface executor failures. Destruction cancels and drains on unwind.
	void Drain() {
		executor.WorkOnTasks();
		context.InterruptCheck();
	}

private:
	ClientContext &context;
	IcebergCatalog &catalog;
	TaskExecutor executor;
};

} // namespace duckdb
