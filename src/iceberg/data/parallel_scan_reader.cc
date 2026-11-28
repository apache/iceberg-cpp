/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "iceberg/data/parallel_scan_reader.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <format>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/data/arrow_batch_sizer_internal.h"
#include "iceberg/data/parallel_scan_reader_internal.h"
#include "iceberg/expression/binder.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/executor.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace internal {

ParallelScanMetricsSnapshot ParallelScanMetrics::Snapshot() const {
  std::lock_guard lock(mutex_);
  return snapshot_;
}

namespace {

// Names a task in error messages, e.g. "data file s3://b/f.parquet [0, 1024)".
std::string DescribeTask(const FileScanTask& task) {
  const auto& data_file = task.data_file();
  if (data_file == nullptr) {
    return "task without a data file";
  }
  if (!task.is_split()) {
    return std::format("data file {}", data_file->file_path);
  }
  return std::format("data file {} [{}, {})", data_file->file_path, task.start(),
                     task.start() + task.length());
}

Error WithContext(const Error& error, std::string_view context) {
  return Error{.kind = error.kind,
               .message = std::format("{}: {}", context, error.message)};
}

std::string_view NullSafe(const char* value) {
  return value == nullptr ? std::string_view{} : std::string_view{value};
}

// Compares the physical layout of two Arrow schemas. Field names, formats,
// nullability, children, and dictionaries must match; metadata is ignored and the
// root name is not compared.
Status CheckSameLayout(const ArrowSchema& expected, const ArrowSchema& actual,
                       const std::string& path, bool compare_name) {
  if (NullSafe(expected.format) != NullSafe(actual.format)) {
    return InvalidSchema("Field {} has Arrow format '{}', expected '{}'", path,
                         NullSafe(actual.format), NullSafe(expected.format));
  }
  if (compare_name && NullSafe(expected.name) != NullSafe(actual.name)) {
    return InvalidSchema("Field {} is named '{}', expected '{}'", path,
                         NullSafe(actual.name), NullSafe(expected.name));
  }
  if ((expected.flags & ARROW_FLAG_NULLABLE) != (actual.flags & ARROW_FLAG_NULLABLE)) {
    return InvalidSchema("Field {} nullability differs from the projected schema", path);
  }
  if (expected.n_children != actual.n_children) {
    return InvalidSchema("Field {} has {} children, expected {}", path, actual.n_children,
                         expected.n_children);
  }
  for (int64_t index = 0; index < expected.n_children; ++index) {
    const ArrowSchema* expected_child = expected.children[index];
    const ArrowSchema* actual_child = actual.children[index];
    if (actual_child == nullptr || expected_child == nullptr) {
      return InvalidSchema("Field {} has a null child", path);
    }
    std::string child_path = std::format("{}.{}", path, NullSafe(expected_child->name));
    ICEBERG_RETURN_UNEXPECTED(
        CheckSameLayout(*expected_child, *actual_child, child_path, true));
  }
  if ((expected.dictionary == nullptr) != (actual.dictionary == nullptr)) {
    return InvalidSchema("Field {} dictionary encoding differs from the projected schema",
                         path);
  }
  if (expected.dictionary != nullptr) {
    ICEBERG_RETURN_UNEXPECTED(CheckSameLayout(*expected.dictionary, *actual.dictionary,
                                              path + ".dictionary", false));
  }
  return {};
}

void ReleaseArray(ArrowArray* array) {
  if (array->release != nullptr) {
    array->release(array);
  }
}

struct QueuedBatch {
  ArrowArray array;
  int64_t charge;
};

void ReleaseAll(std::deque<QueuedBatch>& batches) {
  for (auto& batch : batches) {
    ReleaseArray(&batch.array);
  }
  batches.clear();
}

// Retain unbound predicates unless table-schema binding folds them to constants.
class ResidualSimplifier : public Binder {
 public:
  ResidualSimplifier(const iceberg::Schema& schema, bool case_sensitive)
      : Binder(schema, case_sensitive),
        schema_(schema),
        case_sensitive_(case_sensitive) {}

  using Binder::Predicate;
  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override {
    ICEBERG_ASSIGN_OR_RAISE(auto bound, pred->Bind(schema_, case_sensitive_));
    if (bound->op() == Expression::Operation::kTrue ||
        bound->op() == Expression::Operation::kFalse) {
      return bound;
    }
    return pred;
  }

 private:
  const iceberg::Schema& schema_;
  bool case_sensitive_;
};

// A worker keeps its task stream and at most one completed batch between executor
// submissions. Queue backpressure and the planning token park workers without
// occupying executor threads. All scheduling state is guarded by mutex_. Reader,
// planning, release, and Executor::Submit callbacks run outside that lock.
class ParallelScan : public std::enable_shared_from_this<ParallelScan> {
 public:
  ParallelScan(FileScanTaskGroupStreamPtr groups, ArrowSchema output_schema,
               const ParallelScanReader::Options& options,
               std::shared_ptr<ParallelScanMetrics> metrics)
      : groups_(std::move(groups)),
        output_schema_(output_schema),
        executor_(*options.executor),
        table_schema_(options.reader_options.table_schema),
        case_sensitive_(options.reader_options.case_sensitive),
        max_queued_batches_(options.max_queued_batches),
        max_queued_bytes_(options.max_queued_bytes),
        metrics_(std::move(metrics)) {}

  ~ParallelScan() {
    Shutdown();
    if (output_schema_.release != nullptr) {
      output_schema_.release(&output_schema_);
    }
  }

  ParallelScan(const ParallelScan&) = delete;
  ParallelScan& operator=(const ParallelScan&) = delete;

  Status Start(const FileScanTaskReader::Options& options, int32_t worker_count) {
    workers_.reserve(static_cast<size_t>(worker_count));
    for (int32_t index = 0; index < worker_count; ++index) {
      auto worker = std::make_unique<Worker>();
      ICEBERG_ASSIGN_OR_RAISE(worker->reader, FileScanTaskReader::Make(options));
      ICEBERG_ASSIGN_OR_RAISE(worker->sizer, ArrowBatchSizer::Make(output_schema_));
      workers_.push_back(std::move(worker));
    }
    active_workers_ = worker_count;
    return ScheduleReady();
  }

  Result<std::optional<ArrowArray>> Next() {
    std::unique_lock lock(mutex_);
    consumer_cv_.wait(lock, [this] {
      return error_.has_value() || !queue_.empty() || active_workers_ == 0;
    });
    if (error_.has_value()) {
      return unexpected(*error_);
    }
    if (queue_.empty()) {
      return std::nullopt;
    }
    QueuedBatch batch = queue_.front();
    queue_.pop_front();
    queued_bytes_ -= batch.charge;
    WakeWorkers(WorkerState::kWaitingAdmission);
    lock.unlock();
    // Keep the popped batch owned if submitting a continuation throws.
    ArrowArrayGuard guard(&batch.array);
    (void)ScheduleReady();
    guard.Release();
    return batch.array;
  }

  Result<ArrowSchema> Schema() const {
    ArrowSchema schema{};
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowSchemaDeepCopy(&output_schema_, &schema));
    return schema;
  }

  void Shutdown() {
    std::deque<QueuedBatch> drained;
    std::vector<std::unique_ptr<Worker>> workers;
    {
      std::lock_guard lock(mutex_);
      stopped_ = true;
      drained.swap(queue_);
      queued_bytes_ = 0;
    }
    ReleaseAll(drained);
    {
      std::unique_lock lock(mutex_);
      idle_cv_.wait(lock, [this] { return running_workers_ == 0 && !dispatching_; });
      workers.swap(workers_);
    }
    // Queued executor callbacks retain the scan, but only check stopped_ and return.
    // Release all actual resources now, including parked workers' pending batches.
    workers.clear();
    groups_.reset();
  }

 private:
  enum class WorkerState {
    kReady,
    kScheduled,
    kRunning,
    kWaitingPlanning,
    kWaitingAdmission,
    kDone,
  };

  struct Worker {
    ~Worker() { Reset(); }

    void CloseStream() {
      auto old = std::exchange(stream, ArrowArrayStream{});
      if (old.release != nullptr) {
        old.release(&old);
      }
    }

    void Reset() {
      CloseStream();
      ReleaseArray(&pending);
      group.reset();
    }

    WorkerState state = WorkerState::kReady;
    bool running = false;
    std::unique_ptr<FileScanTaskReader> reader;
    std::unique_ptr<ArrowBatchSizer> sizer;
    std::shared_ptr<FileScanTaskGroup> group;
    size_t task_index = 0;
    ArrowArrayStream stream{};
    ArrowArray pending{};
    int64_t charge = 0;
    uint64_t ticket = 0;
    std::chrono::steady_clock::time_point wait_start;
    std::string context;
  };

  // Serializes Submit calls, and acts as a trampoline for inline executors.
  Status ScheduleReady() {
    {
      std::lock_guard lock(mutex_);
      if (stopped_ || dispatching_) {
        return {};
      }
      dispatching_ = true;
    }
    Status status;
    while (true) {
      size_t index = 0;
      {
        std::lock_guard lock(mutex_);
        if (stopped_) {
          dispatching_ = false;
          idle_cv_.notify_all();
          return status;
        }
        for (; index < workers_.size(); ++index) {
          if (workers_[index]->state == WorkerState::kReady &&
              !workers_[index]->running) {
            break;
          }
        }
        if (index == workers_.size()) {
          dispatching_ = false;
          idle_cv_.notify_all();
          return status;
        }
        workers_[index]->state = WorkerState::kScheduled;
      }
      try {
        status = executor_.Submit(ExecutorTask(
            [self = shared_from_this(), index] { self->WorkerMain(index); }));
      } catch (const std::exception& e) {
        status = UnknownError("Failed to submit parallel scan worker: {}", e.what());
      } catch (...) {
        status = UnknownError("Failed to submit parallel scan worker");
      }
      if (!status.has_value()) {
        Fail(WithContext(status.error(), "Failed to submit parallel scan worker"));
        break;
      }
    }
    {
      std::lock_guard lock(mutex_);
      dispatching_ = false;
    }
    idle_cv_.notify_all();
    return status;
  }

  void WorkerMain(size_t index) {
    Worker* worker;
    {
      std::lock_guard lock(mutex_);
      if (stopped_) {
        return;
      }
      worker = workers_[index].get();
      worker->state = WorkerState::kRunning;
      worker->running = true;
      ++running_workers_;
    }
    Status status;
    try {
      status = RunWorker(*worker);
    } catch (const std::exception& e) {
      status = UnknownError("Parallel scan worker failed for {}: {}", worker->context,
                            e.what());
    } catch (...) {
      status = UnknownError("Parallel scan worker failed for {}", worker->context);
    }
    if (!status.has_value()) {
      Fail(status.error());
    }
    bool finished;
    {
      std::lock_guard lock(mutex_);
      finished = stopped_ || worker->state == WorkerState::kRunning;
      if (finished) {
        if (worker->pending.release != nullptr) {
          held_bytes_ -= worker->charge;
          RecordWait(*worker);
        }
        worker->state = WorkerState::kDone;
        --active_workers_;
      }
    }
    if (finished) {
      worker->Reset();
    }
    {
      std::lock_guard lock(mutex_);
      worker->running = false;
    }
    (void)ScheduleReady();
    {
      std::lock_guard lock(mutex_);
      --running_workers_;
    }
    idle_cv_.notify_all();
    consumer_cv_.notify_all();
  }

  Status RunWorker(Worker& worker) {
    while (!IsStopped()) {
      if (worker.pending.release != nullptr) {
        if (!TryEnqueue(worker)) {
          return {};
        }
      } else if (worker.stream.release != nullptr) {
        // Check cancellation before every callback, including after get_schema.
        ArrowArray batch{};
        ArrowArrayGuard guard(&batch);
        if (int code = worker.stream.get_next(&worker.stream, &batch); code != 0) {
          return IOError("Failed to read {} (code {}): {}", worker.context, code,
                         LastError(worker.stream));
        }
        if (batch.release == nullptr) {
          worker.CloseStream();
          ++worker.task_index;
          continue;
        }
        auto charge = worker.sizer->Measure(batch);
        if (!charge.has_value()) {
          return unexpected(
              WithContext(charge.error(), "Failed to size a batch of " + worker.context));
        }
        worker.pending = batch;
        guard.Release();
        worker.charge = *charge;
        worker.wait_start = std::chrono::steady_clock::now();
        std::lock_guard lock(mutex_);
        worker.ticket = next_ticket_++;
        held_bytes_ += *charge;
        RecordMetrics([this](auto& snapshot) {
          snapshot.peak_held_bytes = std::max(snapshot.peak_held_bytes, held_bytes_);
        });
      } else if (worker.group != nullptr &&
                 worker.task_index < worker.group->tasks().size()) {
        const auto& task = worker.group->tasks()[worker.task_index];
        if (task == nullptr) {
          return InvalidArgument("Task group contains a null task");
        }
        ICEBERG_RETURN_UNEXPECTED(OpenTask(worker, *task));
      } else {
        ICEBERG_ASSIGN_OR_RAISE(auto claimed, ClaimGroup(worker));
        if (!claimed) {
          return {};  // Parked until the planning token is released.
        }
        worker.group = std::move(*claimed);
        worker.task_index = 0;
        if (worker.group == nullptr) {
          return {};
        }
      }
    }
    return {};
  }

  Result<std::optional<std::shared_ptr<FileScanTaskGroup>>> ClaimGroup(Worker& worker) {
    {
      std::lock_guard lock(mutex_);
      if (stopped_ || planning_done_) {
        return std::shared_ptr<FileScanTaskGroup>{};
      }
      if (planning_in_progress_) {
        worker.state = WorkerState::kWaitingPlanning;
        return std::nullopt;
      }
      planning_in_progress_ = true;
    }

    Result<std::optional<std::shared_ptr<FileScanTaskGroup>>> next = std::nullopt;
    try {
      next = groups_->Next();
    } catch (const std::exception& e) {
      next = UnknownError("Planning task groups failed: {}", e.what());
    } catch (...) {
      next = UnknownError("Planning task groups failed with an unknown exception");
    }
    const bool finished = !next.has_value() || !next->has_value();
    if (finished) {
      groups_.reset();
    }
    {
      std::lock_guard lock(mutex_);
      planning_in_progress_ = false;
      planning_done_ = finished;
      WakeWorkers(WorkerState::kWaitingPlanning);
    }
    if (!next.has_value()) {
      return unexpected(WithContext(next.error(), "Failed to plan task groups"));
    }
    if (!next->has_value()) {
      return std::shared_ptr<FileScanTaskGroup>{};
    }
    if (**next == nullptr) {
      return InvalidArgument("Task group stream returned a null group");
    }
    RecordMetrics([](auto& snapshot) { ++snapshot.groups_claimed; });
    return next;
  }

  static std::string LastError(ArrowArrayStream& stream) {
    const char* message = stream.get_last_error(&stream);
    return message == nullptr ? "unknown error" : message;
  }

  Status OpenTask(Worker& worker, const FileScanTask& task) {
    worker.context = DescribeTask(task);
    ICEBERG_ASSIGN_OR_RAISE(auto simplified, SimplifyTask(task));
    auto opened = worker.reader->Open(*simplified);
    if (!opened.has_value()) {
      return unexpected(WithContext(opened.error(), "Failed to open " + worker.context));
    }
    worker.stream = *opened;
    RecordMetrics([](auto& snapshot) { ++snapshot.tasks_opened; });
    ArrowSchema schema{};
    ArrowSchemaGuard guard(&schema);
    if (int code = worker.stream.get_schema(&worker.stream, &schema); code != 0) {
      return IOError("Failed to read the schema of {} (code {}): {}", worker.context,
                     code, LastError(worker.stream));
    }
    if (auto status = CheckSameLayout(output_schema_, schema, "root", false);
        !status.has_value()) {
      return unexpected(WithContext(
          status.error(), "Task stream schema does not match the projected schema for " +
                              worker.context));
    }
    return {};
  }

  // Fold each predicate using the table schema, retaining unbound predicates that
  // still need evaluation. Their references are then bound to the read projection
  // by FileScanTaskReader, with the correct projected field positions.
  Result<std::shared_ptr<FileScanTask>> SimplifyTask(const FileScanTask& task) {
    auto filter = task.residual_filter();
    if (filter != nullptr) {
      ResidualSimplifier simplifier(*table_schema_, case_sensitive_);
      ICEBERG_ASSIGN_OR_RAISE(filter,
                              Visit<std::shared_ptr<Expression>>(filter, simplifier));
    }
    return FileScanTask::MakeSplit(task.data_file(), task.start(), task.length(),
                                   task.delete_files(), std::move(filter));
  }

  bool TryEnqueue(Worker& worker) {
    std::unique_lock lock(mutex_);
    if (stopped_) {
      return false;
    }
    if (worker.ticket != serving_ticket_ || !CanAdmit(worker.charge)) {
      worker.state = WorkerState::kWaitingAdmission;
      return false;
    }
    queue_.push_back(QueuedBatch{.array = worker.pending, .charge = worker.charge});
    worker.pending.release = nullptr;
    held_bytes_ -= worker.charge;
    queued_bytes_ += worker.charge;
    ++serving_ticket_;
    RecordWait(worker);
    RecordMetrics([&](auto& snapshot) {
      ++snapshot.batches_admitted;
      snapshot.bytes_admitted += worker.charge;
      if (worker.charge > max_queued_bytes_) {
        ++snapshot.oversized_admissions;
      }
      snapshot.peak_queued_batches =
          std::max(snapshot.peak_queued_batches, static_cast<int64_t>(queue_.size()));
      snapshot.peak_queued_bytes = std::max(snapshot.peak_queued_bytes, queued_bytes_);
    });
    WakeWorkers(WorkerState::kWaitingAdmission);
    lock.unlock();
    consumer_cv_.notify_one();
    return true;
  }

  bool CanAdmit(int64_t charge) const {
    if (std::cmp_greater_equal(queue_.size(), max_queued_batches_)) {
      return false;
    }
    if (charge > max_queued_bytes_) {
      return queue_.empty();
    }
    return queued_bytes_ <= max_queued_bytes_ - charge;
  }

  void WakeWorkers(WorkerState state) {
    for (auto& worker : workers_) {
      if (worker->state == state &&
          (state != WorkerState::kWaitingAdmission ||
           (worker->ticket == serving_ticket_ && CanAdmit(worker->charge)))) {
        worker->state = WorkerState::kReady;
      }
    }
  }

  void RecordWait(const Worker& worker) {
    const auto waited = std::chrono::steady_clock::now() - worker.wait_start;
    RecordMetrics([&](auto& snapshot) { snapshot.producer_wait += waited; });
  }

  template <typename Update>
  void RecordMetrics(Update&& update) {
    if (metrics_ != nullptr) {
      metrics_->Update(std::forward<Update>(update));
    }
  }

  bool IsStopped() {
    std::lock_guard lock(mutex_);
    return stopped_;
  }

  void Fail(const Error& error) {
    std::deque<QueuedBatch> drained;
    {
      std::lock_guard lock(mutex_);
      if (!error_.has_value()) {
        error_ = error;
      }
      stopped_ = true;
      drained.swap(queue_);
      queued_bytes_ = 0;
    }
    consumer_cv_.notify_all();
    ReleaseAll(drained);
  }

  FileScanTaskGroupStreamPtr groups_;
  ArrowSchema output_schema_;
  Executor& executor_;
  std::shared_ptr<iceberg::Schema> table_schema_;
  bool case_sensitive_;
  const int64_t max_queued_batches_;
  const int64_t max_queued_bytes_;
  std::shared_ptr<ParallelScanMetrics> metrics_;
  std::vector<std::unique_ptr<Worker>> workers_;

  std::mutex mutex_;
  std::condition_variable consumer_cv_;
  std::condition_variable idle_cv_;
  bool stopped_ = false;
  bool dispatching_ = false;
  bool planning_in_progress_ = false;
  bool planning_done_ = false;
  int32_t active_workers_ = 0;
  int32_t running_workers_ = 0;
  std::optional<Error> error_;
  std::deque<QueuedBatch> queue_;
  int64_t queued_bytes_ = 0;
  int64_t held_bytes_ = 0;
  uint64_t next_ticket_ = 0;
  uint64_t serving_ticket_ = 0;
};

// Adapts ParallelScan to the ArrowArrayStream provider interface.
class ParallelScanSource {
 public:
  explicit ParallelScanSource(std::shared_ptr<ParallelScan> scan)
      : scan_(std::move(scan)) {}

  Status Close() {
    scan_->Shutdown();
    return {};
  }

  Result<std::optional<ArrowArray>> Next() { return scan_->Next(); }

  Result<ArrowSchema> Schema() { return scan_->Schema(); }

 private:
  std::shared_ptr<ParallelScan> scan_;
};

}  // namespace

Result<ArrowArrayStream> OpenParallelScan(FileScanTaskGroupStreamPtr groups,
                                          ParallelScanReader::Options options,
                                          std::shared_ptr<ParallelScanMetrics> metrics) {
  ICEBERG_PRECHECK(groups != nullptr, "Task group stream must not be null");
  ICEBERG_PRECHECK(options.worker_count > 0, "Worker count must be positive: {}",
                   options.worker_count);
  ICEBERG_PRECHECK(options.max_queued_batches > 0,
                   "Maximum queued batches must be positive: {}",
                   options.max_queued_batches);
  ICEBERG_PRECHECK(options.max_queued_bytes > 0,
                   "Maximum queued bytes must be positive: {}", options.max_queued_bytes);
  ICEBERG_PRECHECK(options.reader_options.projected_schema != nullptr,
                   "Projected schema must not be null");

  ICEBERG_PRECHECK(options.executor != nullptr, "Executor must not be null");

  options.reader_options.apply_residuals = true;
  const auto properties = ReaderProperties::FromMap(options.reader_options.properties);

  ArrowSchema output_schema{};
  ArrowSchemaGuard schema_guard(&output_schema);
  ICEBERG_RETURN_UNEXPECTED(
      ToArrowSchema(*options.reader_options.projected_schema, &output_schema,
                    properties.Get(ReaderProperties::kArrowUseLargeList)));

  auto scan = std::make_shared<ParallelScan>(std::move(groups), output_schema, options,
                                             std::move(metrics));
  schema_guard.Release();
  if (auto status = scan->Start(options.reader_options, options.worker_count);
      !status.has_value()) {
    scan->Shutdown();
    return unexpected(status.error());
  }
  return MakeArrowArrayStream(std::make_unique<ParallelScanSource>(std::move(scan)));
}

}  // namespace internal

Result<ArrowArrayStream> ParallelScanReader::Open(FileScanTaskGroupStreamPtr groups,
                                                  Options options) {
  return internal::OpenParallelScan(std::move(groups), std::move(options),
                                    /*metrics=*/nullptr);
}

}  // namespace iceberg
