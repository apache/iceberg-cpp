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
#include <thread>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/data/arrow_batch_sizer_internal.h"
#include "iceberg/data/parallel_scan_reader_internal.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
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

// Releases an ArrowArrayStream on scope exit.
class StreamGuard {
 public:
  explicit StreamGuard(ArrowArrayStream* stream) : stream_(stream) {}
  ~StreamGuard() {
    if (stream_->release != nullptr) {
      stream_->release(stream_);
    }
  }
  StreamGuard(const StreamGuard&) = delete;
  StreamGuard& operator=(const StreamGuard&) = delete;

 private:
  ArrowArrayStream* stream_;
};

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

// Shared state of one parallel read. All fields below `mutex_` are guarded by it.
// No planning or reader callback, Arrow release callback, thread join, or group
// stream destructor runs while `mutex_` is held.
class ParallelScan {
 public:
  ParallelScan(FileScanTaskGroupStreamPtr groups, ArrowSchema output_schema,
               int64_t max_queued_batches, int64_t max_queued_bytes,
               std::shared_ptr<ParallelScanMetrics> metrics)
      : groups_(std::move(groups)),
        output_schema_(output_schema),
        max_queued_batches_(max_queued_batches),
        max_queued_bytes_(max_queued_bytes),
        metrics_(std::move(metrics)) {}

  ~ParallelScan() {
    Shutdown();
    if (output_schema_.release != nullptr) {
      output_schema_.release(&output_schema_);
    }
  }

  ParallelScan(const ParallelScan&) = delete;
  ParallelScan& operator=(const ParallelScan&) = delete;

  Status Start(const FileScanTaskReader::Options& reader_options, int32_t worker_count) {
    std::vector<std::unique_ptr<FileScanTaskReader>> readers;
    std::vector<std::unique_ptr<ArrowBatchSizer>> sizers;
    readers.reserve(static_cast<size_t>(worker_count));
    sizers.reserve(static_cast<size_t>(worker_count));
    for (int32_t index = 0; index < worker_count; ++index) {
      ICEBERG_ASSIGN_OR_RAISE(auto reader, FileScanTaskReader::Make(reader_options));
      ICEBERG_ASSIGN_OR_RAISE(auto sizer, ArrowBatchSizer::Make(output_schema_));
      readers.push_back(std::move(reader));
      sizers.push_back(std::move(sizer));
    }

    {
      std::lock_guard lock(mutex_);
      active_workers_ = worker_count;
    }
    workers_.reserve(static_cast<size_t>(worker_count));
    for (int32_t index = 0; index < worker_count; ++index) {
      try {
        workers_.emplace_back([this, reader = std::move(readers[index]),
                               sizer = std::move(sizers[index])]() mutable {
          WorkerMain(*reader, *sizer);
        });
      } catch (const std::exception& e) {
        // Threads that never started will not decrement the active count.
        {
          std::lock_guard lock(mutex_);
          active_workers_ -= worker_count - index;
        }
        Shutdown();
        return UnknownError("Failed to start parallel scan worker: {}", e.what());
      }
    }
    return {};
  }

  // Consumer side of get_next. Not called concurrently with itself or Shutdown().
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
    lock.unlock();
    // Producers are admitted in ticket order, and a condition variable cannot wake
    // one specific waiter, so wake them all and let the head ticket re-check. At
    // most worker_count threads wait here.
    producer_cv_.notify_all();
    return batch.array;
  }

  Result<ArrowSchema> Schema() const {
    ArrowSchema schema{};
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowSchemaDeepCopy(&output_schema_, &schema));
    return schema;
  }

  // Stops new work, joins workers, and releases every batch that was not returned.
  // Idempotent.
  void Shutdown() {
    std::deque<QueuedBatch> drained;
    {
      std::lock_guard lock(mutex_);
      stopped_ = true;
      drained.swap(queue_);
      queued_bytes_ = 0;
    }
    planning_cv_.notify_all();
    producer_cv_.notify_all();
    consumer_cv_.notify_all();
    ReleaseAll(drained);

    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    workers_.clear();

    // Workers have exited, so nothing else can enqueue or plan.
    {
      std::lock_guard lock(mutex_);
      drained.swap(queue_);
    }
    ReleaseAll(drained);
    groups_.reset();
  }

 private:
  void WorkerMain(FileScanTaskReader& reader, ArrowBatchSizer& sizer) {
    Status status;
    try {
      status = RunWorker(reader, sizer);
    } catch (const std::exception& e) {
      status = UnknownError("Parallel scan worker failed: {}", e.what());
    } catch (...) {
      status = UnknownError("Parallel scan worker failed with an unknown exception");
    }
    if (!status.has_value()) {
      Fail(status.error());
    }
    {
      std::lock_guard lock(mutex_);
      --active_workers_;
    }
    consumer_cv_.notify_all();
  }

  Status RunWorker(FileScanTaskReader& reader, ArrowBatchSizer& sizer) {
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto group, ClaimGroup());
      if (group == nullptr) {
        return {};
      }
      for (const auto& task : group->tasks()) {
        if (IsStopped()) {
          return {};
        }
        if (task == nullptr) {
          return InvalidArgument("Task group contains a null task");
        }
        ICEBERG_RETURN_UNEXPECTED(ReadTask(reader, sizer, *task));
      }
    }
  }

  // Returns the next group, or nullptr when planning is exhausted or the read stopped.
  // Only one worker calls the group stream at a time, outside all locks.
  Result<std::shared_ptr<FileScanTaskGroup>> ClaimGroup() {
    {
      std::unique_lock lock(mutex_);
      planning_cv_.wait(lock, [this] { return stopped_ || !planning_in_progress_; });
      if (stopped_ || planning_done_) {
        return nullptr;
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

    const bool finished = !next.has_value() || !next.value().has_value();
    if (finished) {
      // This worker still holds the planning token, so no other worker uses the
      // stream. Destroy it now, outside locks, so metrics reporting runs at EOF.
      groups_.reset();
    }

    {
      std::lock_guard lock(mutex_);
      planning_in_progress_ = false;
      if (finished) {
        planning_done_ = true;
      }
    }
    planning_cv_.notify_all();

    if (!next.has_value()) {
      return unexpected(WithContext(next.error(), "Failed to plan task groups"));
    }
    if (!next.value().has_value()) {
      return nullptr;
    }
    auto group = std::move(next.value().value());
    if (group == nullptr) {
      return InvalidArgument("Task group stream returned a null group");
    }
    if (metrics_ != nullptr) {
      metrics_->Update([](auto& snapshot) { ++snapshot.groups_claimed; });
    }
    return group;
  }

  Status ReadTask(FileScanTaskReader& reader, ArrowBatchSizer& sizer,
                  const FileScanTask& task) {
    const std::string context = DescribeTask(task);
    auto opened = reader.Open(task);
    if (!opened.has_value()) {
      return unexpected(WithContext(opened.error(), "Failed to open " + context));
    }
    ArrowArrayStream stream = std::move(opened.value());
    StreamGuard stream_guard(&stream);
    if (metrics_ != nullptr) {
      metrics_->Update([](auto& snapshot) { ++snapshot.tasks_opened; });
    }

    auto last_error = [&stream]() {
      const char* message = stream.get_last_error(&stream);
      return message == nullptr ? std::string("unknown error") : std::string(message);
    };

    ArrowSchema task_schema{};
    if (int code = stream.get_schema(&stream, &task_schema); code != 0) {
      return IOError("Failed to read the schema of {} (code {}): {}", context, code,
                     last_error());
    }
    internal::ArrowSchemaGuard task_schema_guard(&task_schema);
    if (auto status = CheckSameLayout(output_schema_, task_schema, "root", false);
        !status.has_value()) {
      return unexpected(WithContext(
          status.error(),
          "Task stream schema does not match the projected schema for " + context));
    }

    // Enqueue() reports a stop, so no separate check is needed per batch.
    while (true) {
      ArrowArray batch{};
      if (int code = stream.get_next(&stream, &batch); code != 0) {
        return IOError("Failed to read {} (code {}): {}", context, code, last_error());
      }
      if (batch.release == nullptr) {
        return {};
      }
      auto charge = sizer.Measure(batch);
      if (!charge.has_value()) {
        ReleaseArray(&batch);
        return unexpected(
            WithContext(charge.error(), "Failed to size a batch of " + context));
      }
      if (!Enqueue(batch, charge.value())) {
        // Stopped while waiting; the array was not admitted.
        ReleaseArray(&batch);
        return {};
      }
    }
  }

  // Blocks until the batch is admitted in FIFO order or the read stops. Returns
  // false when stopped, in which case the caller still owns `batch`.
  bool Enqueue(const ArrowArray& batch, int64_t charge) {
    const auto wait_start = std::chrono::steady_clock::now();
    std::unique_lock lock(mutex_);
    const uint64_t ticket = next_ticket_++;
    held_bytes_ += charge;
    RecordMetrics([this](auto& snapshot) {
      snapshot.peak_held_bytes = std::max(snapshot.peak_held_bytes, held_bytes_);
    });

    producer_cv_.wait(lock, [&] {
      return stopped_ || (ticket == serving_ticket_ && CanAdmit(charge));
    });
    held_bytes_ -= charge;
    const auto waited = std::chrono::steady_clock::now() - wait_start;
    if (stopped_) {
      RecordMetrics([&](auto& snapshot) { snapshot.producer_wait += waited; });
      return false;
    }

    const bool oversized = charge > max_queued_bytes_;
    queue_.push_back(QueuedBatch{.array = batch, .charge = charge});
    queued_bytes_ += charge;
    ++serving_ticket_;
    RecordMetrics([&](auto& snapshot) {
      snapshot.producer_wait += waited;
      ++snapshot.batches_admitted;
      snapshot.bytes_admitted += charge;
      if (oversized) {
        ++snapshot.oversized_admissions;
      }
      snapshot.peak_queued_batches =
          std::max(snapshot.peak_queued_batches, static_cast<int64_t>(queue_.size()));
      snapshot.peak_queued_bytes = std::max(snapshot.peak_queued_bytes, queued_bytes_);
    });
    lock.unlock();
    producer_cv_.notify_all();
    consumer_cv_.notify_one();
    return true;
  }

  // Requires `mutex_`.
  bool CanAdmit(int64_t charge) const {
    if (static_cast<int64_t>(queue_.size()) >= max_queued_batches_) {
      return false;
    }
    if (charge > max_queued_bytes_) {
      // An oversized batch enters only as the sole queued batch.
      return queue_.empty();
    }
    return queued_bytes_ <= max_queued_bytes_ - charge;
  }

  // Requires `mutex_`; the metrics lock is always taken after it.
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

  // Records the first error, stops new work, and releases queued batches.
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
    planning_cv_.notify_all();
    producer_cv_.notify_all();
    consumer_cv_.notify_all();
    ReleaseAll(drained);
  }

  // Only touched by the worker holding the planning token, or after all workers
  // have joined.
  FileScanTaskGroupStreamPtr groups_;
  ArrowSchema output_schema_;
  const int64_t max_queued_batches_;
  const int64_t max_queued_bytes_;
  std::shared_ptr<ParallelScanMetrics> metrics_;
  std::vector<std::thread> workers_;

  std::mutex mutex_;
  std::condition_variable planning_cv_;
  std::condition_variable producer_cv_;
  std::condition_variable consumer_cv_;
  bool stopped_ = false;
  bool planning_in_progress_ = false;
  bool planning_done_ = false;
  int32_t active_workers_ = 0;
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
  explicit ParallelScanSource(std::unique_ptr<ParallelScan> scan)
      : scan_(std::move(scan)) {}

  Status Close() {
    scan_->Shutdown();
    return {};
  }

  Result<std::optional<ArrowArray>> Next() { return scan_->Next(); }

  Result<ArrowSchema> Schema() { return scan_->Schema(); }

 private:
  std::unique_ptr<ParallelScan> scan_;
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

  options.reader_options.apply_residuals = true;
  const auto properties = ReaderProperties::FromMap(options.reader_options.properties);

  ArrowSchema output_schema{};
  ICEBERG_RETURN_UNEXPECTED(
      ToArrowSchema(*options.reader_options.projected_schema, &output_schema,
                    properties.Get(ReaderProperties::kArrowUseLargeList)));

  auto scan = std::make_unique<ParallelScan>(
      std::move(groups), output_schema, options.max_queued_batches,
      options.max_queued_bytes, std::move(metrics));
  ICEBERG_RETURN_UNEXPECTED(scan->Start(options.reader_options, options.worker_count));
  return MakeArrowArrayStream(std::make_unique<ParallelScanSource>(std::move(scan)));
}

}  // namespace internal

Result<ArrowArrayStream> ParallelScanReader::Open(FileScanTaskGroupStreamPtr groups,
                                                  Options options) {
  return internal::OpenParallelScan(std::move(groups), std::move(options),
                                    /*metrics=*/nullptr);
}

}  // namespace iceberg
