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

#pragma once

/// \file iceberg/data/parallel_scan_reader_internal.h
/// Queue and producer metrics for ParallelScanReader. Not a stable API.

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>

#include "iceberg/arrow_c_data.h"
#include "iceberg/data/parallel_scan_reader.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"

namespace iceberg::internal {

/// \brief A point-in-time copy of a parallel read's queue and producer counters.
struct ParallelScanMetricsSnapshot {
  /// Groups taken from the group stream.
  int64_t groups_claimed = 0;
  /// Tasks opened by workers.
  int64_t tasks_opened = 0;
  /// Batches admitted to the output queue.
  int64_t batches_admitted = 0;
  /// Charged bytes of all admitted batches.
  int64_t bytes_admitted = 0;
  /// Batches admitted although their charge exceeded `max_queued_bytes`.
  int64_t oversized_admissions = 0;
  /// Peak number of batches in the output queue.
  int64_t peak_queued_batches = 0;
  /// Peak charged bytes in the output queue.
  int64_t peak_queued_bytes = 0;
  /// Peak charged bytes of completed batches that workers held while waiting.
  int64_t peak_held_bytes = 0;
  /// Total time that workers waited for admission.
  std::chrono::nanoseconds producer_wait{0};
};

/// \brief Thread-safe counters shared between a parallel read and its observers.
///
/// Snapshot() takes only this object's lock, so it may be called from any thread,
/// including from a group stream destructor or a metrics reporter.
class ICEBERG_DATA_EXPORT ParallelScanMetrics {
 public:
  /// \brief Return a copy of the current counters.
  ParallelScanMetricsSnapshot Snapshot() const;

  /// \brief Apply `update` to the counters under this object's lock.
  template <typename Fn>
  void Update(Fn&& update) {
    std::lock_guard lock(mutex_);
    update(snapshot_);
  }

 private:
  mutable std::mutex mutex_;
  ParallelScanMetricsSnapshot snapshot_;
};

/// \brief Same as ParallelScanReader::Open(), also returning live metrics.
///
/// `metrics` may be retained after the stream is released.
ICEBERG_DATA_EXPORT Result<ArrowArrayStream> OpenParallelScan(
    FileScanTaskGroupStreamPtr groups, ParallelScanReader::Options options,
    std::shared_ptr<ParallelScanMetrics> metrics);

}  // namespace iceberg::internal
