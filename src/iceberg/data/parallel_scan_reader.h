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

/// \file iceberg/data/parallel_scan_reader.h
/// Reads planned FileScanTaskGroups concurrently into one Arrow stream.

#include <cstdint>

#include "iceberg/arrow_c_data.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/table_scan.h"

namespace iceberg {

/// \brief Reads the groups of a FileScanTaskGroupStream with a fixed set of workers.
///
/// Each worker owns its own FileScanTaskReader. Workers take groups from the
/// single-pass group stream one at a time, read the tasks within a group in order,
/// and place each completed batch into an output queue bounded by batch count and
/// by charged bytes. The caller receives one Arrow C stream that yields batches as
/// workers admit them.
///
/// Output order across groups and tasks is unspecified; the batches of one task keep
/// their order relative to each other. Every batch matches the projected schema.
///
/// Residual filters are always enforced: `apply_residuals` in the reader options is
/// forced on. `reader_options.case_sensitive` must match the planning scan's
/// `TableScan::is_case_sensitive()`, because task residuals are unbound and groups do
/// not carry that setting.
///
/// Queue memory: `max_queued_bytes` limits admission using the summed spans of the
/// Arrow buffers of queued batches, counting every buffer occurrence. A batch larger
/// than the budget is admitted only when the queue is empty, as its sole batch.
/// Zero-byte batches still count toward `max_queued_batches`. This is not a limit
/// on process memory: format reader buffers, delete indexes, allocator overhead, and
/// arrays already returned to the caller are outside the budget. Each worker can also
/// hold one completed batch while it waits for admission. Per worker, expect roughly
/// one open format reader with its read buffers, one task's delete index, and one
/// completed batch, in addition to the queue budget.
///
/// Preconditions, which the reader cannot check:
/// - Format reader factories are registered before Open() and are not changed until
///   the returned stream is released.
/// - `reader_options.io` supports concurrent reads from several threads.
/// - Tasks and their shared data and delete file metadata are not mutated while the
///   stream is in use.
/// - Any executor passed to `TableScanBuilder::PlanWith()` outlives the returned
///   stream, which owns the lazy group stream.
///
/// Stream callbacks follow the Arrow C stream contract: they must not be called
/// concurrently, and `release` must not race with `get_next`, `get_schema`, or
/// `get_last_error`. Releasing the stream between callbacks stops assigning work,
/// wakes waiting workers, releases batches that were not returned, and joins the
/// workers. A planning call or read that is already in progress finishes first.
/// Cancelling a `get_next` that is blocked on another thread is not supported.
///
/// When planning, reading, schema checking, or batch sizing fails, the reader stops
/// assigning work, releases queued batches, and reports the first error from the
/// next `get_next` call as `EIO`, with a message that names the file and byte range
/// available from `get_last_error`. Arrays already returned to the caller remain
/// valid after the stream is released.
class ICEBERG_DATA_EXPORT ParallelScanReader {
 public:
  /// \brief Options for one parallel read.
  struct Options {
    /// Options for the per-worker task readers. `apply_residuals` is forced on.
    FileScanTaskReader::Options reader_options;
    /// Number of worker threads. Must be positive.
    int32_t worker_count = 0;
    /// Maximum number of batches waiting in the output queue. Must be positive.
    int64_t max_queued_batches = 0;
    /// Target for the charged bytes of batches waiting in the output queue. Must be
    /// positive.
    int64_t max_queued_bytes = 0;
  };

  /// \brief Start reading `groups` and return a stream of their projected live rows.
  ///
  /// The returned stream owns `groups`. Workers start before Open() returns.
  static Result<ArrowArrayStream> Open(FileScanTaskGroupStreamPtr groups,
                                       Options options);
};

}  // namespace iceberg
