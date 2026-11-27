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

/// \file iceberg/data/arrow_batch_sizer_internal.h
/// Measures the Arrow buffer bytes that a batch occupies in a scan queue.

#include <cstdint>
#include <memory>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"

namespace iceberg::internal {

/// \brief Charges Arrow batches that share one physical schema.
///
/// The charge is the checked sum of the spans of every present buffer in the root
/// array, its children, and its dictionaries, including variadic data buffers and
/// their size buffer. Each buffer occurrence is counted, even when several buffers
/// share a pointer, so a charge can overstate retained memory but never understates
/// the measured spans.
///
/// Before any nanoarrow call that sizes buffers, Measure() checks that offsets and
/// lengths are nonnegative, that their sum and every layout-dependent buffer or
/// child-length product fit in `int64_t` and `size_t`, and that buffer, child, and
/// dictionary counts match the schema. Layouts whose spans nanoarrow cannot
/// determine (list views and run-end encoded arrays) are rejected.
///
/// A sizer is not thread-safe; use one per thread.
class ICEBERG_DATA_EXPORT ArrowBatchSizer {
 public:
  /// \brief Create a sizer for batches described by `schema`.
  ///
  /// The schema is copied; the caller keeps ownership of `schema`.
  static Result<std::unique_ptr<ArrowBatchSizer>> Make(const ArrowSchema& schema);

  ~ArrowBatchSizer();

  ArrowBatchSizer(const ArrowBatchSizer&) = delete;
  ArrowBatchSizer& operator=(const ArrowBatchSizer&) = delete;

  /// \brief Return the charged bytes of `array`, or an error if it cannot be sized.
  ///
  /// `array` remains owned by the caller and is not modified.
  Result<int64_t> Measure(const ArrowArray& array);

 private:
  class Impl;
  explicit ArrowBatchSizer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg::internal
