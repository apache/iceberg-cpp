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

#include <cstdint>
#include <memory>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Opens metadata scan tasks as Arrow streams.
class ICEBERG_EXPORT MetadataScanTaskReader {
 public:
  /// \brief Options shared by all tasks opened by this reader.
  struct Options {
    /// Positive maximum number of rows per Arrow batch.
    int64_t batch_size = 1024;
  };

  /// \brief Create a reusable task reader from shared read options.
  static Result<std::unique_ptr<MetadataScanTaskReader>> Make(Options options);

  ~MetadataScanTaskReader();

  /// \brief Open a task and return an Arrow C stream for its projected rows.
  ///
  /// Batches are built on demand. The stream retains its input data and can
  /// outlive this reader and the task; each call starts an independent read.
  Result<ArrowArrayStream> Open(const MetadataScanTask& task) const;

  MetadataScanTaskReader(const MetadataScanTaskReader&) = delete;
  MetadataScanTaskReader& operator=(const MetadataScanTaskReader&) = delete;

 private:
  explicit MetadataScanTaskReader(Options options);

  const Options options_;
};

}  // namespace iceberg
