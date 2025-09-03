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

#include <optional>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief A reader interface that returns ArrowArray in a streaming fashion.
class ICEBERG_EXPORT ArrowArrayReader {
 public:
  /// \brief Read next batch of data.
  ///
  /// \return std::nullopt if the reader has no more data, otherwise `ArrowArray`.
  virtual Result<std::optional<ArrowArray>> Next() = 0;

  /// \brief Get schema of data returned by `Next`.
  virtual Result<ArrowSchema> Schema() const = 0;

  /// \brief Close this reader and release all resources.
  virtual Status Close() = 0;

  virtual ~ArrowArrayReader() = default;
};

}  // namespace iceberg
