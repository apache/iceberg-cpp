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

/// \file iceberg/data/writer.h
/// Base interface for Iceberg data file writers.

#include <cstdint>
#include <memory>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base interface for data file writers.
class ICEBERG_EXPORT FileWriter {
 public:
  virtual ~FileWriter();

  /// \brief Write a batch of records.
  /// \note The ownership of the ArrowArray will be transferred to the writer.
  virtual Status Write(ArrowArray* data) = 0;

  /// \brief Get the current number of bytes written.
  virtual Result<int64_t> Length() const = 0;

  /// \brief Close the writer and finalize the file.
  virtual Status Close() = 0;

  /// \brief File metadata for all files produced by this writer.
  struct ICEBERG_EXPORT WriteResult {
    /// Usually a writer produces a single data or delete file.
    /// Position delete writer may produce multiple file-scoped delete files.
    /// In the future, multiple files can be produced if file rolling is supported.
    std::vector<std::shared_ptr<DataFile>> data_files;
  };

  /// \brief Get file metadata for all files produced by this writer.
  /// \note This method should be called after Close().
  virtual Result<WriteResult> Metadata() = 0;
};

}  // namespace iceberg
