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

/// \file iceberg/data/file_scan_task_reader.h
/// Delete-aware FileScanTask reader for copy-on-write and merge-on-read paths.

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Opens a FileScanTask as an ArrowArrayStream.
///
/// FileScanTaskReader chooses the copy-on-write path for tasks without deletes and
/// the merge-on-read path for tasks with v2 position or equality deletes. The returned
/// stream always exposes `projected_schema`.
///
/// TODO(gangwu): Add a mode that emits a `_deleted` column instead of filtering rows.
/// TODO(gangwu): Use evaluator to apply residual expression filters.
class ICEBERG_DATA_EXPORT FileScanTaskReader {
 public:
  /// \brief Options shared by all tasks opened by this reader.
  struct Options {
    /// FileIO instance for reading data and delete files.
    std::shared_ptr<FileIO> io;
    /// The table schema. Used as the primary field lookup for delete file resolution.
    std::shared_ptr<Schema> table_schema;
    /// Optional list of historical table schemas for field lookup.
    std::vector<std::shared_ptr<Schema>> schemas;
    /// The output schema for the returned ArrowArrayStream. Must be a
    /// projection of table_schema.
    std::shared_ptr<Schema> projected_schema;
    /// Optional name mapping for files written without field IDs.
    std::shared_ptr<NameMapping> name_mapping;
    /// Format-specific or implementation-specific options for data readers.
    std::unordered_map<std::string, std::string> properties;
  };

  /// \brief Create a reusable task reader from shared read context.
  static Result<std::unique_ptr<FileScanTaskReader>> Make(Options options);

  ~FileScanTaskReader();

  /// \brief Open a task and return an Arrow C stream for its projected live rows.
  Result<ArrowArrayStream> Open(const FileScanTask& task);

  FileScanTaskReader(const FileScanTaskReader&) = delete;
  FileScanTaskReader& operator=(const FileScanTaskReader&) = delete;

 private:
  class Impl;
  explicit FileScanTaskReader(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
