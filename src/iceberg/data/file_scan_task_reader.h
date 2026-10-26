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
/// stream exposes the fields of `projected_schema` in one stable physical Arrow layout,
/// which may depend on the file format and reader properties (for example, `large_list`
/// instead of `list`). Every batch matches the schema reported by the stream. When
/// `apply_residuals` is enabled, each task's residual filter is enforced after deletes
/// and before final projection.
///
/// TODO(gangwu): Add a mode that emits a `_deleted` column instead of filtering rows.
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
    /// The read and output projection; this need not be the full table schema.
    /// Must be a projection of table_schema. With apply_residuals enabled, callers
    /// must include every field referenced by each task's original residual, even
    /// if binding would fold its predicate to a constant. Open() fails on missing
    /// references rather than adding fields from table_schema. Filter-only columns
    /// remain in the output; delete-only columns are added internally as needed
    /// and removed before returning batches.
    std::shared_ptr<Schema> projected_schema;
    /// Case sensitivity for residual binding and reader field name resolution.
    /// Must match TableScan::is_case_sensitive(); tasks do not carry this setting.
    bool case_sensitive = true;
    /// Optional name mapping for files written without field IDs.
    std::shared_ptr<NameMapping> name_mapping;
    /// Format-specific or implementation-specific options for data readers.
    std::unordered_map<std::string, std::string> properties;
    /// Whether to enforce each task's residual filter row by row, after deletes
    /// and before final projection. Residuals are bound against projected_schema;
    /// invalid references and unsupported expressions return errors.
    bool apply_residuals{false};
  };

  /// \brief Create a reusable task reader from shared read context.
  static Result<std::unique_ptr<FileScanTaskReader>> Make(Options options);

  ~FileScanTaskReader();

  /// \brief Open a task and return an Arrow C stream for its projected live rows.
  ///
  /// When Options::apply_residuals is set, Options::projected_schema must contain
  /// all fields referenced by the task's original residual. Missing references
  /// cause Open() to fail; they are not added from the table schema. Rows are
  /// filtered after deletes and before final projection.
  ///
  /// Partial tasks are supported for Parquet, with each row group assigned to the
  /// half-open range containing its midpoint, as in Java Iceberg. Row positions and
  /// inherited row IDs remain relative to the whole file. Other formats require
  /// whole-file tasks. The task and its shared data/delete-file metadata must not be
  /// mutated while the stream uses them.
  Result<ArrowArrayStream> Open(const FileScanTask& task);

  FileScanTaskReader(const FileScanTaskReader&) = delete;
  FileScanTaskReader& operator=(const FileScanTaskReader&) = delete;

 private:
  class Impl;
  explicit FileScanTaskReader(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
