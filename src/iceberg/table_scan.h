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

#include <string>
#include <vector>

#include "iceberg/expression/expression.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Builder class for creating TableScan instances.
class ICEBERG_EXPORT TableScanBuilder {
 public:
  /// \brief Constructs a TableScanBuilder for the given table.
  /// \param table Reference to the table to scan.
  explicit TableScanBuilder(const Table& table);

  /// \brief Sets the snapshot ID to scan.
  /// \param snapshot_id The ID of the snapshot.
  /// \return Reference to the builder.
  TableScanBuilder& WithSnapshotId(int64_t snapshot_id);

  /// \brief Selects columns to include in the scan.
  /// \param column_names A list of column names.
  /// \return Reference to the builder.
  TableScanBuilder& WithColumnNames(const std::vector<std::string>& column_names);

  /// \brief Applies a filter expression to the scan.
  /// \param filter Filter expression to use.
  /// \return Reference to the builder.
  TableScanBuilder& WithFilter(const std::shared_ptr<Expression>& filter);

  /// \brief Builds and returns a TableScan instance.
  /// \return A Result containing the TableScan or an error.
  Result<std::unique_ptr<TableScan>> Build();

 private:
  const Table& table_;
  std::vector<std::string> column_names_;
  std::optional<int64_t> snapshot_id_;
  std::shared_ptr<Expression> filter_;
};

/// \brief Represents a configured scan operation on a table.
class ICEBERG_EXPORT TableScan {
 public:
  /// \brief Scan context holding snapshot and scan-specific metadata.
  struct ScanContext {
    std::shared_ptr<Snapshot> snapshot_;  ///< Snapshot to scan.
    std::shared_ptr<Schema> schema_;      ///< Projected schema.
    std::vector<int32_t> field_ids_;      ///< Field IDs of selected columns.
    std::shared_ptr<Expression> filter_;  ///< Filter expression to apply.
  };

  /// \brief Constructs a TableScan with the given context and file I/O.
  /// \param context Scan context including snapshot, schema, and filter.
  /// \param file_io File I/O instance for reading manifests and data files.
  TableScan(std::unique_ptr<ScanContext> context, std::shared_ptr<FileIO> file_io);

  /// \brief Plans the scan tasks by resolving manifests and data files.
  ///
  /// Returns a list of file scan tasks if successful.
  /// \return A Result containing scan tasks or an error.
  Result<std::vector<std::unique_ptr<FileScanTask>>> PlanFiles() const;

 private:
  /// \brief Creates a reader for the manifest list.
  /// \param file_path Path to the manifest list file.
  /// \return A Result containing the reader or an error.
  Result<std::unique_ptr<ManifestListReader>> CreateManifestListReader(
      const std::string& file_path) const;

  /// \brief Creates a reader for a manifest file.
  /// \param file_path Path to the manifest file.
  /// \return A Result containing the reader or an error.
  Result<std::unique_ptr<ManifestReader>> CreateManifestReader(
      const std::string& file_path) const;

  std::unique_ptr<ScanContext> context_;
  std::shared_ptr<FileIO> file_io_;
};

/// \brief Represents a task to scan a portion of a data file.
struct ICEBERG_EXPORT FileScanTask {
  std::string file_path_;                 ///< Path to the data file.
  uint64_t start_;                        ///< Start byte offset.
  uint64_t length_;                       ///< Length in bytes to scan.
  std::optional<uint64_t> record_count_;  ///< Optional number of records.
  DataFile::Content file_content_;        ///< Type of file content.
  FileFormatType file_format_;            ///< Format of the data file.
  std::shared_ptr<Schema> schema_;        ///< Projected schema.
  std::vector<int32_t> field_ids_;        ///< Field IDs to project.
  std::shared_ptr<Expression> filter_;    ///< Filter expression to apply.
};

}  // namespace iceberg
