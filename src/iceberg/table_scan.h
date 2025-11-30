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

#include "iceberg/arrow_c_data.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief An abstract scan task.
class ICEBERG_EXPORT ScanTask {
 public:
  virtual ~ScanTask() = default;

  /// \brief The number of bytes that should be read by this scan task.
  virtual int64_t size_bytes() const = 0;

  /// \brief The number of files that should be read by this scan task.
  virtual int32_t files_count() const = 0;

  /// \brief The number of rows that should be read by this scan task.
  virtual int64_t estimated_row_count() const = 0;
};

/// \brief Task representing a data file and its corresponding delete files.
class ICEBERG_EXPORT FileScanTask : public ScanTask {
 public:
  explicit FileScanTask(std::shared_ptr<DataFile> data_file);

  /// \brief The data file that should be read by this scan task.
  const std::shared_ptr<DataFile>& data_file() const;

  int64_t size_bytes() const override;
  int32_t files_count() const override;
  int64_t estimated_row_count() const override;

  /**
   * \brief Returns a C-ABI compatible ArrowArrayStream to read the data for this task.
   *
   * \param io The FileIO instance for accessing the file data.
   * \param projected_schema The projected schema for reading the data.
   * \param filter Optional filter expression to apply during reading.
   * \return A Result containing an ArrowArrayStream, or an error on failure.
   */
  Result<ArrowArrayStream> ToArrow(const std::shared_ptr<FileIO>& io,
                                   const std::shared_ptr<Schema>& projected_schema,
                                   const std::shared_ptr<Expression>& filter) const;

 private:
  /// \brief Data file metadata.
  std::shared_ptr<DataFile> data_file_;
};

/// \brief Scan context holding snapshot and scan-specific metadata.
struct TableScanContext {
  /// \brief Table metadata.
  std::shared_ptr<TableMetadata> table_metadata;
  /// \brief Snapshot to scan.
  std::shared_ptr<Snapshot> snapshot;
  /// \brief Projected schema.
  std::shared_ptr<Schema> projected_schema;
  /// \brief Filter expression to apply.
  std::shared_ptr<Expression> filter;
  /// \brief Whether the scan is case-sensitive.
  bool case_sensitive = false;
  /// \brief Additional options for the scan.
  std::unordered_map<std::string, std::string> options;
  /// \brief Optional limit on the number of rows to scan.
  std::optional<int64_t> limit;
};

/// \brief Builder class for creating TableScan instances.
class ICEBERG_EXPORT TableScanBuilder {
 public:
  /// \brief Constructs a TableScanBuilder for the given table.
  /// \param table_metadata The metadata of the table to scan.
  /// \param file_io The FileIO instance for reading manifests and data files.
  explicit TableScanBuilder(std::shared_ptr<TableMetadata> table_metadata,
                            std::shared_ptr<FileIO> file_io);

  /// \brief Sets the snapshot ID to scan.
  /// \param snapshot_id The ID of the snapshot.
  /// \return Reference to the builder.
  TableScanBuilder& WithSnapshotId(int64_t snapshot_id);

  /// \brief Selects columns to include in the scan.
  /// \param column_names A list of column names. If empty, all columns will be selected.
  /// \return Reference to the builder.
  TableScanBuilder& WithColumnNames(std::vector<std::string> column_names);

  /// \brief Sets the schema to use for the scan.
  /// \param schema The schema to use.
  /// \return Reference to the builder.
  TableScanBuilder& WithProjectedSchema(std::shared_ptr<Schema> schema);

  /// \brief Applies a filter expression to the scan.
  /// \param filter Filter expression to use.
  /// \return Reference to the builder.
  TableScanBuilder& WithFilter(std::shared_ptr<Expression> filter);

  /// \brief Sets whether the scan should be case-sensitive.
  /// \param case_sensitive Whether the scan is case-sensitive.
  /// /return Reference to the builder.
  TableScanBuilder& WithCaseSensitive(bool case_sensitive);

  /// \brief Sets an option for the scan.
  /// \param property The name of the option.
  /// \param value The value of the option.
  /// \return Reference to the builder.
  TableScanBuilder& WithOption(std::string property, std::string value);

  /// \brief Sets an optional limit on the number of rows to scan.
  /// \param limit Optional limit on the number of rows.
  /// \return Reference to the builder.
  TableScanBuilder& WithLimit(std::optional<int64_t> limit);

  /// \brief Builds and returns a TableScan instance.
  /// \return A Result containing the TableScan or an error.
  Result<std::unique_ptr<TableScan>> Build();

 private:
  /// \brief the file I/O instance for reading manifests and data files.
  std::shared_ptr<FileIO> file_io_;
  /// \brief column names to project in the scan.
  std::vector<std::string> column_names_;
  /// \brief snapshot ID to scan, if specified.
  std::optional<int64_t> snapshot_id_;
  /// \brief Context for the scan, including snapshot, schema, and filter.
  TableScanContext context_;
};

/// \brief Represents a configured scan operation on a table.
class ICEBERG_EXPORT TableScan {
 public:
  virtual ~TableScan() = default;

  /// \brief Constructs a TableScan with the given context and file I/O.
  /// \param context Scan context including snapshot, schema, and filter.
  /// \param file_io File I/O instance for reading manifests and data files.
  TableScan(TableScanContext context, std::shared_ptr<FileIO> file_io);

  /// \brief Returns the snapshot being scanned.
  /// \return A shared pointer to the snapshot.
  const std::shared_ptr<Snapshot>& snapshot() const;

  /// \brief Returns the projected schema for the scan.
  /// \return A shared pointer to the projected schema.
  const std::shared_ptr<Schema>& projection() const;

  /// \brief Returns the scan context.
  /// \return A reference to the TableScanContext.
  const TableScanContext& context() const;

  /// \brief Returns the file I/O instance used for reading manifests and data files.
  /// \return A shared pointer to the FileIO instance.
  const std::shared_ptr<FileIO>& io() const;

  /// \brief Plans the scan tasks by resolving manifests and data files.
  /// \return A Result containing scan tasks or an error.
  virtual Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles() const = 0;

 protected:
  /// \brief context for the scan, including snapshot, schema, and filter.
  const TableScanContext context_;
  /// \brief File I/O instance for reading manifests and data files.
  std::shared_ptr<FileIO> file_io_;
};

/// \brief A scan that reads data files and applies delete files to filter rows.
class ICEBERG_EXPORT DataTableScan : public TableScan {
 public:
  /// \brief Constructs a DataScan with the given context and file I/O.
  DataTableScan(TableScanContext context, std::shared_ptr<FileIO> file_io);

  /// \brief Plans the scan tasks by resolving manifests and data files.
  /// \return A Result containing scan tasks or an error.
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles() const override;
};

}  // namespace iceberg
