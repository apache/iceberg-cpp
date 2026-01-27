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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

namespace iceberg {

/// \brief An abstract scan task.
class ICEBERG_EXPORT ScanTask {
 public:
  enum class Kind : uint8_t {
    kFileScanTask,
  };

  /// \brief The kind of scan task.
  virtual Kind kind() const = 0;

  /// \brief The number of bytes that should be read by this scan task.
  virtual int64_t size_bytes() const = 0;

  /// \brief The number of files that should be read by this scan task.
  virtual int32_t files_count() const = 0;

  /// \brief The number of rows that should be read by this scan task.
  virtual int64_t estimated_row_count() const = 0;

  virtual ~ScanTask();
};

/// \brief Task representing a data file and its corresponding delete files.
class ICEBERG_EXPORT FileScanTask : public ScanTask {
 public:
  /// \brief Construct with data file, delete files, and residual filter.
  ///
  /// \param data_file The data file to read.
  /// \param delete_files Delete files that apply to this data file.
  /// \param filter Optional residual filter to apply after reading.
  explicit FileScanTask(std::shared_ptr<DataFile> data_file,
                        std::vector<std::shared_ptr<DataFile>> delete_files = {},
                        std::shared_ptr<Expression> filter = nullptr);

  /// \brief The data file that should be read by this scan task.
  const std::shared_ptr<DataFile>& data_file() const { return data_file_; }

  /// \brief Delete files that apply to this data file.
  const std::vector<std::shared_ptr<DataFile>>& delete_files() const {
    return delete_files_;
  }

  /// \brief Residual filter to apply after reading.
  const std::shared_ptr<Expression>& residual_filter() const { return residual_filter_; }

  Kind kind() const override { return Kind::kFileScanTask; }
  int64_t size_bytes() const override;
  int32_t files_count() const override;
  int64_t estimated_row_count() const override;

  /// TODO(gangwu): move it to iceberg/data/task_scanner.h
  ///
  /// \brief Returns a C-ABI compatible ArrowArrayStream to read the data for this task.
  ///
  /// \param io The FileIO instance for accessing the file data.
  /// \param projected_schema The projected schema for reading the data.
  /// \return A Result containing an ArrowArrayStream, or an error on failure.
  Result<ArrowArrayStream> ToArrow(const std::shared_ptr<FileIO>& io,
                                   std::shared_ptr<Schema> projected_schema) const;

 private:
  std::shared_ptr<DataFile> data_file_;
  std::vector<std::shared_ptr<DataFile>> delete_files_;
  std::shared_ptr<Expression> residual_filter_;
};

namespace internal {

// Internal table scan context used by different scan implementations.
struct TableScanContext {
  std::optional<int64_t> snapshot_id;
  std::shared_ptr<Expression> filter;
  bool ignore_residuals{false};
  bool case_sensitive{true};
  bool return_column_stats{false};
  std::unordered_set<int32_t> columns_to_keep_stats;
  std::vector<std::string> selected_columns;
  std::shared_ptr<Schema> projected_schema;
  std::unordered_map<std::string, std::string> options;
  bool from_snapshot_id_inclusive{false};
  std::optional<int64_t> from_snapshot_id;
  std::optional<int64_t> to_snapshot_id;
  std::string branch{};
  std::optional<int64_t> min_rows_requested;

  // Validate the context parameters to see if they have conflicts.
  [[nodiscard]] Status Validate() const;
};

}  // namespace internal

/// \brief Builder class for creating TableScan instances.
class ICEBERG_EXPORT TableScanBuilder : public ErrorCollector {
 public:
  /// \brief Constructs a TableScanBuilder for the given table.
  /// \param metadata Current table metadata.
  /// \param io FileIO instance for reading manifests files.
  static Result<std::unique_ptr<TableScanBuilder>> Make(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io);

  /// \brief Update property that will override the table's behavior
  /// based on the incoming pair. Unknown properties will be ignored.
  /// \param key name of the table property to be overridden
  /// \param value value to override with
  TableScanBuilder& Option(std::string key, std::string value);

  /// \brief Set the projected schema.
  /// \param schema a projection schema
  TableScanBuilder& Project(std::shared_ptr<Schema> schema);

  /// \brief If data columns are selected via Select(), controls whether
  /// the match to the schema will be done with case sensitivity. Default is true.
  /// \param case_sensitive whether the scan is case-sensitive
  TableScanBuilder& CaseSensitive(bool case_sensitive);

  /// \brief Request this scan to load the column stats with each data file.
  ///
  /// Column stats include: value count, null value count, lower bounds, and upper bounds.
  TableScanBuilder& IncludeColumnStats();

  /// \brief Request this scan to load the column stats for the specific columns with each
  /// data file.
  ///
  /// Column stats include: value count, null value count, lower bounds, and upper bounds.
  ///
  /// \param requested_columns column names for which to keep the stats.
  TableScanBuilder& IncludeColumnStats(const std::vector<std::string>& requested_columns);

  /// \brief Request this scan to read the given data columns.
  ///
  /// This produces an expected schema that includes all fields that are either selected
  /// or used by this scan's filter expression.
  ///
  /// \param column_names column names from the table's schema
  TableScanBuilder& Select(const std::vector<std::string>& column_names);

  /// \brief Set the expression to filter data.
  /// \param filter a filter expression
  TableScanBuilder& Filter(std::shared_ptr<Expression> filter);

  /// \brief Request data filtering to files but not to rows in those files.
  TableScanBuilder& IgnoreResiduals();

  /// \brief Request this scan to return at least the given number of rows.
  ///
  /// This is used as a hint and is entirely optional in order to not have to return more
  /// rows than necessary. This may return fewer rows if the scan does not contain that
  /// many, or it may return more than requested.
  ///
  /// \param num_rows The minimum number of rows requested
  TableScanBuilder& MinRowsRequested(int64_t num_rows);

  /// \brief Request this scan to use the given snapshot by ID.
  /// \param snapshot_id a snapshot ID
  /// \note InvalidArgument will be returned if the snapshot cannot be found
  TableScanBuilder& UseSnapshot(int64_t snapshot_id);

  /// \brief Request this scan to use the given reference.
  /// \param ref reference
  /// \note InvalidArgument will be returned if a reference with the given name
  /// could not be found
  TableScanBuilder& UseRef(const std::string& ref);

  /// \brief Request this scan to use the most recent snapshot as of the given time
  /// in milliseconds on the branch in the scan or main if no branch is set.
  /// \param timestamp_millis a timestamp in milliseconds.
  /// \note InvalidArgument will be returned if the snapshot cannot be found or time
  /// travel is attempted on a tag
  TableScanBuilder& AsOfTime(int64_t timestamp_millis);

  /// \brief Instructs this scan to look for changes starting from a particular snapshot.
  ///
  /// If the start snapshot is not configured, it defaults to the oldest ancestor of the
  /// end snapshot (inclusive).
  ///
  /// \param from_snapshot_id the start snapshot ID
  /// \param inclusive whether the start snapshot is inclusive, default is false
  /// \note InvalidArgument will be returned if the start snapshot is not an ancestor of
  /// the end snapshot
  TableScanBuilder& FromSnapshot(int64_t from_snapshot_id, bool inclusive = false);

  /// \brief Instructs this scan to look for changes starting from a particular snapshot.
  ///
  /// If the start snapshot is not configured, it defaults to the oldest ancestor of the
  /// end snapshot (inclusive).
  ///
  /// \param ref the start ref name that points to a particular snapshot ID
  /// \param inclusive whether the start snapshot is inclusive, default is false
  /// \note InvalidArgument will be returned if the start snapshot is not an ancestor of
  /// the end snapshot
  TableScanBuilder& FromSnapshot(const std::string& ref, bool inclusive = false);

  /// \brief Instructs this scan to look for changes up to a particular snapshot
  /// (inclusive).
  ///
  /// If the end snapshot is not configured, it defaults to the current table snapshot
  /// (inclusive).
  ///
  /// \param to_snapshot_id the end snapshot ID (inclusive)
  TableScanBuilder& ToSnapshot(int64_t to_snapshot_id);

  /// \brief Instructs this scan to look for changes up to a particular snapshot ref
  /// (inclusive).
  ///
  /// If the end snapshot is not configured, it defaults to the current table snapshot
  /// (inclusive).
  ///
  /// \param ref the end snapshot Ref (inclusive)
  TableScanBuilder& ToSnapshot(const std::string& ref);

  /// \brief Use the specified branch
  /// \param branch the branch name
  TableScanBuilder& UseBranch(const std::string& branch);

  /// \brief Builds and returns a TableScan instance.
  /// \return A Result containing the TableScan or an error.
  Result<std::unique_ptr<TableScan>> Build();

 private:
  TableScanBuilder(std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io);

  // Return the schema bound to the specified snapshot.
  Result<std::reference_wrapper<const std::shared_ptr<Schema>>> ResolveSnapshotSchema();

  // Return whether current configuration indicates an incremental scan mode.
  bool IsIncrementalScan() const;

  std::shared_ptr<TableMetadata> metadata_;
  std::shared_ptr<FileIO> io_;
  internal::TableScanContext context_;
  std::shared_ptr<Schema> snapshot_schema_;
};

/// \brief Represents a configured scan operation on a table.
class ICEBERG_EXPORT TableScan {
 public:
  virtual ~TableScan();

  /// \brief Returns the table metadata being scanned.
  const std::shared_ptr<TableMetadata>& metadata() const;

  /// \brief Returns the snapshot to scan. If there is no snapshot, returns nullptr.
  Result<std::shared_ptr<Snapshot>> snapshot() const;

  /// \brief Returns the projected schema for the scan.
  Result<std::shared_ptr<Schema>> schema() const;

  /// \brief Returns the scan context.
  const internal::TableScanContext& context() const;

  /// \brief Returns the file I/O instance used for reading files.
  const std::shared_ptr<FileIO>& io() const;

  /// \brief Returns this scan's filter expression.
  const std::shared_ptr<Expression>& filter() const;

  /// \brief Returns whether this scan is case-sensitive.
  bool is_case_sensitive() const;

  /// \brief Plans the scan tasks by resolving manifests and data files.
  /// \return A Result containing scan tasks or an error.
  virtual Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles() const = 0;

 protected:
  TableScan(std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
            std::shared_ptr<FileIO> io, internal::TableScanContext context);

  Result<std::reference_wrapper<const std::shared_ptr<Schema>>> ResolveProjectedSchema()
      const;

  virtual const std::vector<std::string>& ScanColumns() const;

  const std::shared_ptr<TableMetadata> metadata_;
  const std::shared_ptr<Schema> schema_;
  const std::shared_ptr<FileIO> io_;
  const internal::TableScanContext context_;
  mutable std::shared_ptr<Schema> projected_schema_;
};

/// \brief A scan that reads data files and applies delete files to filter rows.
class ICEBERG_EXPORT DataTableScan : public TableScan {
 public:
  /// \brief Constructs a DataTableScan instance.
  static Result<std::unique_ptr<DataTableScan>> Make(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileIO> io, internal::TableScanContext context);

  /// \brief Plans the scan tasks by resolving manifests and data files.
  /// \return A Result containing scan tasks or an error.
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles() const override;

 protected:
  DataTableScan(std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
                std::shared_ptr<FileIO> io, internal::TableScanContext context);
};

}  // namespace iceberg
