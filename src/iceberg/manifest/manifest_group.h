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

/// \file iceberg/manifest/manifest_group.h
/// Coordinates reading manifest files and producing scan tasks.

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/delete_file_index.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

namespace iceberg {

/// \brief Context passed to task creation functions.
struct ICEBERG_EXPORT TaskContext {
 public:
  std::shared_ptr<PartitionSpec> spec;
  DeleteFileIndex* deletes;
  ResidualEvaluator* residuals;
  bool drop_stats;
  std::unordered_set<int32_t> columns_to_keep_stats;
};

/// \brief Coordinates reading manifest files and producing scan tasks.
class ICEBERG_EXPORT ManifestGroup : public ErrorCollector {
 public:
  /// \brief Construct a ManifestGroup with a list of manifests.
  ///
  /// \param io FileIO for reading manifest files.
  /// \param schema Current table schema.
  /// \param specs_by_id Mapping of partition spec ID to PartitionSpec.
  /// \param manifests List of manifest files to process.
  static Result<std::unique_ptr<ManifestGroup>> Make(
      std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
      std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id_,
      std::vector<ManifestFile> manifests);

  /// \brief Construct a ManifestGroup with pre-separated manifests.
  ///
  /// \param io FileIO for reading manifest files.
  /// \param schema Current table schema.
  /// \param specs_by_id Mapping of partition spec ID to PartitionSpec.
  /// \param data_manifests List of data manifest files.
  /// \param delete_manifests List of delete manifest files.
  static Result<std::unique_ptr<ManifestGroup>> Make(
      std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
      std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
      std::vector<ManifestFile> data_manifests,
      std::vector<ManifestFile> delete_manifests);

  ~ManifestGroup() override;

  ManifestGroup(ManifestGroup&&) noexcept;
  ManifestGroup& operator=(ManifestGroup&&) noexcept;
  ManifestGroup(const ManifestGroup&) = delete;
  ManifestGroup& operator=(const ManifestGroup&) = delete;

  /// \brief Set a row-level data filter.
  ManifestGroup& FilterData(std::shared_ptr<Expression> filter);

  /// \brief Set a filter that is evaluated against each DataFile's metadata.
  ManifestGroup& FilterFiles(std::shared_ptr<Expression> filter);

  /// \brief Set a partition filter expression.
  ManifestGroup& FilterPartitions(std::shared_ptr<Expression> filter);

  /// \brief Set a custom manifest entry filter predicate.
  ///
  /// \param predicate A function that returns true if the entry should be included.
  ManifestGroup& FilterManifestEntries(
      std::function<bool(const ManifestEntry&)> predicate);

  /// \brief Ignore deleted entries in manifests.
  ManifestGroup& IgnoreDeleted();

  /// \brief Ignore existing entries in manifests.
  ManifestGroup& IgnoreExisting();

  /// \brief Ignore residual filter computation.
  ManifestGroup& IgnoreResiduals();

  /// \brief Select specific columns from manifest entries.
  ///
  /// \param columns Column names to select from manifest entries.
  ManifestGroup& Select(std::vector<std::string> columns);

  /// \brief Set case sensitivity for column name matching.
  ManifestGroup& CaseSensitive(bool case_sensitive);

  /// \brief Specify columns that should retain their statistics.
  ///
  /// \param column_ids Field IDs of columns whose statistics should be preserved.
  ManifestGroup& ColumnsToKeepStats(std::unordered_set<int32_t> column_ids);

  /// \brief Plan scan tasks for all matching data files.
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles();

  /// \brief Get all matching manifest entries.
  Result<std::vector<ManifestEntry>> Entries();

  using CreateTasksFunction =
      std::function<Result<std::vector<std::shared_ptr<ScanTask>>>(
          std::vector<ManifestEntry>&&, const TaskContext&)>;

  /// \brief Plan tasks using a custom task creation function.
  ///
  /// \param create_tasks A function that creates ScanTasks from entries and context.
  /// \return A list of ScanTask objects, or error on failure.
  Result<std::vector<std::shared_ptr<ScanTask>>> Plan(
      const CreateTasksFunction& create_tasks);

 private:
  ManifestGroup(std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
                std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
                std::vector<ManifestFile> data_manifests,
                DeleteFileIndex::Builder&& delete_index_builder);

  Result<std::unordered_map<int32_t, std::vector<ManifestEntry>>> ReadEntries();

  Result<std::unique_ptr<ManifestReader>> MakeReader(const ManifestFile& manifest);

  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Schema> schema_;
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id_;
  std::vector<ManifestFile> data_manifests_;
  DeleteFileIndex::Builder delete_index_builder_;
  std::shared_ptr<Expression> data_filter_;
  std::shared_ptr<Expression> file_filter_;
  std::shared_ptr<Expression> partition_filter_;
  std::function<bool(const ManifestEntry&)> manifest_entry_predicate_;
  std::vector<std::string> columns_;
  std::unordered_set<int32_t> columns_to_keep_stats_;
  bool case_sensitive_ = true;
  bool ignore_deleted_ = false;
  bool ignore_existing_ = false;
  bool ignore_residuals_ = false;
};

}  // namespace iceberg
