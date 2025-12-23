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

/// \file iceberg/manifest/manifest_reader_internal.h
/// Reader implementation for manifest list files and manifest files.

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iceberg/expression/evaluator.h"
#include "iceberg/expression/expression.h"
#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/file_reader.h"
#include "iceberg/inheritable_metadata.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief Read manifest entries from a manifest file.
///
/// This implementation supports lazy reader creation and filtering based on
/// partition expressions, row expressions, and partition sets. Following the
/// Java implementation pattern.
class ManifestReaderImpl : public ManifestReader {
 public:
  /// \brief Construct a ManifestReaderImpl for lazy initialization.
  ///
  /// \param manifest_path Path to the manifest file.
  /// \param manifest_length Length of the manifest file (optional).
  /// \param file_io File IO implementation.
  /// \param schema Table schema.
  /// \param spec Partition spec.
  /// \param inheritable_metadata Metadata inherited from manifest.
  /// \param first_row_id First row ID for V3 manifests.
  /// \note ManifestReader::Make() functions should guarantee non-null parameters.
  ManifestReaderImpl(std::string manifest_path, std::optional<int64_t> manifest_length,
                     std::shared_ptr<FileIO> file_io, std::shared_ptr<Schema> schema,
                     std::shared_ptr<PartitionSpec> spec,
                     std::unique_ptr<InheritableMetadata> inheritable_metadata,
                     std::optional<int64_t> first_row_id);

  Result<std::vector<ManifestEntry>> Entries() override;

  Result<std::vector<ManifestEntry>> LiveEntries() override;

  ManifestReader& Select(const std::vector<std::string>& columns) override;

  ManifestReader& FilterPartitions(std::shared_ptr<Expression> expr) override;

  ManifestReader& FilterPartitions(std::shared_ptr<PartitionSet> partition_set) override;

  ManifestReader& FilterRows(std::shared_ptr<Expression> expr) override;

  ManifestReader& CaseSensitive(bool case_sensitive) override;

 private:
  /// \brief Read entries with optional live-only filtering.
  Result<std::vector<ManifestEntry>> ReadEntries(bool only_live);

  /// \brief Lazily open the underlying Avro reader with appropriate schema projection.
  Status OpenReader(std::shared_ptr<Schema> projection);

  /// \brief Check if there's a non-trivial partition filter.
  bool HasPartitionFilter() const;

  /// \brief Check if there's a non-trivial row filter.
  bool HasRowFilter() const;

  /// \brief Get or create the partition evaluator.
  Result<Evaluator*> GetEvaluator();

  /// \brief Get or create the metrics evaluator.
  Result<InclusiveMetricsEvaluator*> GetMetricsEvaluator();

  /// \brief Check if a partition is in the partition set.
  bool InPartitionSet(const DataFile& file) const;

  // Fields set at construction
  const std::string manifest_path_;
  const std::optional<int64_t> manifest_length_;
  const std::shared_ptr<FileIO> file_io_;
  const std::shared_ptr<Schema> schema_;
  const std::shared_ptr<PartitionSpec> spec_;
  const std::unique_ptr<InheritableMetadata> inheritable_metadata_;
  std::optional<int64_t> first_row_id_;

  // Configuration fields
  std::vector<std::string> columns_;
  std::shared_ptr<Expression> part_filter_{True::Instance()};
  std::shared_ptr<Expression> row_filter_{True::Instance()};
  std::shared_ptr<PartitionSet> partition_set_;
  bool case_sensitive_{true};

  // Lazy fields
  std::unique_ptr<Reader> file_reader_;
  std::shared_ptr<Schema> file_schema_;
  std::unique_ptr<Evaluator> evaluator_;
  std::unique_ptr<InclusiveMetricsEvaluator> metrics_evaluator_;
};

/// \brief Read manifest files from a manifest list file.
class ManifestListReaderImpl : public ManifestListReader {
 public:
  explicit ManifestListReaderImpl(std::unique_ptr<Reader> reader,
                                  std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)), reader_(std::move(reader)) {}

  Result<std::vector<ManifestFile>> Files() const override;

  Result<std::unordered_map<std::string, std::string>> Metadata() const override;

 private:
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<Reader> reader_;
};

enum class ManifestFileField : int32_t {
  kManifestPath = 0,
  kManifestLength = 1,
  kPartitionSpecId = 2,
  kContent = 3,
  kSequenceNumber = 4,
  kMinSequenceNumber = 5,
  kAddedSnapshotId = 6,
  kAddedFilesCount = 7,
  kExistingFilesCount = 8,
  kDeletedFilesCount = 9,
  kAddedRowsCount = 10,
  kExistingRowsCount = 11,
  kDeletedRowsCount = 12,
  kPartitionFieldSummary = 13,
  kKeyMetadata = 14,
  kFirstRowId = 15,
  // kNextUnusedId is the placeholder for the next unused index.
  // Always keep this as the last index when adding new fields.
  kNextUnusedId = 16,
};

Result<ManifestFileField> ManifestFileFieldFromIndex(int32_t index);

}  // namespace iceberg
