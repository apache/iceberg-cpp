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

/// \file iceberg/manifest/manifest_reader.h
/// Data reader interface for manifest files.

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Read manifest entries from a manifest file.
class ICEBERG_EXPORT ManifestReader {
 public:
  virtual ~ManifestReader() = default;

  /// \brief Read all manifest entries in the manifest file.
  ///
  /// TODO(gangwu): provide a lazy-evaluated iterator interface for better performance.
  virtual Result<std::vector<ManifestEntry>> Entries() = 0;

  /// \brief Read only live (non-deleted) manifest entries.
  virtual Result<std::vector<ManifestEntry>> LiveEntries() = 0;

  /// \brief Select specific columns of data file to read from the manifest entries.
  ///
  /// \note Column names should match the names in `DataFile` schema. Unmatched names
  /// will be ignored.
  virtual ManifestReader& Select(const std::vector<std::string>& columns) = 0;

  /// \brief Filter manifest entries by partition filter.
  ///
  /// \note Unlike the Java implementation, this method does not combine new expressions
  /// with existing ones. Each call replaces the previous partition filter.
  virtual ManifestReader& FilterPartitions(std::shared_ptr<Expression> expr) = 0;

  /// \brief Filter manifest entries to a specific set of partitions.
  virtual ManifestReader& FilterPartitions(
      std::shared_ptr<class PartitionSet> partition_set) = 0;

  /// \brief Filter manifest entries by row-level filter.
  ///
  /// \note Unlike the Java implementation, this method does not combine new expressions
  /// with existing ones. Each call replaces the previous row filter.
  virtual ManifestReader& FilterRows(std::shared_ptr<Expression> expr) = 0;

  /// \brief Set case sensitivity for column name matching.
  virtual ManifestReader& CaseSensitive(bool case_sensitive) = 0;

  /// \brief Try to drop stats from returned DataFile objects.
  virtual ManifestReader& TryDropStats() = 0;

  /// \brief Determine whether stats should be dropped based on selected columns.
  ///
  /// Returns true if the selected columns do not include any stats columns, or only
  /// include record_count (which is a primitive, not a large map).
  static bool ShouldDropStats(const std::vector<std::string>& columns);

  /// \brief Creates a reader for a manifest file.
  /// \param manifest A ManifestFile object containing metadata about the manifest.
  /// \param file_io File IO implementation to use.
  /// \param schema Schema used to bind the partition type.
  /// \param spec Partition spec used for this manifest file.
  /// \return A Result containing the reader or an error.
  static Result<std::unique_ptr<ManifestReader>> Make(
      const ManifestFile& manifest, std::shared_ptr<FileIO> file_io,
      std::shared_ptr<Schema> schema, std::shared_ptr<PartitionSpec> spec);

  /// \brief Creates a reader for a manifest file.
  /// \param manifest_location Path to the manifest file.
  /// \param file_io File IO implementation to use.
  /// \param schema Schema used to bind the partition type.
  /// \param spec Partition spec used for this manifest file.
  /// \return A Result containing the reader or an error.
  static Result<std::unique_ptr<ManifestReader>> Make(
      std::string_view manifest_location, std::shared_ptr<FileIO> file_io,
      std::shared_ptr<Schema> schema, std::shared_ptr<PartitionSpec> spec);

  /// \brief Add stats columns to the column list if needed.
  static std::vector<std::string> WithStatsColumns(
      const std::vector<std::string>& columns);
};

/// \brief Read manifest files from a manifest list file.
class ICEBERG_EXPORT ManifestListReader {
 public:
  virtual ~ManifestListReader() = default;

  /// \brief Read all manifest files in the manifest list file.
  virtual Result<std::vector<ManifestFile>> Files() const = 0;

  /// \brief Get the metadata of the manifest list file.
  virtual Result<std::unordered_map<std::string, std::string>> Metadata() const = 0;

  /// \brief Creates a reader for the manifest list.
  /// \param manifest_list_location Path to the manifest list file.
  /// \param file_io File IO implementation to use.
  /// \return A Result containing the reader or an error.
  static Result<std::unique_ptr<ManifestListReader>> Make(
      std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io);
};

}  // namespace iceberg
