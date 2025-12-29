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

/// \file iceberg/manifest/rolling_manifest_writer.h
/// Rolling manifest writer that can produce multiple manifest files.

#include <functional>
#include <memory>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief A rolling manifest writer that can produce multiple manifest files.
class ICEBERG_EXPORT RollingManifestWriter {
 public:
  /// \brief Factory function type for creating ManifestWriter instances.
  using ManifestWriterFactory = std::function<Result<std::unique_ptr<ManifestWriter>>()>;

  /// \brief Construct a rolling manifest writer.
  /// \param manifest_writer_factory Factory function to create new ManifestWriter
  /// instances.
  /// \param target_file_size_in_bytes Target file size in bytes. When the current
  /// file reaches this size (and row count is a multiple of 250), a new file
  /// will be created.
  RollingManifestWriter(ManifestWriterFactory manifest_writer_factory,
                        int64_t target_file_size_in_bytes);

  ~RollingManifestWriter();

  /// \brief Add an added entry for a file.
  ///
  /// \param file a data file
  /// \return Status indicating success or failure
  /// \note The entry's snapshot ID will be this manifest's snapshot ID. The
  /// entry's data sequence number will be the provided data sequence number.
  /// The entry's file sequence number will be assigned at commit.
  Status WriteAddedEntry(std::shared_ptr<DataFile> file,
                         std::optional<int64_t> data_sequence_number = std::nullopt);

  /// \brief Add an existing entry for a file.
  ///
  /// \param file an existing data file
  /// \param file_snapshot_id snapshot ID when the data file was added to the table
  /// \param data_sequence_number a data sequence number of the file (assigned when
  /// the file was added)
  /// \param file_sequence_number a file sequence number (assigned when the file
  /// was added)
  /// \return Status indicating success or failure
  /// \note The original data and file sequence numbers, snapshot ID, which were
  /// assigned at commit, must be preserved when adding an existing entry.
  Status WriteExistingEntry(std::shared_ptr<DataFile> file, int64_t file_snapshot_id,
                            int64_t data_sequence_number,
                            std::optional<int64_t> file_sequence_number = std::nullopt);

  /// \brief Add a delete entry for a file.
  ///
  /// \param file a deleted data file
  /// \param data_sequence_number a data sequence number of the file (assigned when
  /// the file was added)
  /// \param file_sequence_number a file sequence number (assigned when the file
  /// was added)
  /// \return Status indicating success or failure
  /// \note The entry's snapshot ID will be this manifest's snapshot ID. However,
  /// the original data and file sequence numbers of the file must be preserved
  /// when the file is marked as deleted.
  Status WriteDeletedEntry(std::shared_ptr<DataFile> file, int64_t data_sequence_number,
                           std::optional<int64_t> file_sequence_number = std::nullopt);

  /// \brief Close the rolling manifest writer.
  Status Close();

  /// \brief Get the list of manifest files produced by this writer.
  /// \return A vector of ManifestFile objects
  /// \note Only valid after the writer is closed.
  Result<std::vector<ManifestFile>> ToManifestFiles() const;

 private:
  /// \brief Get or create the current writer, rolling to a new file if needed.
  /// \return The current ManifestWriter, or an error if creation fails
  Result<ManifestWriter*> CurrentWriter();

  /// \brief Check if we should roll to a new file.
  ///
  /// This method checks if the current file has reached the target size
  /// or the number of rows has reached the threshold. If so, it rolls to a new file.
  bool ShouldRollToNewFile() const;

  /// \brief Close the current writer and add its ManifestFile to the list.
  Status CloseCurrentWriter();

  /// \brief The number of rows after which to consider rolling to a new file.
  /// \note This aligned with Iceberg's Java impl.
  static constexpr int64_t kRowsDivisor = 250;

  ManifestWriterFactory manifest_writer_factory_;
  int64_t target_file_size_in_bytes_;
  std::vector<ManifestFile> manifest_files_;

  int64_t current_file_rows_{0};
  std::unique_ptr<ManifestWriter> current_writer_{nullptr};
  bool closed_{false};
};

}  // namespace iceberg
