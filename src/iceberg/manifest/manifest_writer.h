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

/// \file iceberg/manifest/manifest_writer.h
/// Data writer interface for manifest files and manifest list files.

#include <memory>
#include <string>
#include <vector>

#include "iceberg/file_writer.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/metrics.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Write manifest entries to a manifest file.
class ICEBERG_EXPORT ManifestWriter {
 public:
  ~ManifestWriter();

  /// \brief Write the entry that all its fields are populated correctly.
  /// \param entry Manifest entry to write.
  /// \return Status::OK() if entry was written successfully
  /// \note All other write entry variants delegate to this method after populating
  /// the necessary fields.
  Status WriteEntry(const ManifestEntry& entry);

  /// \brief Add an added entry for a file with a specific sequence number.
  ///
  /// \param file an added data file
  /// \param data_sequence_number a data sequence number for the file
  /// \return Status::OK() if the entry was written successfully
  /// \note The entry's snapshot ID will be this manifest's snapshot ID. The entry's data
  /// sequence number will be the provided data sequence number. The entry's file sequence
  /// number will be assigned at commit.
  Status WriteAddedEntry(std::shared_ptr<DataFile> file,
                         std::optional<int64_t> data_sequence_number = std::nullopt);
  /// \brief Add a new entry to the manifest.
  /// The method populates the snapshot id and status fields of the entry.
  Status WriteAddedEntry(const ManifestEntry& entry);

  /// \brief Add an existing entry for a file.
  /// \param file an existing data file
  /// \param file_snapshot_id snapshot ID when the data file was added to the table
  /// \param data_sequence_number a data sequence number of the file (assigned when the
  /// file was added)
  /// \param file_sequence_number a file sequence number (assigned when the file was
  /// added)
  /// \return Status::OK() if the entry was written successfully
  /// \note The original data and file sequence numbers, snapshot ID, which were assigned
  /// at commit, must be preserved when adding an existing entry.
  Status WriteExistingEntry(std::shared_ptr<DataFile> file, int64_t file_snapshot_id,
                            int64_t data_sequence_number,
                            std::optional<int64_t> file_sequence_number = std::nullopt);
  /// \brief Add an existing entry to the manifest.
  /// The method populates the status field of the entry.
  Status WriteExistingEntry(const ManifestEntry& entry);

  /// \brief Add a delete entry for a file.
  /// \param file a deleted data file
  /// \param data_sequence_number a data sequence number of the file (assigned when the
  /// file was added)
  /// \param file_sequence_number a file sequence number (assigned when the file was
  /// added)
  /// \return Status::OK() if the entry was written successfully
  /// \note The entry's snapshot ID will be this manifest's snapshot ID. However, the
  /// original data and file sequence numbers of the file must be preserved when the file
  /// is marked as deleted.
  Status WriteDeletedEntry(std::shared_ptr<DataFile> file, int64_t data_sequence_number,
                           std::optional<int64_t> file_sequence_number = std::nullopt);
  /// \brief Add a deleted entry to the manifest.
  /// The method populates the snapshot id and status fields of the entry.
  Status WriteDeletedEntry(const ManifestEntry& entry);

  /// \brief Write manifest entries to file.
  /// \param entries Already populated manifest entries to write.
  /// \return Status::OK() if all entries were written successfully
  Status AddAll(const std::vector<ManifestEntry>& entries);

  /// \brief Close writer and flush to storage.
  Status Close();

  /// \brief Get the content of the manifest.
  ManifestContent content() const;

  /// \brief Get the metrics of written manifest file.
  /// \note Only valid after the file is closed.
  Result<Metrics> metrics() const;

  /// \brief Get the ManifestFile object.
  /// \note Only valid after the file is closed.
  Result<ManifestFile> ToManifestFile() const;

  /// \brief Creates a writer for a manifest file.
  /// \param snapshot_id ID of the snapshot.
  /// \param manifest_location Path to the manifest file.
  /// \param file_io File IO implementation to use.
  /// \param partition_spec Partition spec for the manifest.
  /// \param current_schema Current table schema.
  /// \return A Result containing the writer or an error.
  static Result<std::unique_ptr<ManifestWriter>> MakeV1Writer(
      std::optional<int64_t> snapshot_id, std::string_view manifest_location,
      std::shared_ptr<FileIO> file_io, std::shared_ptr<PartitionSpec> partition_spec,
      std::shared_ptr<Schema> current_schema);

  /// \brief Creates a writer for a manifest file.
  /// \param snapshot_id ID of the snapshot.
  /// \param manifest_location Path to the manifest file.
  /// \param file_io File IO implementation to use.
  /// \param partition_spec Partition spec for the manifest.
  /// \param current_schema Schema containing the source fields referenced by partition
  /// spec.
  /// \param content Content of the manifest.
  /// \return A Result containing the writer or an error.
  static Result<std::unique_ptr<ManifestWriter>> MakeV2Writer(
      std::optional<int64_t> snapshot_id, std::string_view manifest_location,
      std::shared_ptr<FileIO> file_io, std::shared_ptr<PartitionSpec> partition_spec,
      std::shared_ptr<Schema> current_schema, ManifestContent content);

  /// \brief Creates a writer for a manifest file.
  /// \param snapshot_id ID of the snapshot.
  /// \param first_row_id First row ID of the snapshot.
  /// \param manifest_location Path to the manifest file.
  /// \param file_io File IO implementation to use.
  /// \param partition_spec Partition spec for the manifest.
  /// \param current_schema Schema containing the source fields referenced by partition
  /// spec.
  /// \param content Content of the manifest.
  /// \return A Result containing the writer or an error.
  static Result<std::unique_ptr<ManifestWriter>> MakeV3Writer(
      std::optional<int64_t> snapshot_id, std::optional<int64_t> first_row_id,
      std::string_view manifest_location, std::shared_ptr<FileIO> file_io,
      std::shared_ptr<PartitionSpec> partition_spec,
      std::shared_ptr<Schema> current_schema, ManifestContent content);

 private:
  // Private constructor for internal use only, use the static Make*Writer methods
  // instead.
  ManifestWriter(std::unique_ptr<Writer> writer,
                 std::unique_ptr<class ManifestEntryAdapter> adapter,
                 std::string_view manifest_location, std::optional<int64_t> first_row_id);

  Status CheckDataFile(const DataFile& file) const;

  static constexpr int64_t kBatchSize = 1024;
  std::unique_ptr<Writer> writer_;
  std::unique_ptr<class ManifestEntryAdapter> adapter_;
  bool closed_{false};
  std::string manifest_location_;
  std::optional<int64_t> first_row_id_;

  int32_t add_files_count_{0};
  int32_t existing_files_count_{0};
  int32_t delete_files_count_{0};
  int64_t add_rows_count_{0L};
  int64_t existing_rows_count_{0L};
  int64_t delete_rows_count_{0L};
  std::optional<int64_t> min_sequence_number_{std::nullopt};
  std::unique_ptr<PartitionSummary> partition_summary_;
};

/// \brief Write manifest files to a manifest list file.
class ICEBERG_EXPORT ManifestListWriter {
 public:
  ~ManifestListWriter();

  /// \brief Write manifest file to manifest list file.
  /// \param file Manifest file to write.
  /// \return Status::OK() if file was written successfully
  Status Add(const ManifestFile& file);

  /// \brief Write manifest file list to manifest list file.
  /// \param files Manifest file list to write.
  /// \return Status::OK() if all files were written successfully
  Status AddAll(const std::vector<ManifestFile>& files);

  /// \brief Close writer and flush to storage.
  Status Close();

  /// \brief Get the next row id to assign.
  std::optional<int64_t> next_row_id() const;

  /// \brief Creates a writer for the v1 manifest list.
  /// \param snapshot_id ID of the snapshot.
  /// \param parent_snapshot_id ID of the parent snapshot.
  /// \param manifest_list_location Path to the manifest list file.
  /// \param file_io File IO implementation to use.
  /// \return A Result containing the writer or an error.
  static Result<std::unique_ptr<ManifestListWriter>> MakeV1Writer(
      int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
      std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io);

  /// \brief Creates a writer for the manifest list.
  /// \param snapshot_id ID of the snapshot.
  /// \param parent_snapshot_id ID of the parent snapshot.
  /// \param sequence_number Sequence number of the snapshot.
  /// \param manifest_list_location Path to the manifest list file.
  /// \param file_io File IO implementation to use.
  /// \return A Result containing the writer or an error.
  static Result<std::unique_ptr<ManifestListWriter>> MakeV2Writer(
      int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
      int64_t sequence_number, std::string_view manifest_list_location,
      std::shared_ptr<FileIO> file_io);

  /// \brief Creates a writer for the manifest list.
  /// \param snapshot_id ID of the snapshot.
  /// \param parent_snapshot_id ID of the parent snapshot.
  /// \param sequence_number Sequence number of the snapshot.
  /// \param first_row_id First row ID of the snapshot.
  /// \param manifest_list_location Path to the manifest list file.
  /// \param file_io File IO implementation to use.
  /// \return A Result containing the writer or an error.
  static Result<std::unique_ptr<ManifestListWriter>> MakeV3Writer(
      int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
      int64_t sequence_number, int64_t first_row_id,
      std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io);

 private:
  // Private constructor for internal use only, use the static Make*Writer methods
  // instead.
  ManifestListWriter(std::unique_ptr<Writer> writer,
                     std::unique_ptr<class ManifestFileAdapter> adapter);

  static constexpr int64_t kBatchSize = 1024;
  std::unique_ptr<Writer> writer_;
  std::unique_ptr<class ManifestFileAdapter> adapter_;
};

}  // namespace iceberg
