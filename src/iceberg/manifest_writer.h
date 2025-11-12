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

/// \file iceberg/manifest_writer.h
/// Data writer interface for manifest files and manifest list files.

#include <memory>
#include <string>
#include <vector>

#include "iceberg/file_writer.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/manifest_adapter.h"
#include "iceberg/metrics.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Write manifest entries to a manifest file.
class ICEBERG_EXPORT ManifestWriter {
 public:
  ManifestWriter(std::unique_ptr<Writer> writer,
                 std::unique_ptr<ManifestEntryAdapter> adapter,
                 std::string_view manifest_location)
      : writer_(std::move(writer)),
        adapter_(std::move(adapter)),
        manifest_location_(manifest_location) {}

  ~ManifestWriter() = default;

  /// \brief Add a new Manifest entry.
  /// \param entry Manifest entry to write.
  /// \return Status::OK() if entry was written successfully
  Status AddEntry(const ManifestEntry& entry);

  /// \brief Add an existing Manifest entry.
  /// \param entry Manifest entry to write.
  /// \return Status::OK() if entry was written successfully
  Status AddExisting(const ManifestEntry& entry);

  /// \brief Add a delete Manifest entry.
  /// \param entry Manifest entry to write.
  /// \return Status::OK() if entry was written successfully
  Status AddDelete(const ManifestEntry& entry);

  /// \brief Write manifest entries to file.
  /// \param entries Manifest entries to write.
  /// \return Status::OK() if all entries were written successfully
  Status AddAll(const std::vector<ManifestEntry>& entries);

  /// \brief Close writer and flush to storage.
  Status Close();

  /// \brief Get the metrics of written manifest file.
  std::optional<Metrics> metrics() const;

  /// \brief Get the content of the manifest.
  ManifestContent content() const;

  /// \brief Get the ManifestFile object.
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
  static constexpr int64_t kBatchSize = 1024;
  std::unique_ptr<Writer> writer_;
  std::unique_ptr<ManifestEntryAdapter> adapter_;
  bool closed_{false};
  std::string manifest_location_;
  // TODO(zhjwpku): consider passing key metadata when makeing ManifestWriter.
  std::vector<uint8_t> key_metadata_;
};

/// \brief Write manifest files to a manifest list file.
class ICEBERG_EXPORT ManifestListWriter {
 public:
  ManifestListWriter(std::unique_ptr<Writer> writer,
                     std::unique_ptr<ManifestFileAdapter> adapter)
      : writer_(std::move(writer)), adapter_(std::move(adapter)) {}

  ~ManifestListWriter() = default;

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
  static constexpr int64_t kBatchSize = 1024;
  std::unique_ptr<Writer> writer_;
  std::unique_ptr<ManifestFileAdapter> adapter_;
};

}  // namespace iceberg
