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

/// \file iceberg/internal/manifest_reader_internal.h
/// Reader implementation for manifest list files and manifest files.

#include "iceberg/file_reader.h"
#include "iceberg/inheritable_metadata.h"
#include "iceberg/manifest_reader.h"

namespace iceberg {

/// \brief Read manifest entries from a manifest file.
class ManifestReaderImpl : public ManifestReader {
 public:
  explicit ManifestReaderImpl(std::unique_ptr<Reader> reader,
                              std::shared_ptr<Schema> schema,
                              std::unique_ptr<InheritableMetadata> inheritable_metadata,
                              std::optional<int64_t> first_row_id)
      : schema_(std::move(schema)),
        reader_(std::move(reader)),
        inheritable_metadata_(std::move(inheritable_metadata)),
        first_row_id_(first_row_id) {}

  Result<std::vector<ManifestEntry>> Entries() const override;

  Result<std::unordered_map<std::string, std::string>> Metadata() const override;

 private:
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<Reader> reader_;
  std::unique_ptr<InheritableMetadata> inheritable_metadata_;
  mutable std::optional<int64_t> first_row_id_;
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
