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

/// \file iceberg/internal/manifest_writer_internal.h
/// Writer implementation for manifest list files and manifest files.

#include "iceberg/manifest_writer.h"
#include "iceberg/v1_metadata.h"
#include "iceberg/v2_metadata.h"
#include "iceberg/v3_metadata.h"

namespace iceberg {

/// \brief Write manifest entries to a manifest file.
class ManifestWriterImpl : public ManifestWriter {
 public:
  explicit ManifestWriterImpl(int64_t snapshot_id, std::unique_ptr<Writer> writer,
                              std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)), writer_(std::move(writer)) {}

 protected:
  virtual ManifestEntry prepare(const ManifestEntry& entry) = 0;

 private:
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<Writer> writer_;
};

/// \brief Write v1 manifest entries to a manifest file.
class ManifestWriterV1 : public ManifestWriterImpl {
 public:
  explicit ManifestWriterV1(int64_t snapshot_id, std::unique_ptr<Writer> writer,
                            std::shared_ptr<Schema> schema)
      : ManifestWriterImpl(snapshot_id, std::move(writer), std::move(schema)) {}

  Status Add(const ManifestEntry& entry) override;
  Status AddAll(const std::vector<ManifestEntry>& entries) override;
  Status Close() override;

 protected:
  ManifestEntry prepare(const ManifestEntry& entry) override;

 private:
  V1MetaData::ManifestEntryWrapper wrapper_;
};

/// \brief Write v2 manifest entries to a manifest file.
class ManifestWriterV2 : public ManifestWriterImpl {
 public:
  explicit ManifestWriterV2(int64_t snapshot_id, std::unique_ptr<Writer> writer,
                            std::shared_ptr<Schema> schema)
      : ManifestWriterImpl(snapshot_id, std::move(writer), std::move(schema)),
        wrapper_(snapshot_id) {}

  Status Add(const ManifestEntry& entry) override;
  Status AddAll(const std::vector<ManifestEntry>& entries) override;

  Status Close() override;

 protected:
  ManifestEntry prepare(const ManifestEntry& entry) override;

 private:
  V2MetaData::ManifestEntryWrapper wrapper_;
};

/// \brief Write v3 manifest entries to a manifest file.
class ManifestWriterV3 : public ManifestWriterImpl {
 public:
  explicit ManifestWriterV3(int64_t snapshot_id, int64_t first_row_id,
                            std::unique_ptr<Writer> writer,
                            std::shared_ptr<Schema> schema)
      : ManifestWriterImpl(snapshot_id, std::move(writer), std::move(schema)),
        wrapper_(snapshot_id) {}

  Status Add(const ManifestEntry& entry) override;
  Status AddAll(const std::vector<ManifestEntry>& entries) override;

  Status Close() override;

 protected:
  ManifestEntry prepare(const ManifestEntry& entry) override;

 private:
  V3MetaData::ManifestEntryWrapper wrapper_;
};

/// \brief Write manifest files to a manifest list file.
class ManifestListWriterImpl : public ManifestListWriter {
 public:
  explicit ManifestListWriterImpl(int64_t snapshot_id, int64_t parent_snapshot_id,
                                  std::unique_ptr<Writer> writer,
                                  std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)), writer_(std::move(writer)) {}

 protected:
  virtual ManifestFile prepare(const ManifestFile& file) = 0;

 private:
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<Writer> writer_;
};

/// \brief Write v1 manifest files to a manifest list file.
class ManifestListWriterV1 : public ManifestListWriterImpl {
 public:
  explicit ManifestListWriterV1(int64_t snapshot_id, int64_t parent_snapshot_id,

                                std::unique_ptr<Writer> writer,
                                std::shared_ptr<Schema> schema)
      : ManifestListWriterImpl(snapshot_id, parent_snapshot_id, std::move(writer),
                               std::move(schema)) {}

  Status Add(const ManifestFile& file) override;
  Status AddAll(const std::vector<ManifestFile>& files) override;
  Status Close() override;

 protected:
  ManifestFile prepare(const ManifestFile& file) override;

 private:
  V1MetaData::ManifestFileWrapper wrapper_;
};

/// \brief Write v2 manifest files to a manifest list file.
class ManifestListWriterV2 : public ManifestListWriterImpl {
 public:
  explicit ManifestListWriterV2(int64_t snapshot_id, int64_t parent_snapshot_id,
                                int64_t sequence_number, std::unique_ptr<Writer> writer,
                                std::shared_ptr<Schema> schema)
      : ManifestListWriterImpl(snapshot_id, parent_snapshot_id, std::move(writer),
                               std::move(schema)),
        wrapper_(snapshot_id, sequence_number) {}

  Status Add(const ManifestFile& file) override;
  Status AddAll(const std::vector<ManifestFile>& files) override;

  Status Close() override;

 protected:
  ManifestFile prepare(const ManifestFile& file) override;

 private:
  V2MetaData::ManifestFileWrapper wrapper_;
};

/// \brief Write v3 manifest files to a manifest list file.
class ManifestListWriterV3 : public ManifestListWriterImpl {
 public:
  explicit ManifestListWriterV3(int64_t snapshot_id, int64_t parent_snapshot_id,
                                int64_t sequence_number, int64_t first_row_id,
                                std::unique_ptr<Writer> writer,
                                std::shared_ptr<Schema> schema)
      : ManifestListWriterImpl(snapshot_id, parent_snapshot_id, std::move(writer),
                               std::move(schema)),
        wrapper_(snapshot_id, sequence_number) {}

  Status Add(const ManifestFile& file) override;
  Status AddAll(const std::vector<ManifestFile>& files) override;

  Status Close() override;

 protected:
  ManifestFile prepare(const ManifestFile& file) override;

 private:
  int64_t next_row_id_ = 0;
  V3MetaData::ManifestFileWrapper wrapper_;
};

}  // namespace iceberg
