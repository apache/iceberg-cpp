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

namespace iceberg {

/// \brief Write manifest entries to a manifest file.
class ManifestWriterImpl : public ManifestWriter {
 public:
  explicit ManifestWriterImpl(std::unique_ptr<Writer> writer,
                              std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)), writer_(std::move(writer)) {}

  Status WriteManifestEntries(const std::vector<ManifestEntry>& entries) const override;

 private:
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<Writer> writer_;
};

/// \brief Write manifest files to a manifest list file.
class ManifestListWriterImpl : public ManifestListWriter {
 public:
  explicit ManifestListWriterImpl(std::unique_ptr<Writer> writer,
                                  std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)), writer_(std::move(writer)) {}

  Status WriteManifestFiles(const std::vector<ManifestFile>& files) const override;

 private:
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<Writer> writer_;
};

}  // namespace iceberg
