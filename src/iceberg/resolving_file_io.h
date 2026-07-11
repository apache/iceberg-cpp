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

/// \file iceberg/resolving_file_io.h
/// \brief FileIO that resolves the concrete implementation per file-path scheme.

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/storage_credential.h"

namespace iceberg {

/// \brief FileIO that uses the location scheme to choose the concrete FileIO,
/// mirroring Java's ResolvingFileIO.
///
/// Resolution is per file path and independent of `warehouse` (often a logical
/// identifier rather than a storage URI). Implementations are loaded lazily
/// from FileIORegistry with this FileIO's properties and cached. Vended
/// credentials are forwarded in full to every resolved FileIO that supports
/// them; each applies the prefixes it understands and ignores the rest.
class ICEBERG_EXPORT ResolvingFileIO final : public FileIO,
                                             public SupportsStorageCredentials {
 public:
  explicit ResolvingFileIO(std::unordered_map<std::string, std::string> properties);
  ~ResolvingFileIO() override;

  /// \brief The FileIORegistry name of the implementation serving `location`.
  static Result<std::string_view> ResolveFileIOName(std::string_view location);

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override;

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override;

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override;

  Status DeleteFile(const std::string& file_location) override;

  Status DeleteFiles(const std::vector<std::string>& file_locations) override;

  Status SetStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials) override;

  const std::vector<StorageCredential>& credentials() const override;

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }

 private:
  /// \brief Load (or return the cached) implementation serving `location`.
  Result<FileIO*> FileIOForPath(std::string_view location);

  std::unordered_map<std::string, std::string> properties_;
  // Guards lazy resolution; set credentials before sharing across threads.
  std::mutex mutex_;
  std::vector<StorageCredential> storage_credentials_;
  std::unordered_map<std::string_view, std::unique_ptr<FileIO>> io_by_name_;
};

}  // namespace iceberg
