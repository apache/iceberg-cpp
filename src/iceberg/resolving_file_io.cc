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

#include "iceberg/resolving_file_io.h"

#include <utility>

#include "iceberg/file_io_registry.h"
#include "iceberg/util/macros.h"

namespace iceberg {

ResolvingFileIO::ResolvingFileIO(std::unordered_map<std::string, std::string> properties)
    : properties_(std::move(properties)) {}

ResolvingFileIO::~ResolvingFileIO() = default;

Result<std::string_view> ResolvingFileIO::ResolveFileIOName(std::string_view location) {
  const auto pos = location.find("://");
  if (pos == std::string_view::npos) {
    return FileIORegistry::kArrowLocalFileIO;
  }

  const auto scheme = location.substr(0, pos);
  if (scheme == "file") {
    return FileIORegistry::kArrowLocalFileIO;
  }
  // S3-compatible schemes served by the S3 FileIO (Java: SCHEME_TO_FILE_IO).
  if (scheme == "s3" || scheme == "s3a" || scheme == "s3n" || scheme == "oss") {
    return FileIORegistry::kArrowS3FileIO;
  }

  return NotSupported("URI scheme '{}' is not supported for FileIO resolution", scheme);
}

Result<FileIO*> ResolvingFileIO::FileIOForPath(std::string_view location) {
  ICEBERG_ASSIGN_OR_RAISE(const auto name, ResolveFileIOName(location));

  std::lock_guard lock(mutex_);
  auto it = io_by_name_.find(name);
  if (it == io_by_name_.end()) {
    ICEBERG_ASSIGN_OR_RAISE(auto io,
                            FileIORegistry::Load(std::string(name), properties_));
    // Forward all credentials; each implementation applies the prefixes it
    // understands.
    if (!storage_credentials_.empty()) {
      if (auto* credentialed = io->AsSupportsStorageCredentials()) {
        ICEBERG_RETURN_UNEXPECTED(
            credentialed->SetStorageCredentials(storage_credentials_));
      }
    }
    it = io_by_name_.emplace(name, std::move(io)).first;
  }
  return it->second.get();
}

Result<std::unique_ptr<InputFile>> ResolvingFileIO::NewInputFile(
    std::string file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto* io, FileIOForPath(file_location));
  return io->NewInputFile(std::move(file_location));
}

Result<std::unique_ptr<InputFile>> ResolvingFileIO::NewInputFile(
    std::string file_location, size_t length) {
  ICEBERG_ASSIGN_OR_RAISE(auto* io, FileIOForPath(file_location));
  return io->NewInputFile(std::move(file_location), length);
}

Result<std::unique_ptr<OutputFile>> ResolvingFileIO::NewOutputFile(
    std::string file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto* io, FileIOForPath(file_location));
  return io->NewOutputFile(std::move(file_location));
}

Status ResolvingFileIO::DeleteFile(const std::string& file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto* io, FileIOForPath(file_location));
  return io->DeleteFile(file_location);
}

Status ResolvingFileIO::DeleteFiles(const std::vector<std::string>& file_locations) {
  std::unordered_map<FileIO*, std::vector<std::string>> locations_by_io;
  for (const auto& file_location : file_locations) {
    ICEBERG_ASSIGN_OR_RAISE(auto* io, FileIOForPath(file_location));
    locations_by_io[io].push_back(file_location);
  }
  for (auto& [io, locations] : locations_by_io) {
    ICEBERG_RETURN_UNEXPECTED(io->DeleteFiles(locations));
  }
  return {};
}

Status ResolvingFileIO::SetStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials) {
  std::lock_guard lock(mutex_);
  storage_credentials_ = storage_credentials;
  for (auto& [name, io] : io_by_name_) {
    if (auto* credentialed = io->AsSupportsStorageCredentials()) {
      ICEBERG_RETURN_UNEXPECTED(
          credentialed->SetStorageCredentials(storage_credentials_));
    }
  }
  return {};
}

const std::vector<StorageCredential>& ResolvingFileIO::credentials() const {
  return storage_credentials_;
}

}  // namespace iceberg
