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

#include <mutex>
#include <shared_mutex>
#include <utility>

#include "iceberg/file_io_registry.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

ResolvingFileIO::ResolvingFileIO(std::unordered_map<std::string, std::string> properties)
    : properties_(std::move(properties)) {}

ResolvingFileIO::~ResolvingFileIO() = default;

Result<std::shared_ptr<FileIO>> ResolvingFileIO::FileIOForPath(
    std::string_view location) {
  const auto scheme = StringUtils::ToLower(LocationUtil::ParseScheme(location));
  ICEBERG_ASSIGN_OR_RAISE(const auto name, FileIORegistry::Resolve(scheme));

  {
    std::shared_lock lock(mutex_);
    if (const auto cached = io_by_name_.find(name); cached != io_by_name_.end()) {
      return cached->second;
    }
  }

  std::unique_lock lock(mutex_);
  auto it = io_by_name_.find(name);
  if (it == io_by_name_.end()) {
    ICEBERG_ASSIGN_OR_RAISE(auto io, FileIORegistry::Load(name, properties_));
    // Forward all credentials; each implementation applies the prefixes it
    // understands.
    if (!storage_credentials_.empty()) {
      if (auto* credentialed = io->AsSupportsStorageCredentials()) {
        ICEBERG_RETURN_UNEXPECTED(
            credentialed->SetStorageCredentials(storage_credentials_));
      }
    }
    it = io_by_name_.emplace(std::string(name), std::move(io)).first;
  }
  return it->second;
}

Result<std::unique_ptr<InputFile>> ResolvingFileIO::NewInputFile(
    std::string file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto io, FileIOForPath(file_location));
  return io->NewInputFile(std::move(file_location));
}

Result<std::unique_ptr<InputFile>> ResolvingFileIO::NewInputFile(
    std::string file_location, size_t length) {
  ICEBERG_ASSIGN_OR_RAISE(auto io, FileIOForPath(file_location));
  return io->NewInputFile(std::move(file_location), length);
}

Result<std::unique_ptr<OutputFile>> ResolvingFileIO::NewOutputFile(
    std::string file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto io, FileIOForPath(file_location));
  return io->NewOutputFile(std::move(file_location));
}

Status ResolvingFileIO::DeleteFile(const std::string& file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto io, FileIOForPath(file_location));
  return io->DeleteFile(file_location);
}

Status ResolvingFileIO::DeleteFiles(const std::vector<std::string>& file_locations) {
  std::unordered_map<std::shared_ptr<FileIO>, std::vector<std::string>> locations_by_io;
  for (const auto& file_location : file_locations) {
    ICEBERG_ASSIGN_OR_RAISE(auto io, FileIOForPath(file_location));
    locations_by_io[io].push_back(file_location);
  }
  for (auto& [io, locations] : locations_by_io) {
    ICEBERG_RETURN_UNEXPECTED(io->DeleteFiles(locations));
  }
  return {};
}

Status ResolvingFileIO::SetStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials) {
  // Rebuild delegates lazily with the new credentials. Updating live delegates
  // instead would leave the resolver inconsistent if one of them rejected them.
  std::unique_lock lock(mutex_);
  storage_credentials_ = storage_credentials;
  io_by_name_.clear();
  return {};
}

const std::vector<StorageCredential>& ResolvingFileIO::credentials() const {
  return storage_credentials_;
}

}  // namespace iceberg
