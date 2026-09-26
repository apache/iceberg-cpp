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

  // Loads without holding `mutex_`: building a client can block (an S3 client
  // without static keys may wait on the EC2 metadata service), which would
  // stall every other operation. Forwards all credentials; each implementation
  // applies the prefixes it understands.
  auto load = [&](const std::vector<StorageCredential>& credentials)
      -> Result<std::shared_ptr<FileIO>> {
    ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<FileIO> io,
                            FileIORegistry::Load(name, properties_));
    if (!credentials.empty()) {
      if (auto* credentialed = io->AsSupportsStorageCredentials()) {
        ICEBERG_RETURN_UNEXPECTED(credentialed->SetStorageCredentials(credentials));
      }
    }
    return io;
  };

  while (true) {
    uint64_t generation = 0;
    std::vector<StorageCredential> credentials;
    {
      std::shared_lock lock(mutex_);
      if (const auto cached = io_by_name_.find(name); cached != io_by_name_.end()) {
        return cached->second;
      }
      generation = credential_generation_;
      credentials = storage_credentials_;
    }
    // Declared before the lock, so a delegate that is not cached is torn down
    // only after the lock is released.
    auto loaded = load(credentials);
    std::unique_lock lock(mutex_);
    if (generation != credential_generation_) {
      continue;  // Credentials were replaced mid-load; load again with them.
    }
    ICEBERG_RETURN_UNEXPECTED(loaded);
    // A concurrent first access may have cached one already; that one wins.
    return io_by_name_.try_emplace(name, *loaded).first->second;
  }
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
  // Retired outside the lock: tearing down a delegate can block.
  decltype(io_by_name_) retired;
  {
    std::unique_lock lock(mutex_);
    storage_credentials_ = storage_credentials;
    ++credential_generation_;
    retired.swap(io_by_name_);
  }
  return {};
}

std::vector<StorageCredential> ResolvingFileIO::credentials() const {
  std::shared_lock lock(mutex_);
  return storage_credentials_;
}

}  // namespace iceberg
