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

#include "iceberg/catalog/rest/rest_file_io.h"

#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/types.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

std::unordered_map<std::string, std::string> MergeFileIOProperties(
    const std::unordered_map<std::string, std::string>& catalog_config,
    const std::unordered_map<std::string, std::string>& table_config) {
  auto properties = catalog_config;
  for (const auto& [key, value] : table_config) {
    properties[key] = value;
  }
  return properties;
}

}  // namespace

Result<std::unique_ptr<FileIO>> MakeCatalogFileIO(const RestCatalogProperties& config) {
  std::string io_impl = config.Get(RestCatalogProperties::kIOImpl);
  if (io_impl.empty()) {
    // Resolve the FileIO per file-path scheme instead of guessing from
    // `warehouse`, which is often a logical identifier rather than a storage
    // URI (Java defaults to ResolvingFileIO likewise).
    io_impl = std::string(FileIORegistry::kResolvingFileIO);
  }

  // TODO(gangwu): Support Java-style customized FileIO creation flows instead of
  // resolving a single catalog-scoped FileIO instance only from properties.
  return FileIORegistry::Load(io_impl, config.configs());
}

Result<std::unique_ptr<FileIO>> MakeTableFileIO(
    const std::unordered_map<std::string, std::string>& catalog_config,
    const std::unordered_map<std::string, std::string>& table_config,
    const std::vector<StorageCredential>& storage_credentials) {
  const auto default_properties = MergeFileIOProperties(catalog_config, table_config);
  ICEBERG_ASSIGN_OR_RAISE(
      auto io, MakeCatalogFileIO(RestCatalogProperties::FromMap(default_properties)));

  if (storage_credentials.empty()) {
    return io;
  } else if (auto* credentialed = io->AsSupportsStorageCredentials()) {
    ICEBERG_RETURN_UNEXPECTED(credentialed->SetStorageCredentials(storage_credentials));
  } else {
    return NotSupported("Configured FileIO does not support vended storage credentials");
  }
  return io;
}

}  // namespace iceberg::rest
