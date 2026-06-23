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

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/types.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

bool IsBuiltinImpl(std::string_view io_impl) {
  return io_impl == FileIORegistry::kArrowLocalFileIO ||
         io_impl == FileIORegistry::kArrowS3FileIO;
}

// Rewrites S3-compatible schemes (s3a/s3n/oss) to the canonical s3:// so a vended
// credential matches a table location using an equivalent scheme (e.g. DLF vends
// an `s3` credential for oss:// locations).
std::string CanonicalizeS3Scheme(std::string_view location) {
  for (std::string_view scheme : {"s3a://", "s3n://", "oss://"}) {
    if (location.starts_with(scheme)) {
      return std::string("s3://").append(location.substr(scheme.size()));
    }
  }
  return std::string(location);
}

}  // namespace

Result<BuiltinFileIOKind> DetectBuiltinFileIO(std::string_view location) {
  const auto pos = location.find("://");
  if (pos == std::string_view::npos) {
    return BuiltinFileIOKind::kArrowLocal;
  }

  const auto scheme = location.substr(0, pos);
  if (scheme == "file") {
    return BuiltinFileIOKind::kArrowLocal;
  }
  if (scheme == "s3" || scheme == "s3a" || scheme == "s3n") {
    return BuiltinFileIOKind::kArrowS3;
  }

  return NotSupported("URI scheme '{}' is not supported for automatic FileIO resolution",
                      scheme);
}

std::string_view BuiltinFileIOName(BuiltinFileIOKind kind) {
  switch (kind) {
    case BuiltinFileIOKind::kArrowLocal:
      return FileIORegistry::kArrowLocalFileIO;
    case BuiltinFileIOKind::kArrowS3:
      return FileIORegistry::kArrowS3FileIO;
  }
  std::unreachable();
}

Result<std::unique_ptr<FileIO>> MakeCatalogFileIO(const RestCatalogProperties& config) {
  std::string io_impl = config.Get(RestCatalogProperties::kIOImpl);
  std::string warehouse = config.Get(RestCatalogProperties::kWarehouse);

  if (io_impl.empty()) {
    if (warehouse.empty()) {
      return InvalidArgument(R"("{}" or "{}" property is required to create FileIO)",
                             RestCatalogProperties::kIOImpl.key(),
                             RestCatalogProperties::kWarehouse.key());
    }
    ICEBERG_ASSIGN_OR_RAISE(const auto detected_kind, DetectBuiltinFileIO(warehouse));
    io_impl = std::string(BuiltinFileIOName(detected_kind));
  }

  if (!warehouse.empty() && IsBuiltinImpl(io_impl)) {
    ICEBERG_ASSIGN_OR_RAISE(const auto detected_kind, DetectBuiltinFileIO(warehouse));
    const auto detected_name = BuiltinFileIOName(detected_kind);
    if (io_impl != detected_name) {
      return InvalidArgument(
          R"("io-impl" value '{}' is incompatible with warehouse '{}')", io_impl,
          warehouse);
    }
  }

  // TODO(gangwu): Support Java-style customized FileIO creation flows instead of
  // resolving a single catalog-scoped FileIO instance only from properties.
  return FileIORegistry::Load(io_impl, config.configs());
}

const StorageCredential* SelectS3StorageCredential(
    const std::vector<StorageCredential>& credentials,
    std::string_view storage_location) {
  const std::string location = CanonicalizeS3Scheme(storage_location);
  const StorageCredential* best = nullptr;
  size_t best_size = 0;
  for (const auto& cred : credentials) {
    if (!cred.prefix.starts_with("s3")) {
      continue;
    }
    // Keep the longest S3 prefix that matches this location; matching globally
    // would bind a credential to paths it does not cover.
    const std::string prefix = CanonicalizeS3Scheme(cred.prefix);
    if (location.starts_with(prefix) && (best == nullptr || prefix.size() > best_size)) {
      best = &cred;
      best_size = prefix.size();
    }
  }
  return best;
}

bool HasOnlyNonS3StorageCredentials(const std::vector<StorageCredential>& credentials) {
  return !credentials.empty() &&
         std::ranges::none_of(credentials, [](const StorageCredential& cred) {
           return cred.prefix.starts_with("s3");
         });
}

Result<std::unique_ptr<FileIO>> MakeS3FileIOFromCredential(
    const std::unordered_map<std::string, std::string>& catalog_config,
    const std::unordered_map<std::string, std::string>& table_config,
    const StorageCredential& s3_cred) {
  auto properties = catalog_config;
  for (const auto& [key, value] : table_config) {
    properties[key] = value;
  }
  for (const auto& [key, value] : s3_cred.config) {
    properties[key] = value;
  }
  return FileIORegistry::Load(std::string(FileIORegistry::kArrowS3FileIO), properties);
}

}  // namespace iceberg::rest
