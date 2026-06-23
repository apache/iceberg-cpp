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

#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/result.h"

namespace iceberg::rest {

enum class BuiltinFileIOKind : uint8_t {
  kArrowLocal,
  kArrowS3,
};

ICEBERG_REST_EXPORT Result<BuiltinFileIOKind> DetectBuiltinFileIO(
    std::string_view location);

ICEBERG_REST_EXPORT std::string_view BuiltinFileIOName(BuiltinFileIOKind kind);

ICEBERG_REST_EXPORT Result<std::unique_ptr<FileIO>> MakeCatalogFileIO(
    const RestCatalogProperties& config);

/// \brief Returns the S3-family vended credential (prefix starting with "s3")
/// whose prefix is the longest match for `storage_location`, or nullptr if none
/// applies (Iceberg REST spec / Java `S3FileIO` longest-prefix matching). The
/// s3/s3a/s3n schemes are treated as equivalent when matching.
ICEBERG_REST_EXPORT const StorageCredential* SelectS3StorageCredential(
    const std::vector<StorageCredential>& credentials, std::string_view storage_location);

/// \brief True if `credentials` is non-empty but contains no S3-family
/// credential (prefix starting with "s3") — i.e. only unsupported schemes such
/// as GCS/ADLS were vended, which an S3/local FileIO cannot honor.
ICEBERG_REST_EXPORT bool HasOnlyNonS3StorageCredentials(
    const std::vector<StorageCredential>& credentials);

/// \brief Builds an `arrow-fs-s3` FileIO, merging catalog, table, then
/// credential config (later wins).
ICEBERG_REST_EXPORT Result<std::unique_ptr<FileIO>> MakeS3FileIOFromCredential(
    const std::unordered_map<std::string, std::string>& catalog_config,
    const std::unordered_map<std::string, std::string>& table_config,
    const StorageCredential& s3_cred);

}  // namespace iceberg::rest
