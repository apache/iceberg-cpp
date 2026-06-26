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

/// \brief True if `credentials` is non-empty but has no S3-family credential
/// (prefix starting with "s3") — only unsupported schemes (GCS/ADLS) were vended.
ICEBERG_REST_EXPORT bool HasOnlyNonS3StorageCredentials(
    const std::vector<StorageCredential>& credentials);

/// \brief Build an S3 FileIO that routes each object path to a per-prefix file
/// system, one per S3-family vended credential (config merged catalog < table <
/// credential). Non-S3 credentials are ignored.
ICEBERG_REST_EXPORT Result<std::unique_ptr<FileIO>> MakeS3FileIOFromCredentials(
    const std::unordered_map<std::string, std::string>& catalog_config,
    const std::unordered_map<std::string, std::string>& table_config,
    const std::vector<StorageCredential>& storage_credentials);

}  // namespace iceberg::rest
