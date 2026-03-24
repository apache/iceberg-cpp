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

/// \file iceberg/puffin/json_serde_internal.h
/// JSON serialization/deserialization for Puffin file metadata.

#include <string>
#include <string_view>

#include <nlohmann/json_fwd.hpp>

#include "iceberg/iceberg_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"

namespace iceberg::puffin {

/// \brief Serialize a BlobMetadata to JSON.
ICEBERG_EXPORT nlohmann::json ToJson(const BlobMetadata& blob_metadata);

/// \brief Deserialize a BlobMetadata from JSON.
ICEBERG_EXPORT Result<BlobMetadata> BlobMetadataFromJson(const nlohmann::json& json);

/// \brief Serialize a FileMetadata to JSON.
ICEBERG_EXPORT nlohmann::json ToJson(const FileMetadata& file_metadata);

/// \brief Deserialize a FileMetadata from JSON.
ICEBERG_EXPORT Result<FileMetadata> FileMetadataFromJson(const nlohmann::json& json);

/// \brief Serialize a FileMetadata to a JSON string.
ICEBERG_EXPORT std::string ToJsonString(const FileMetadata& file_metadata,
                                        bool pretty = false);

/// \brief Deserialize a FileMetadata from a JSON string.
ICEBERG_EXPORT Result<FileMetadata> FileMetadataFromJsonString(
    std::string_view json_string);

}  // namespace iceberg::puffin
