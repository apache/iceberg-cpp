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

#include <memory>
#include <string>
#include <unordered_map>

#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/result.h"

#if ICEBERG_S3_ENABLED
#  include <arrow/filesystem/s3fs.h>
#endif

namespace iceberg::arrow {

#if ICEBERG_S3_ENABLED
/// \brief Build Arrow ``S3Options`` from an Iceberg properties map.
///
/// Production code should use MakeS3FileIO(); this is exposed so the
/// property-to-option mapping (region resolution, endpoint scheme handling,
/// addressing style) can be unit tested without a live S3 endpoint.
ICEBERG_BUNDLE_EXPORT Result<::arrow::fs::S3Options> ConfigureS3Options(
    const std::unordered_map<std::string, std::string>& properties);

/// \brief Build an Arrow S3 file system from a properties map (initializes S3 if
/// needed). Exposed so the credential-aware FileIO can build one fs per prefix.
ICEBERG_BUNDLE_EXPORT Result<std::shared_ptr<::arrow::fs::FileSystem>>
BuildArrowS3FileSystem(const std::unordered_map<std::string, std::string>& properties);
#endif

}  // namespace iceberg::arrow
