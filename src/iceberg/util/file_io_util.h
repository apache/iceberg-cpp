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
#include <string_view>
#include <unordered_map>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

class ICEBERG_EXPORT FileIOUtil {
 public:
  /// \brief Detect FileIO registry key from URI scheme.
  ///
  /// Returns "s3" for "s3://..." URIs, "local" otherwise.
  static std::string DetectFileIOName(std::string_view uri);

  /// \brief Create a FileIO instance based on properties.
  ///
  /// Checks the "io-impl" property first; if not set, falls back to
  /// detecting the scheme from the "warehouse" property.
  static Result<std::unique_ptr<FileIO>> CreateFileIO(
      const std::unordered_map<std::string, std::string>& properties);
};

}  // namespace iceberg
