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

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Registry for FileIO implementations.
///
/// Provides a mechanism to register and load FileIO implementations by name.
/// This allows the REST catalog (and others) to resolve FileIO implementations
/// at runtime based on configuration properties like "io-impl".
class ICEBERG_EXPORT FileIORegistry {
 public:
  static constexpr std::string_view kArrowLocalFileIO = "arrow-fs-local";
  static constexpr std::string_view kArrowS3FileIO = "arrow-fs-s3";

  /// Factory function type for creating FileIO instances.
  using Factory = std::function<Result<std::unique_ptr<FileIO>>(
      const std::unordered_map<std::string, std::string>& properties)>;

  /// \brief Register a FileIO factory under the given name.
  ///
  /// \param name The implementation name (e.g., "local", "s3")
  /// \param factory The factory function that creates the FileIO instance.
  static void Register(const std::string& name, Factory factory);

  /// \brief Load a FileIO implementation by name.
  ///
  /// \param name The implementation name to look up.
  /// \param properties Configuration properties to pass to the factory.
  /// \return A unique_ptr to the FileIO instance, or an error if not found.
  static Result<std::unique_ptr<FileIO>> Load(
      const std::string& name,
      const std::unordered_map<std::string, std::string>& properties);
};

}  // namespace iceberg
