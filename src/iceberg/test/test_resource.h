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

#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include <nlohmann/json.hpp>

#include "iceberg/json_internal.h"
#include "iceberg/result.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/test_config.h"

namespace iceberg {

/// \brief Get the full path to a resource file in the test resources directory
static std::string GetResourcePath(const std::string& file_name) {
  return std::string(ICEBERG_TEST_RESOURCES) + "/" + file_name;
}

/// \brief Read table metadata from a JSON file and return the Result directly
static Result<std::unique_ptr<TableMetadata>> ReadTableMetadataFromResource(
    const std::string& file_name) {
  std::filesystem::path path{GetResourcePath(file_name)};
  if (!std::filesystem::exists(path)) {
    return InvalidArgument("File does not exist: {}", path.string());
  }

  std::ifstream file(path);
  std::stringstream buffer;
  buffer << file.rdbuf();

  return TableMetadataFromJson(nlohmann::json::parse(buffer.str()));
}

}  // namespace iceberg
