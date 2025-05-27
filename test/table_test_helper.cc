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

#include "table_test_helper.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/json_internal.h"
#include "iceberg/test/test_config.h"

namespace iceberg {

std::string TableTestHelper::GetResourcePath(const std::string& file_name) {
  return std::string(ICEBERG_TEST_RESOURCES) + "/" + file_name;
}

void TableTestHelper::ReadJsonFile(const std::string& file_name, std::string* content) {
  std::filesystem::path path{GetResourcePath(file_name)};
  ASSERT_TRUE(std::filesystem::exists(path)) << "File does not exist: " << path.string();

  std::ifstream file(path);
  std::stringstream buffer;
  buffer << file.rdbuf();
  *content = buffer.str();
}

void TableTestHelper::ReadTableMetadata(const std::string& file_name,
                                        std::unique_ptr<TableMetadata>* metadata) {
  std::string json_content;
  ReadJsonFile(file_name, &json_content);

  nlohmann::json json = nlohmann::json::parse(json_content);
  auto result = TableMetadataFromJson(json);
  ASSERT_TRUE(result.has_value()) << "Failed to parse table metadata from " << file_name
                                  << ": " << result.error().message;
  *metadata = std::move(result.value());
}

}  // namespace iceberg
