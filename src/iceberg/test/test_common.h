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

#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Get the full path to a resource file in the test resources directory
std::string GetResourcePath(const std::string& file_name);

/// \brief Read a JSON file from the test resources directory
void ReadJsonFile(const std::string& file_name, std::string* content);

/// \brief Read table metadata from a JSON file in the test resources directory
void ReadTableMetadata(const std::string& file_name,
                       std::unique_ptr<TableMetadata>* metadata);

}  // namespace iceberg
