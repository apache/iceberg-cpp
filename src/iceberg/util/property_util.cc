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

#include "iceberg/util/property_util.h"

#include <charconv>

#include "iceberg/table_properties.h"

namespace iceberg {

Status PropertyUtil::ValidateCommitProperties(
    const std::unordered_map<std::string, std::string>& properties) {
  for (const auto& property : TableProperties::commit_properties()) {
    if (auto it = properties.find(property); it != properties.end()) {
      int32_t parsed;
      auto [ptr, ec] = std::from_chars(it->second.data(),
                                       it->second.data() + it->second.size(), parsed);
      if (ec == std::errc::invalid_argument) {
        return ValidationFailed("Table property {} must have integer value, but got {}",
                                property, it->second);
      } else if (ec == std::errc::result_out_of_range) {
        return ValidationFailed("Table property {} value out of range {}", property,
                                it->second);
      }
      if (parsed < 0) {
        return ValidationFailed(
            "Table property {} must have non negative integer value, but got {}",
            property, parsed);
      }
    }
  }
  return {};
}

}  // namespace iceberg
