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

#include "iceberg/metrics_config.h"

#include <string>
#include <unordered_map>

#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table_properties.h"

namespace iceberg {

Status MetricsConfig::VerifyReferencedColumns(
    const std::unordered_map<std::string, std::string>& updates, const Schema& schema) {
  for (const auto& [key, value] : updates) {
    if (!key.starts_with(TableProperties::kMetricModeColumnConfPrefix)) {
      continue;
    }
    auto field_name =
        std::string_view(key).substr(TableProperties::kMetricModeColumnConfPrefix.size());
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema.FindFieldByName(field_name));
    if (!field.has_value()) {
      return ValidationFailed(
          "Invalid metrics config, could not find column {} from table prop {} in "
          "schema {}",
          field_name, key, schema.ToString());
    }
  }
  return {};
}

}  // namespace iceberg
