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

#include "iceberg/catalog/catalog_util.h"

#include "iceberg/table_identifier.h"

namespace iceberg {

std::string CatalogUtil::FullTableName(std::string_view catalog_name,
                                       const TableIdentifier& identifier) {
  if (catalog_name.empty()) {
    return identifier.ToString();
  }

  std::string result;
  if (catalog_name.contains('/') || catalog_name.contains(':')) {
    result = catalog_name;
    if (!catalog_name.ends_with('/')) {
      result += '/';
    }
  } else {
    result = std::string(catalog_name) + '.';
  }

  for (const auto& level : identifier.ns.levels) {
    result += level + '.';
  }
  result += identifier.name;
  return result;
}

}  // namespace iceberg
