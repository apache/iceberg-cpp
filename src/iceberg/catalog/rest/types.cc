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

#include "iceberg/catalog/rest/types.h"

#include <algorithm>
#include <format>

#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

// Configuration and Error types

Status CatalogConfig::Validate() const {
  // TODO(Li Feiyang): Add an invalidEndpoint test that validates endpoint format.
  // See:
  // https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/rest/responses/TestConfigResponseParser.java#L164
  // for reference.
  return {};
}

Status ErrorModel::Validate() const {
  if (message.empty() || type.empty()) {
    return Invalid("Invalid error model: missing required fields");
  }

  if (code < 400 || code > 600) {
    return Invalid("Invalid error model: code {} is out of range [400, 600]", code);
  }

  // stack is optional, no validation needed
  return {};
}

// We don't validate the error field because ErrorModel::Validate has been called in the
// FromJson.
Status ErrorResponse::Validate() const { return {}; }

// Namespace operations

Status ListNamespacesResponse::Validate() const { return {}; }

Status CreateNamespaceRequest::Validate() const { return {}; }

Status CreateNamespaceResponse::Validate() const { return {}; }

Status GetNamespaceResponse::Validate() const { return {}; }

Status UpdateNamespacePropertiesRequest::Validate() const {
  // keys in updates and removals must not overlap
  if (removals.empty() || updates.empty()) {
    return {};
  }

  auto extract_and_sort = [](const auto& container, auto key_extractor) {
    std::vector<std::string_view> result;
    result.reserve(container.size());
    for (const auto& item : container) {
      result.push_back(std::string_view{key_extractor(item)});
    }
    std::ranges::sort(result);
    return result;
  };

  auto sorted_removals =
      extract_and_sort(removals, [](const auto& s) -> const auto& { return s; });
  auto sorted_update_keys = extract_and_sort(
      updates, [](const auto& pair) -> const auto& { return pair.first; });

  std::vector<std::string_view> common;
  std::ranges::set_intersection(sorted_removals, sorted_update_keys,
                                std::back_inserter(common));

  if (!common.empty()) {
    return Invalid(
        "Invalid namespace update: cannot simultaneously set and remove keys: {}",
        common);
  }
  return {};
}

Status UpdateNamespacePropertiesResponse::Validate() const { return {}; }

// Table operations

Status ListTablesResponse::Validate() const { return {}; }

Status LoadTableResult::Validate() const {
  if (!metadata) {
    return Invalid("Invalid metadata: null");
  }
  return {};
}

Status RegisterTableRequest::Validate() const {
  if (name.empty()) {
    return Invalid("Missing table name");
  }

  if (metadata_location.empty()) {
    return Invalid("Empty metadata location");
  }

  return {};
}

Status RenameTableRequest::Validate() const {
  ICEBERG_RETURN_UNEXPECTED(source.Validate());
  ICEBERG_RETURN_UNEXPECTED(destination.Validate());
  return {};
}

}  // namespace iceberg::rest
