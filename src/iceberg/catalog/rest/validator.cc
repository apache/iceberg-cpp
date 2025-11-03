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

#include "iceberg/catalog/rest/validator.h"

#include <format>
#include <ranges>
#include <unordered_set>

#include "iceberg/catalog/rest/types.h"
#include "iceberg/result.h"

namespace iceberg::rest {

// Namespace operations

Status Validator::Validate(const ListNamespacesResponse& response) { return {}; }

Status Validator::Validate(const CreateNamespaceRequest& request) { return {}; }

Status Validator::Validate(const CreateNamespaceResponse& response) { return {}; }

Status Validator::Validate(const GetNamespaceResponse& response) { return {}; }

Status Validator::Validate(const UpdateNamespacePropertiesRequest& request) {
  // keys in updates and removals must not overlap
  if (request.removals.empty() || request.updates.empty()) [[unlikely]] {
    return {};
  }

  std::unordered_set<std::string> remove_set(std::from_range, request.removals);

  auto common =
      request.updates | std::views::keys |
      std::views::filter([&](const std::string& k) { return remove_set.contains(k); }) |
      std::ranges::to<std::vector<std::string>>();

  if (!common.empty()) {
    std::string keys;
    bool first = true;
    std::ranges::for_each(common, [&](const std::string& s) {
      if (!std::exchange(first, false)) keys += ", ";
      keys += s;
    });

    return Invalid(
        "Invalid namespace properties update: cannot simultaneously set and remove keys: "
        "[{}]",
        keys);
  }
  return {};
}

Status Validator::Validate(const UpdateNamespacePropertiesResponse& response) {
  return {};
}

// Table operations

Status Validator::Validate(const ListTablesResponse& response) { return {}; }

Status Validator::Validate(const LoadTableResult& result) {
  if (!result.metadata) [[unlikely]] {
    return Invalid("Invalid metadata: null");
  }
  return {};
}

Status Validator::Validate(const RegisterTableRequest& request) {
  if (request.name.empty()) [[unlikely]] {
    return Invalid("Invalid table name: empty");
  }

  if (request.metadata_location.empty()) [[unlikely]] {
    return Invalid("Invalid metadata location: empty");
  }

  return {};
}

Status Validator::Validate(const RenameTableRequest& request) {
  if (request.source.ns.levels.empty() || request.source.name.empty()) [[unlikely]] {
    return Invalid("Invalid source identifier");
  }

  if (request.destination.ns.levels.empty() || request.destination.name.empty())
      [[unlikely]] {
    return Invalid("Invalid destination identifier");
  }

  return {};
}

}  // namespace iceberg::rest
