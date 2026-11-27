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

/// \file iceberg/storage_namespace.h
/// \brief Identify changes to a FileIO storage namespace.

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/storage_credential.h"

namespace iceberg {

/// Optional capability for FileIOs whose credentials can change storage routing.
/// A credentialed FileIO without this capability bypasses whole-file caching.
class ICEBERG_EXPORT StorageNamespace {
 public:
  virtual ~StorageNamespace() = default;

  /// Changes when a location may resolve to a different store. Reading the generation
  /// must not contact storage. An unknown generation disables caching.
  virtual std::optional<uint64_t> namespace_generation() const = 0;

  /// A resolver can retain this function after retiring a delegate. It returns true
  /// when rebuilding the delegate with the new credentials preserves all routes.
  /// The delegate's FileIO::properties() must reproduce its effective construction
  /// settings, including routing defaults taken from the environment. Background
  /// credential refreshes must stay on those routes.
  using SameRoutes =
      bool (*)(const std::unordered_map<std::string, std::string>& properties,
               const std::vector<StorageCredential>& previous,
               const std::vector<StorageCredential>& next);
  virtual SameRoutes same_routes() const { return nullptr; }
};

}  // namespace iceberg
