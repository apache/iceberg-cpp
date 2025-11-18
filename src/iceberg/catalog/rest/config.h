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
#include <string_view>
#include <unordered_map>

#include <cpr/cprtypes.h>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/util/config.h"

/// \file iceberg/catalog/rest/config.h
/// \brief RestCatalogConfig implementation for Iceberg REST API.

namespace iceberg::rest {

/// \brief Configuration class for a REST Catalog.
class ICEBERG_REST_EXPORT RestCatalogConfig : public ConfigBase<RestCatalogConfig> {
 public:
  template <typename T>
  using Entry = const ConfigBase<RestCatalogConfig>::Entry<T>;

  /// \brief The URI of the REST catalog server.
  inline static std::string_view kUri{"uri"};

  /// \brief The name of the catalog.
  inline static std::string_view kName{"name"};

  /// \brief The warehouse path.
  inline static std::string_view kWarehouse{"warehouse"};

  /// \brief Create a default RestCatalogConfig instance.
  static std::unique_ptr<RestCatalogConfig> default_properties();

  /// \brief Create a RestCatalogConfig instance from a map of key-value pairs.
  static std::unique_ptr<RestCatalogConfig> FromMap(
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Generates extra HTTP headers to be added to every request from the
  /// configuration.
  ///
  /// This includes default headers like Content-Type, User-Agent, X-Client-Version and
  /// any custom headers prefixed with "header." in the properties.
  /// \return A map of header names to values.
  std::unordered_map<std::string, std::string> GetExtraHeaders() const;
};

}  // namespace iceberg::rest
