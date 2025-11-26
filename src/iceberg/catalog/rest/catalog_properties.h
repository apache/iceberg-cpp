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
#include <unordered_map>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"
#include "iceberg/util/config.h"

/// \file iceberg/catalog/rest/catalog_properties.h
/// \brief RestCatalogProperties implementation for Iceberg REST API.

namespace iceberg::rest {

/// \brief Configuration class for a REST Catalog.
class ICEBERG_REST_EXPORT RestCatalogProperties
    : public ConfigBase<RestCatalogProperties> {
 public:
  template <typename T>
  using Entry = const ConfigBase<RestCatalogProperties>::Entry<T>;

  /// \brief The URI of the REST catalog server.
  inline static Entry<std::string> kUri{"uri", ""};
  /// \brief The name of the catalog.
  inline static Entry<std::string> kName{"name", ""};
  /// \brief The warehouse path.
  inline static Entry<std::string> kWarehouse{"warehouse", ""};
  /// \brief The optional prefix for REST API paths.
  inline static Entry<std::string> kPrefix{"prefix", ""};
  /// \brief The prefix for HTTP headers.
  inline static constexpr std::string_view kHeaderPrefix = "header.";

  /// \brief Create a default RestCatalogProperties instance.
  static std::unique_ptr<RestCatalogProperties> default_properties();

  /// \brief Create a RestCatalogProperties instance from a map of key-value pairs.
  static std::unique_ptr<RestCatalogProperties> FromMap(
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Returns HTTP headers to be added to every request.
  std::unordered_map<std::string, std::string> ExtractHeaders() const;

  /// \brief Get the URI of the REST catalog server.
  /// \return The URI if configured, or an error if not set or empty.
  Result<std::string_view> Uri() const;

 private:
  RestCatalogProperties() = default;
};

}  // namespace iceberg::rest
