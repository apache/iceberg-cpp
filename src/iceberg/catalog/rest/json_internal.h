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

#include <nlohmann/json_fwd.hpp>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/json_internal.h
/// JSON serialization and deserialization for Iceberg REST Catalog API types.

namespace iceberg::rest {

template <typename Model>
Result<Model> FromJson(const nlohmann::json& json);

#define ICEBERG_DECLARE_JSON_SERDE(Model)                                        \
  ICEBERG_REST_EXPORT Result<Model> Model##FromJson(const nlohmann::json& json); \
                                                                                 \
  template <>                                                                    \
  ICEBERG_REST_EXPORT Result<Model> FromJson(const nlohmann::json& json);        \
                                                                                 \
  ICEBERG_REST_EXPORT nlohmann::json ToJson(const Model& model);

/// \note Don't forget to add `ICEBERG_DEFINE_FROM_JSON` to the end of
/// `json_internal.cc` to define the `FromJson` function for the model.
ICEBERG_DECLARE_JSON_SERDE(CatalogConfig)
ICEBERG_DECLARE_JSON_SERDE(ErrorModel)
ICEBERG_DECLARE_JSON_SERDE(ErrorResponse)
ICEBERG_DECLARE_JSON_SERDE(ListNamespacesResponse)
ICEBERG_DECLARE_JSON_SERDE(CreateNamespaceRequest)
ICEBERG_DECLARE_JSON_SERDE(CreateNamespaceResponse)
ICEBERG_DECLARE_JSON_SERDE(GetNamespaceResponse)
ICEBERG_DECLARE_JSON_SERDE(UpdateNamespacePropertiesRequest)
ICEBERG_DECLARE_JSON_SERDE(UpdateNamespacePropertiesResponse)
ICEBERG_DECLARE_JSON_SERDE(ListTablesResponse)
ICEBERG_DECLARE_JSON_SERDE(LoadTableResult)
ICEBERG_DECLARE_JSON_SERDE(RegisterTableRequest)
ICEBERG_DECLARE_JSON_SERDE(RenameTableRequest)

#undef ICEBERG_DECLARE_JSON_SERDE

}  // namespace iceberg::rest
