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
#include <unordered_map>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table_scan.h"

/// \file iceberg/catalog/rest/json_serde_internal.h
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
ICEBERG_DECLARE_JSON_SERDE(CreateTableRequest)
ICEBERG_DECLARE_JSON_SERDE(CommitTableRequest)
ICEBERG_DECLARE_JSON_SERDE(CommitTableResponse)
ICEBERG_DECLARE_JSON_SERDE(OAuthTokenResponse)

#undef ICEBERG_DECLARE_JSON_SERDE

ICEBERG_REST_EXPORT Result<PlanTableScanResponse> PlanTableScanResponseFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema);

ICEBERG_REST_EXPORT Result<FetchPlanningResultResponse>
FetchPlanningResultResponseFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema);

ICEBERG_REST_EXPORT Result<FetchScanTasksResponse> FetchScanTasksResponseFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema);

ICEBERG_REST_EXPORT Result<nlohmann::json> ToJson(const PlanTableScanRequest& request);
ICEBERG_REST_EXPORT nlohmann::json ToJson(const FetchScanTasksRequest& request);

ICEBERG_REST_EXPORT nlohmann::json ToJson(const DataFile& df);

ICEBERG_REST_EXPORT Result<DataFile> DataFileFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& partition_spec_by_id,
    const Schema& schema);

ICEBERG_REST_EXPORT Result<std::vector<std::shared_ptr<FileScanTask>>> FileScanTasksFromJson(
    const nlohmann::json& json,
    const std::vector<std::shared_ptr<DataFile>>& delete_files,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& partition_spec_by_id,
    const Schema& schema);

ICEBERG_REST_EXPORT nlohmann::json ToJson(const PlanTableScanResponse& response);
ICEBERG_REST_EXPORT nlohmann::json ToJson(const FetchPlanningResultResponse& response);
ICEBERG_REST_EXPORT nlohmann::json ToJson(const FetchScanTasksResponse& response);

}  // namespace iceberg::rest
