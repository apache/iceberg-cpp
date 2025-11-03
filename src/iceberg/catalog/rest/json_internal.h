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

#include "iceberg/catalog/rest/types.h"
#include "iceberg/result.h"

namespace iceberg::rest {

/// \brief Serializes a `ListNamespacesResponse` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(const ListNamespacesResponse& response);

/// \brief Deserializes a JSON object into a `ListNamespacesResponse` object.
ICEBERG_REST_EXPORT Result<ListNamespacesResponse> ListNamespacesResponseFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `CreateNamespaceRequest` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(const CreateNamespaceRequest& request);

/// \brief Deserializes a JSON object into a `CreateNamespaceRequest` object.
ICEBERG_REST_EXPORT Result<CreateNamespaceRequest> CreateNamespaceRequestFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `CreateNamespaceResponse` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(const CreateNamespaceResponse& response);

/// \brief Deserializes a JSON object into a `CreateNamespaceResponse` object.
ICEBERG_REST_EXPORT Result<CreateNamespaceResponse> CreateNamespaceResponseFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `GetNamespaceResponse` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(const GetNamespaceResponse& response);

/// \brief Deserializes a JSON object into a `GetNamespaceResponse` object.
ICEBERG_REST_EXPORT Result<GetNamespaceResponse> GetNamespaceResponseFromJson(
    const nlohmann::json& json);

/// \brief Serializes an `UpdateNamespacePropertiesRequest` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(
    const UpdateNamespacePropertiesRequest& request);

/// \brief Deserializes a JSON object into an `UpdateNamespacePropertiesRequest` object.
ICEBERG_REST_EXPORT Result<UpdateNamespacePropertiesRequest>
UpdateNamespacePropertiesRequestFromJson(const nlohmann::json& json);

/// \brief Serializes an `UpdateNamespacePropertiesResponse` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(
    const UpdateNamespacePropertiesResponse& response);

/// \brief Deserializes a JSON object into an `UpdateNamespacePropertiesResponse` object.
ICEBERG_REST_EXPORT Result<UpdateNamespacePropertiesResponse>
UpdateNamespacePropertiesResponseFromJson(const nlohmann::json& json);

/// \brief Serializes a `ListTablesResponse` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(const ListTablesResponse& response);

/// \brief Deserializes a JSON object into a `ListTablesResponse` object.
ICEBERG_REST_EXPORT Result<ListTablesResponse> ListTablesResponseFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `LoadTableResult` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(const LoadTableResult& result);

/// \brief Deserializes a JSON object into a `LoadTableResult` object.
ICEBERG_REST_EXPORT Result<LoadTableResult> LoadTableResultFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `RegisterTableRequest` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(const RegisterTableRequest& request);

/// \brief Deserializes a JSON object into a `RegisterTableRequest` object.
ICEBERG_REST_EXPORT Result<RegisterTableRequest> RegisterTableRequestFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `RenameTableRequest` object to JSON.
ICEBERG_REST_EXPORT nlohmann::json ToJson(const RenameTableRequest& request);

/// \brief Deserializes a JSON object into a `RenameTableRequest` object.
ICEBERG_REST_EXPORT Result<RenameTableRequest> RenameTableRequestFromJson(
    const nlohmann::json& json);

}  // namespace iceberg::rest
