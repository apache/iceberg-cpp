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

/// \brief Serializes a `CreateNamespaceRequest` object to JSON.
///
/// \param request The `CreateNamespaceRequest` object to be serialized.
/// \return A JSON object representing the `CreateNamespaceRequest`.
nlohmann::json ToJson(const CreateNamespaceRequest& request);

/// \brief Deserializes a JSON object into a `CreateNamespaceRequest` object.
///
/// \param json The JSON object representing a `CreateNamespaceRequest`.
/// \return A `CreateNamespaceRequest` object or an error if the conversion fails.
Result<CreateNamespaceRequest> CreateNamespaceRequestFromJson(const nlohmann::json& json);

/// \brief Serializes an `UpdateNamespacePropertiesRequest` object to JSON.
///
/// \param request The `UpdateNamespacePropertiesRequest` object to be serialized.
/// \return A JSON object representing the `UpdateNamespacePropertiesRequest`.
nlohmann::json ToJson(const UpdateNamespacePropertiesRequest& request);

/// \brief Deserializes a JSON object into an `UpdateNamespacePropertiesRequest` object.
///
/// \param json The JSON object representing an `UpdateNamespacePropertiesRequest`.
/// \return An `UpdateNamespacePropertiesRequest` object or an error if the conversion
/// fails.
Result<UpdateNamespacePropertiesRequest> UpdateNamespacePropertiesRequestFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `CreateTableRequest` object to JSON.
///
/// \param request The `CreateTableRequest` object to be serialized.
/// \return A JSON object representing the `CreateTableRequest`.
nlohmann::json ToJson(const CreateTableRequest& request);

/// \brief Deserializes a JSON object into a `CreateTableRequest` object.
///
/// \param json The JSON object representing a `CreateTableRequest`.
/// \return A `CreateTableRequest` object or an error if the conversion fails.
Result<CreateTableRequest> CreateTableRequestFromJson(const nlohmann::json& json);

/// \brief Serializes a `RegisterTableRequest` object to JSON.
///
/// \param request The `RegisterTableRequest` object to be serialized.
/// \return A JSON object representing the `RegisterTableRequest`.
nlohmann::json ToJson(const RegisterTableRequest& request);

/// \brief Deserializes a JSON object into a `RegisterTableRequest` object.
///
/// \param json The JSON object representing a `RegisterTableRequest`.
/// \return A `RegisterTableRequest` object or an error if the conversion fails.
Result<RegisterTableRequest> RegisterTableRequestFromJson(const nlohmann::json& json);

/// \brief Serializes a `RenameTableRequest` object to JSON.
///
/// \param request The `RenameTableRequest` object to be serialized.
/// \return A JSON object representing the `RenameTableRequest`.
nlohmann::json ToJson(const RenameTableRequest& request);

/// \brief Deserializes a JSON object into a `RenameTableRequest` object.
///
/// \param json The JSON object representing a `RenameTableRequest`.
/// \return A `RenameTableRequest` object or an error if the conversion fails.
Result<RenameTableRequest> RenameTableRequestFromJson(const nlohmann::json& json);

/// \brief Serializes a `LoadTableResult` object to JSON.
///
/// \param result The `LoadTableResult` object to be serialized.
/// \return A JSON object representing the `LoadTableResult`.
nlohmann::json ToJson(const LoadTableResult& result);

/// \brief Deserializes a JSON object into a `LoadTableResult` object.
///
/// \param json The JSON object representing a `LoadTableResult`.
/// \return A `LoadTableResult` object or an error if the conversion fails.
Result<LoadTableResult> LoadTableResultFromJson(const nlohmann::json& json);

/// \brief Serializes a `ListNamespacesResponse` object to JSON.
///
/// \param response The `ListNamespacesResponse` object to be serialized.
/// \return A JSON object representing the `ListNamespacesResponse`.
nlohmann::json ToJson(const ListNamespacesResponse& response);

/// \brief Deserializes a JSON object into a `ListNamespacesResponse` object.
///
/// \param json The JSON object representing a `ListNamespacesResponse`.
/// \return A `ListNamespacesResponse` object or an error if the conversion fails.
Result<ListNamespacesResponse> ListNamespacesResponseFromJson(const nlohmann::json& json);

/// \brief Serializes a `CreateNamespaceResponse` object to JSON.
///
/// \param response The `CreateNamespaceResponse` object to be serialized.
/// \return A JSON object representing the `CreateNamespaceResponse`.
nlohmann::json ToJson(const CreateNamespaceResponse& response);

/// \brief Deserializes a JSON object into a `CreateNamespaceResponse` object.
///
/// \param json The JSON object representing a `CreateNamespaceResponse`.
/// \return A `CreateNamespaceResponse` object or an error if the conversion fails.
Result<CreateNamespaceResponse> CreateNamespaceResponseFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `GetNamespaceResponse` object to JSON.
///
/// \param response The `GetNamespaceResponse` object to be serialized.
/// \return A JSON object representing the `GetNamespaceResponse`.
nlohmann::json ToJson(const GetNamespaceResponse& response);

/// \brief Deserializes a JSON object into a `GetNamespaceResponse` object.
///
/// \param json The JSON object representing a `GetNamespaceResponse`.
/// \return A `GetNamespaceResponse` object or an error if the conversion fails.
Result<GetNamespaceResponse> GetNamespaceResponseFromJson(const nlohmann::json& json);

/// \brief Serializes an `UpdateNamespacePropertiesResponse` object to JSON.
///
/// \param response The `UpdateNamespacePropertiesResponse` object to be serialized.
/// \return A JSON object representing the `UpdateNamespacePropertiesResponse`.
nlohmann::json ToJson(const UpdateNamespacePropertiesResponse& response);

/// \brief Deserializes a JSON object into an `UpdateNamespacePropertiesResponse` object.
///
/// \param json The JSON object representing an `UpdateNamespacePropertiesResponse`.
/// \return An `UpdateNamespacePropertiesResponse` object or an error if the conversion
/// fails.
Result<UpdateNamespacePropertiesResponse> UpdateNamespacePropertiesResponseFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `ListTablesResponse` object to JSON.
///
/// \param response The `ListTablesResponse` object to be serialized.
/// \return A JSON object representing the `ListTablesResponse`.
nlohmann::json ToJson(const ListTablesResponse& response);

/// \brief Deserializes a JSON object into a `ListTablesResponse` object.
///
/// \param json The JSON object representing a `ListTablesResponse`.
/// \return A `ListTablesResponse` object or an error if the conversion fails.
Result<ListTablesResponse> ListTablesResponseFromJson(const nlohmann::json& json);

}  // namespace iceberg::rest
