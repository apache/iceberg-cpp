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

/// \brief Serializes a `ListNamespaceResponse` object to JSON.
///
/// \param response The `ListNamespaceResponse` object to be serialized.
/// \return A JSON object representing the `ListNamespaceResponse`.
nlohmann::json ToJson(const ListNamespaceResponse& response);

/// \brief Deserializes a JSON object into a `ListNamespaceResponse` object.
///
/// \param json The JSON object representing a `ListNamespaceResponse`.
/// \return A `ListNamespaceResponse` object or an error if the conversion fails.
Result<ListNamespaceResponse> ListNamespaceResponseFromJson(const nlohmann::json& json);

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

/// \brief Serializes a `GetNamespacePropertiesResponse` object to JSON.
///
/// \param response The `GetNamespacePropertiesResponse` object to be serialized.
/// \return A JSON object representing the `GetNamespacePropertiesResponse`.
nlohmann::json ToJson(const GetNamespacePropertiesResponse& response);

/// \brief Deserializes a JSON object into a `GetNamespacePropertiesResponse` object.
///
/// \param json The JSON object representing a `GetNamespacePropertiesResponse`.
/// \return A `GetNamespacePropertiesResponse` object or an error if the conversion fails.
Result<GetNamespacePropertiesResponse> GetNamespacePropertiesResponseFromJson(
    const nlohmann::json& json);

/// \brief Serializes an `UpdateNamespacePropsRequest` object to JSON.
///
/// \param request The `UpdateNamespacePropsRequest` object to be serialized.
/// \return A JSON object representing the `UpdateNamespacePropsRequest`.
nlohmann::json ToJson(const UpdateNamespacePropsRequest& request);

/// \brief Deserializes a JSON object into an `UpdateNamespacePropsRequest` object.
///
/// \param json The JSON object representing an `UpdateNamespacePropsRequest`.
/// \return An `UpdateNamespacePropsRequest` object or an error if the conversion fails.
Result<UpdateNamespacePropsRequest> UpdateNamespacePropsRequestFromJson(
    const nlohmann::json& json);

/// \brief Serializes an `UpdateNamespacePropsResponse` object to JSON.
///
/// \param response The `UpdateNamespacePropsResponse` object to be serialized.
/// \return A JSON object representing the `UpdateNamespacePropsResponse`.
nlohmann::json ToJson(const UpdateNamespacePropsResponse& response);

/// \brief Deserializes a JSON object into an `UpdateNamespacePropsResponse` object.
///
/// \param json The JSON object representing an `UpdateNamespacePropsResponse`.
/// \return An `UpdateNamespacePropsResponse` object or an error if the conversion fails.
Result<UpdateNamespacePropsResponse> UpdateNamespacePropsResponseFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `ListTableResponse` object to JSON.
///
/// \param response The `ListTableResponse` object to be serialized.
/// \return A JSON object representing the `ListTableResponse`.
nlohmann::json ToJson(const ListTableResponse& response);

/// \brief Deserializes a JSON object into a `ListTableResponse` object.
///
/// \param json The JSON object representing a `ListTableResponse`.
/// \return A `ListTableResponse` object or an error if the conversion fails.
Result<ListTableResponse> ListTableResponseFromJson(const nlohmann::json& json);

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

/// \brief Serializes a `LoadTableResponse` object to JSON.
///
/// \param response The `LoadTableResponse` object to be serialized.
/// \return A JSON object representing the `LoadTableResponse`.
nlohmann::json ToJson(const LoadTableResponse& response);

/// \brief Deserializes a JSON object into a `LoadTableResponse` object.
///
/// \param json The JSON object representing a `LoadTableResponse`.
/// \return A `LoadTableResponse` object or an error if the conversion fails.
Result<LoadTableResponse> LoadTableResponseFromJson(const nlohmann::json& json);

}  // namespace iceberg::rest
