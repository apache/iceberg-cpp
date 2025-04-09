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

#include <nlohmann/json_fwd.hpp>

#include "iceberg/error.h"
#include "iceberg/expected.h"
#include "iceberg/type_fwd.h"

namespace iceberg {
/// \brief Serializes a `SortField` object to JSON.
///
/// This function converts a `SortField` object into a JSON representation.
/// The resulting JSON object includes the transform type, source ID, sort direction, and
/// null ordering.
///
/// \param sort_field The `SortField` object to be serialized.
/// \return A JSON object representing the `SortField` in the form of key-value pairs.
nlohmann::json ToJson(const SortField& sort_field);

/// \brief Serializes a `SortOrder` object to JSON.
///
/// This function converts a `SortOrder` object into a JSON representation.
/// The resulting JSON includes the order ID and a list of `SortField` objects.
/// Each `SortField` is serialized as described in the `ToJson(SortField)` function.
///
/// \param sort_order The `SortOrder` object to be serialized.
/// \return A JSON object representing the `SortOrder` with its order ID and fields array.
nlohmann::json ToJson(const SortOrder& sort_order);

/// \brief Deserializes a JSON object into a `SortField` object.
///
/// This function parses the provided JSON and creates a `SortField` object.
/// It expects the JSON object to contain keys for the transform, source ID, direction,
/// and null order.
///
/// \param json The JSON object representing a `SortField`.
/// \return An `expected` value containing either a `SortField` object or an error. If the
/// JSON is malformed or missing expected fields, an error will be returned.
expected<std::unique_ptr<SortField>, Error> SortFieldFromJson(const nlohmann::json& json);

/// \brief Deserializes a JSON object into a `SortOrder` object.
///
/// This function parses the provided JSON and creates a `SortOrder` object.
/// It expects the JSON object to contain the order ID and a list of `SortField` objects.
/// Each `SortField` will be parsed using the `SortFieldFromJson` function.
///
/// \param json The JSON object representing a `SortOrder`.
/// \return An `expected` value containing either a `SortOrder` object or an error. If the
/// JSON is malformed or missing expected fields, an error will be returned.
expected<std::unique_ptr<SortOrder>, Error> SortOrderFromJson(const nlohmann::json& json);

}  // namespace iceberg
