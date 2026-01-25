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

#include "iceberg/expression/expression.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

/// \file iceberg/expression/json_internal.h
/// JSON serialization and deserialization for expressions.

namespace iceberg {

template <typename Model>
Result<Model> FromJson(const nlohmann::json& json);

#define ICEBERG_DECLARE_JSON_SERDE(Model)                                             \
  ICEBERG_EXPORT Result<std::shared_ptr<Model>> Model##FromJson(                      \
      const nlohmann::json& json);                                                    \
                                                                                      \
  template <typename Model>                                                           \
  ICEBERG_EXPORT Result<std::shared_ptr<Model>> FromJson(const nlohmann::json& json); \
                                                                                      \
  ICEBERG_EXPORT nlohmann::json ToJson(const Model& model);

/// \note Don't forget to add `ICEBERG_DEFINE_FROM_JSON` to the end of
/// `json_internal.cc` to define the `FromJson` function for the model.
ICEBERG_DECLARE_JSON_SERDE(Expression)

#undef ICEBERG_DECLARE_JSON_SERDE

/// \brief Converts an operation type string to an Expression::Operation.
///
/// \param type_str The operation type string
/// \return The corresponding Operation or an error if unknown
ICEBERG_EXPORT Result<Expression::Operation> OperationTypeFromString(
    const std::string_view type_str);

/// \brief Converts an Expression::Operation to its string representation.
///
/// \param op The operation to convert
/// \return The operation type string (e.g., "eq", "lt-eq", "is-null")
ICEBERG_EXPORT std::string_view ToStringOperationType(Expression::Operation op);

/// Check if an operation is a unary predicate (no values)
ICEBERG_EXPORT bool IsUnaryOperation(Expression::Operation op);

/// Check if an operation is a set predicate (multiple values)
ICEBERG_EXPORT bool IsSetOperation(Expression::Operation op);

}  // namespace iceberg
