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
#include "iceberg/type_fwd.h"

/// \file iceberg/expression/json_serde_internal.h
/// JSON serialization and deserialization for expressions.

namespace iceberg {

/// \brief Converts an operation type string to an Expression::Operation.
///
/// \param typeStr The operation type string
/// \return The corresponding Operation or an error if unknown
ICEBERG_EXPORT Result<Expression::Operation> OperationTypeFromJson(
    const nlohmann::json& json);

/// \brief Converts an Expression::Operation to its json representation.
///
/// \param op The operation to convert
/// \return The operation type string (e.g., "eq", "lt-eq", "is-null")
ICEBERG_EXPORT nlohmann::json ToJson(Expression::Operation op);

/// \brief Deserializes a JSON object into an Expression.
///
/// \param json A JSON object representing an expression
/// \return A shared pointer to the deserialized Expression or an error
ICEBERG_EXPORT Result<std::shared_ptr<Expression>> ExpressionFromJson(
    const nlohmann::json& json);

/// \brief Serializes an Expression into its JSON representation.
///
/// \param expr The expression to serialize
/// \return A JSON object representing the expression
ICEBERG_EXPORT nlohmann::json ToJson(const Expression& expr);

/// Check if an operation is a unary predicate
ICEBERG_EXPORT bool IsUnaryOperation(Expression::Operation op);

/// Check if an operation is a set predicate
ICEBERG_EXPORT bool IsSetOperation(Expression::Operation op);

}  // namespace iceberg
