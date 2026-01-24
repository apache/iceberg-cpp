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

/// \file iceberg/expression/json_internal.h
/// JSON serialization and deserialization for expressions.

#include <memory>

#include <nlohmann/json_fwd.hpp>

#include "iceberg/expression/expression.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

class UnboundPredicate;
class NamedReference;
class UnboundTransform;
class Literal;

/// \brief Serializes an Expression to JSON.
///
/// This function converts an Expression to its JSON representation following
/// the Iceberg REST API specification. It supports:
/// - Boolean constants: serialized as JSON boolean literals
/// - Logical expressions: And, Or, Not
/// - Unbound predicates: comparison, unary, and set operations
///
/// \param expr The Expression to serialize
/// \return A JSON representation of the expression
ICEBERG_EXPORT nlohmann::json ToJson(const Expression& expr);

/// \brief Deserializes a JSON object into an Expression.
///
/// This function parses the provided JSON and creates an Expression object.
/// It expects the JSON to follow the Iceberg REST API specification:
/// - JSON boolean true/false for constant expressions
/// - Objects with "type" field for other expressions
///
/// \param json The JSON representation of an expression
/// \return A shared pointer to the Expression or an error if parsing fails
ICEBERG_EXPORT Result<std::shared_ptr<Expression>> ExpressionFromJson(
    const nlohmann::json& json);

/// \brief Serializes an unbound predicate to JSON.
///
/// \param predicate The UnboundPredicate to serialize
/// \return A JSON representation of the predicate
ICEBERG_EXPORT nlohmann::json ToJson(const UnboundPredicate& predicate);

/// \brief Deserializes a JSON object into an UnboundPredicate.
///
/// \param json The JSON representation of a predicate
/// \return A shared pointer to the UnboundPredicate or an error if parsing fails
ICEBERG_EXPORT Result<std::shared_ptr<UnboundPredicate>> UnboundPredicateFromJson(
    const nlohmann::json& json);

/// \brief Serializes a NamedReference to JSON.
///
/// \param ref The NamedReference to serialize
/// \return A JSON string representing the reference name
ICEBERG_EXPORT nlohmann::json ToJson(const NamedReference& ref);

/// \brief Serializes an UnboundTransform to JSON.
///
/// \param transform The UnboundTransform to serialize
/// \return A JSON object representing the transform term
ICEBERG_EXPORT nlohmann::json ToJson(const UnboundTransform& transform);

/// \brief Serializes a Literal to JSON.
///
/// \param literal The Literal to serialize
/// \return A JSON value representing the literal
ICEBERG_EXPORT nlohmann::json LiteralToJson(const Literal& literal);

/// \brief Deserializes a JSON value into a Literal.
///
/// \param json The JSON representation of a literal
/// \return A Literal or an error if parsing fails
ICEBERG_EXPORT Result<Literal> LiteralFromJson(const nlohmann::json& json);

/// \brief Converts an Expression::Operation to its JSON string representation.
///
/// \param op The operation to convert
/// \return The JSON type string (e.g., "eq", "lt-eq", "is-null")
ICEBERG_EXPORT std::string_view OperationToJsonType(Expression::Operation op);

/// \brief Converts a JSON type string to an Expression::Operation.
///
/// \param type_str The JSON type string
/// \return The corresponding Operation or an error if unknown
ICEBERG_EXPORT Result<Expression::Operation> OperationFromJsonType(std::string_view type_str);

}  // namespace iceberg
