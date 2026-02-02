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
ICEBERG_EXPORT Result<nlohmann::json> ToJson(Expression::Operation op);

/// \brief Deserializes a JSON object into an Expression.
///
/// \param json A JSON object representing an expression
/// \return A shared pointer to the deserialized Expression or an error
ICEBERG_EXPORT Result<std::shared_ptr<Expression>> ExpressionFromJson(
    const nlohmann::json& json);

/// \brief Serializes an Expression into its JSON representation.
///
/// \param expr The expression to serialize
/// \return A JSON object representing the expression, or an error
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const Expression& expr);

/// \brief Deserializes a JSON object into a NamedReference.
///
/// \param json A JSON object representing a named reference
/// \return A shared pointer to the deserialized NamedReference or an error
ICEBERG_EXPORT Result<std::unique_ptr<NamedReference>> NamedReferenceFromJson(
    const nlohmann::json& json);

/// \brief Serializes a NamedReference into its JSON representation.
///
/// \param ref The named reference to serialize
/// \return A JSON object representing the named reference, or an error
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const NamedReference& ref);

/// \brief Serializes an UnboundTransform into its JSON representation.
///
/// \param transform The unbound transform to serialize
/// \return A JSON object representing the unbound transform, or an error
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const UnboundTransform& transform);

/// \brief Deserializes a JSON object into an UnboundTransform.
///
/// \param json A JSON object representing an unbound transform
/// \return A shared pointer to the deserialized UnboundTransform or an error
ICEBERG_EXPORT Result<std::unique_ptr<UnboundTransform>> UnboundTransformFromJson(
    const nlohmann::json& json);

/// \brief Serializes a Literal into its JSON representation.
///
/// \param literal The literal to serialize
/// \return A JSON value representing the literal, or an error
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const Literal& literal);

/// \brief Deserializes a JSON value into a Literal.
///
/// \param json A JSON value representing a literal. There is a discrepency with the java
/// implementation. In java, a schema is taken as parameter even though the value passed
/// is always null. It could come in handy if we decide to do binding a deserialization
/// time.
/// \return The deserialized Literal or an error.
ICEBERG_EXPORT Result<Literal> LiteralFromJson(const nlohmann::json& json);

/// \brief Serializes an UnboundPredicate into its JSON representation.
///
/// \param pred The unbound predicate to serialize
/// \return A JSON object representing the predicate, or an error
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const UnboundPredicate& pred);

/// \brief Serializes a BoundReference into its JSON representation (field name string).
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const BoundReference& ref);

/// \brief Serializes a BoundTransform into its JSON representation.
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const BoundTransform& transform);

/// \brief Serializes a BoundPredicate into its JSON representation.
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const BoundPredicate& pred);

/// \brief Deserializes a JSON object into an UnboundPredicate.
///
/// \param json A JSON object representing an unbound predicate
/// \return A pointer to the deserialized UnboundPredicate or an error
ICEBERG_EXPORT Result<std::unique_ptr<UnboundPredicate>> UnboundPredicateFromJson(
    const nlohmann::json& json);

/// \brief Serializes a Term into its JSON representation.
///
/// \param term The term to serialize (NamedReference or UnboundTransform)
/// \return A JSON value representing the term, or an error
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const Term& term);

/// \brief Deserializes a JSON object into a NamedReference.
///
/// \param json A JSON object representing a named reference
/// \return A shared pointer to the deserialized NamedReference or an error
ICEBERG_EXPORT Result<std::unique_ptr<NamedReference>> NamedReferenceFromJson(
    const nlohmann::json& json);

/// \brief Serializes a NamedReference into its JSON representation.
///
/// \param ref The named reference to serialize
/// \return A JSON object representing the named reference
ICEBERG_EXPORT nlohmann::json ToJson(const NamedReference& ref);

/// \brief Serializes an UnboundTransform into its JSON representation.
///
/// \param transform The unbound transform to serialize
/// \return A JSON object representing the unbound transform
ICEBERG_EXPORT nlohmann::json ToJson(const UnboundTransform& transform);

/// \brief Deserializes a JSON object into an UnboundTransform.
///
/// \param json A JSON object representing an unbound transform
/// \return A shared pointer to the deserialized UnboundTransform or an error
ICEBERG_EXPORT Result<std::unique_ptr<UnboundTransform>> UnboundTransformFromJson(
    const nlohmann::json& json);

/// \brief Serializes a Literal into its JSON representation.
///
/// \param literal The literal to serialize
/// \return A JSON value representing the literal
ICEBERG_EXPORT nlohmann::json ToJson(const Literal& literal);

/// \brief Deserializes a JSON value into a Literal.
///
/// \param json A JSON value representing a literal
/// \return The deserialized Literal or an error
ICEBERG_EXPORT Result<Literal> LiteralFromJson(const nlohmann::json& json);

/// \brief Serializes an UnboundPredicate into its JSON representation.
///
/// \param pred The unbound predicate to serialize
/// \return A JSON object representing the predicate
ICEBERG_EXPORT nlohmann::json ToJson(const UnboundPredicate& pred);

/// \brief Deserializes a JSON object into an UnboundPredicate.
///
/// \param json A JSON object representing an unbound predicate
/// \return A shared pointer to the deserialized UnboundPredicate or an error
ICEBERG_EXPORT Result<std::unique_ptr<UnboundPredicate>> UnboundPredicateFromJson(
    const nlohmann::json& json);

/// \brief Serializes a Term into its JSON representation.
///
/// \param term The term to serialize (NamedReference or UnboundTransform)
/// \return A JSON value representing the term
ICEBERG_EXPORT nlohmann::json TermToJson(const Term& term);

/// Check if an operation is a unary predicate
ICEBERG_EXPORT bool IsUnaryOperation(Expression::Operation op);

/// Check if an operation is a set predicate
ICEBERG_EXPORT bool IsSetOperation(Expression::Operation op);

}  // namespace iceberg
