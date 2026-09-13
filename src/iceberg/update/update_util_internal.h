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

#include "iceberg/expression/expression.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema_field.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::internal {

// Most primitive types have no mutable state. Parameterized types must be copied
// even when they are reached through a const Literal or shared_ptr alias.
// Returns nullptr for stateless primitives, which are safe to share.
inline std::shared_ptr<PrimitiveType> CopyParameterizedPrimitive(const Type& type) {
  switch (type.type_id()) {
    case TypeId::kDecimal:
      return std::make_shared<DecimalType>(checked_cast<const DecimalType&>(type));
    case TypeId::kFixed:
      return std::make_shared<FixedType>(checked_cast<const FixedType&>(type));
    case TypeId::kGeometry:
      return std::make_shared<GeometryType>(checked_cast<const GeometryType&>(type));
    case TypeId::kGeography:
      return std::make_shared<GeographyType>(checked_cast<const GeographyType&>(type));
    default:
      return nullptr;
  }
}

inline Result<Literal> CopyUpdateLiteral(const Literal& literal) {
  auto type = CopyParameterizedPrimitive(*literal.type());
  if (!type) {
    return literal;
  }
  if (literal.IsNull()) {
    return Literal::Null(std::move(type));
  }
  ICEBERG_ASSIGN_OR_RAISE(auto bytes, literal.Serialize());
  return Literal::Deserialize(bytes, std::move(type));
}

inline Result<std::shared_ptr<Type>> CopyUpdateType(const std::shared_ptr<Type>& type);

// Deep-copies a field, including nested types and default literals, so a caller
// keeping an alias to any of those objects cannot change frozen replay intent.
inline Result<SchemaField> CopyUpdateField(const SchemaField& field) {
  ICEBERG_ASSIGN_OR_RAISE(auto type, CopyUpdateType(field.type()));
  std::shared_ptr<const Literal> initial_default;
  if (field.initial_default()) {
    ICEBERG_ASSIGN_OR_RAISE(auto copy, CopyUpdateLiteral(*field.initial_default()));
    initial_default = std::make_shared<const Literal>(std::move(copy));
  }
  std::shared_ptr<const Literal> write_default;
  if (field.write_default()) {
    ICEBERG_ASSIGN_OR_RAISE(auto copy, CopyUpdateLiteral(*field.write_default()));
    write_default = std::make_shared<const Literal>(std::move(copy));
  }
  return SchemaField(field.field_id(), field.name(), std::move(type), field.optional(),
                     field.doc(), std::move(initial_default), std::move(write_default));
}

inline Result<std::shared_ptr<Type>> CopyUpdateType(const std::shared_ptr<Type>& type) {
  if (!type) {
    return nullptr;
  }
  switch (type->type_id()) {
    case TypeId::kStruct: {
      const auto source_fields = checked_cast<const StructType&>(*type).fields();
      std::vector<SchemaField> fields;
      fields.reserve(source_fields.size());
      for (const auto& field : source_fields) {
        ICEBERG_ASSIGN_OR_RAISE(auto copy, CopyUpdateField(field));
        fields.push_back(std::move(copy));
      }
      return std::make_shared<StructType>(std::move(fields));
    }
    case TypeId::kList: {
      ICEBERG_ASSIGN_OR_RAISE(
          auto element, CopyUpdateField(checked_cast<const ListType&>(*type).element()));
      return std::make_shared<ListType>(std::move(element));
    }
    case TypeId::kMap: {
      const auto& map = checked_cast<const MapType&>(*type);
      ICEBERG_ASSIGN_OR_RAISE(auto key, CopyUpdateField(map.key()));
      ICEBERG_ASSIGN_OR_RAISE(auto value, CopyUpdateField(map.value()));
      return std::make_shared<MapType>(std::move(key), std::move(value));
    }
    default:
      if (auto copy = CopyParameterizedPrimitive(*type)) {
        return copy;
      }
      return type;
  }
}

inline Result<std::shared_ptr<DataFile>> CopyUpdateDataFile(const DataFile& file) {
  auto copy = std::make_shared<DataFile>(file);
  std::vector<Literal> values;
  values.reserve(file.partition.num_fields());
  for (const auto& value : file.partition.values()) {
    ICEBERG_ASSIGN_OR_RAISE(auto frozen, CopyUpdateLiteral(value));
    values.push_back(std::move(frozen));
  }
  copy->partition.Reset(std::move(values));
  return copy;
}

inline Result<std::shared_ptr<Expression>> CopyUpdateExpression(
    const std::shared_ptr<Expression>& expression) {
  if (!expression) {
    return nullptr;
  }
  using Op = Expression::Operation;
  switch (expression->op()) {
    case Op::kTrue:
      return True::Instance();
    case Op::kFalse:
      return False::Instance();
    case Op::kAnd: {
      const auto& node = checked_cast<const And&>(*expression);
      ICEBERG_ASSIGN_OR_RAISE(auto left, CopyUpdateExpression(node.left()));
      ICEBERG_ASSIGN_OR_RAISE(auto right, CopyUpdateExpression(node.right()));
      return And::Make(std::move(left), std::move(right));
    }
    case Op::kOr: {
      const auto& node = checked_cast<const Or&>(*expression);
      ICEBERG_ASSIGN_OR_RAISE(auto left, CopyUpdateExpression(node.left()));
      ICEBERG_ASSIGN_OR_RAISE(auto right, CopyUpdateExpression(node.right()));
      return Or::Make(std::move(left), std::move(right));
    }
    case Op::kNot: {
      const auto& node = checked_cast<const Not&>(*expression);
      ICEBERG_ASSIGN_OR_RAISE(auto child, CopyUpdateExpression(node.child()));
      return Not::Make(std::move(child));
    }
    default:
      break;
  }
  if (!expression->is_unbound_predicate()) {
    // Preserve the normal validation path for already-bound/invalid filters.
    return expression;
  }
  // Expression is a virtual base of UnboundPredicate, so this cast must stay
  // dynamic in release builds too.
  const auto& predicate = dynamic_cast<const UnboundPredicate&>(*expression);
  std::vector<Literal> values;
  for (const auto& value : predicate.literals()) {
    ICEBERG_ASSIGN_OR_RAISE(auto copy, CopyUpdateLiteral(value));
    values.push_back(std::move(copy));
  }
  const auto& term = predicate.unbound_term();
  if (term.kind() == Term::Kind::kReference) {
    const auto& reference = checked_cast<const NamedReference&>(term);
    ICEBERG_ASSIGN_OR_RAISE(auto copy,
                            NamedReference::Make(std::string(reference.name())));
    return UnboundPredicateImpl<BoundReference>::Make(expression->op(), std::move(copy),
                                                      std::move(values));
  }
  const auto& transform = checked_cast<const UnboundTransform&>(term);
  ICEBERG_ASSIGN_OR_RAISE(
      auto reference, NamedReference::Make(std::string(transform.reference()->name())));
  ICEBERG_ASSIGN_OR_RAISE(
      auto copy,
      UnboundTransform::Make(std::move(reference),
                             std::make_shared<Transform>(*transform.transform())));
  return UnboundPredicateImpl<BoundTransform>::Make(expression->op(), std::move(copy),
                                                    std::move(values));
}

}  // namespace iceberg::internal
