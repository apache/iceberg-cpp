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

#include "iceberg/schema_field.h"

#include <format>
#include <string_view>
#include <utility>
#include <variant>

#include "iceberg/expression/literal.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// Treat null defaults as absent.
std::shared_ptr<const Literal> DropNullDefault(std::shared_ptr<const Literal> value) {
  if (value != nullptr && value->IsNull()) {
    return nullptr;
  }
  return value;
}

}  // namespace

SchemaField::SchemaField(int32_t field_id, std::string_view name,
                         std::shared_ptr<Type> type, bool optional, std::string_view doc,
                         std::shared_ptr<const Literal> initial_default,
                         std::shared_ptr<const Literal> write_default)
    : field_id_(field_id),
      name_(name),
      type_(std::move(type)),
      optional_(optional),
      doc_(doc),
      initial_default_(DropNullDefault(std::move(initial_default))),
      write_default_(DropNullDefault(std::move(write_default))) {}

SchemaField SchemaField::MakeOptional(int32_t field_id, std::string_view name,
                                      std::shared_ptr<Type> type, std::string_view doc) {
  return {field_id, name, std::move(type), true, doc};
}

SchemaField SchemaField::MakeRequired(int32_t field_id, std::string_view name,
                                      std::shared_ptr<Type> type, std::string_view doc) {
  return {field_id, name, std::move(type), false, doc};
}

SchemaField SchemaField::WithName(std::string_view name) const {
  return {field_id_, name, type_, optional_, doc_, initial_default_, write_default_};
}

SchemaField SchemaField::WithType(std::shared_ptr<Type> type) const {
  return {field_id_,        name_,         std::move(type), optional_, doc_,
          initial_default_, write_default_};
}

SchemaField SchemaField::WithDoc(std::string_view doc) const {
  return {field_id_, name_, type_, optional_, doc, initial_default_, write_default_};
}

SchemaField SchemaField::WithInitialDefault(
    std::shared_ptr<const Literal> initial_default) const {
  return {field_id_,     name_, type_, optional_, doc_, std::move(initial_default),
          write_default_};
}

SchemaField SchemaField::WithWriteDefault(
    std::shared_ptr<const Literal> write_default) const {
  return {field_id_,
          name_,
          type_,
          optional_,
          doc_,
          initial_default_,
          std::move(write_default)};
}

Result<Literal> SchemaField::CastDefaultValue(
    const Literal& value, const std::shared_ptr<PrimitiveType>& target_type) {
  if (target_type->type_id() == TypeId::kDecimal &&
      std::holds_alternative<Decimal>(value.value())) {
    const auto& source_type = internal::checked_cast<const DecimalType&>(*value.type());
    const auto& decimal_type = internal::checked_cast<const DecimalType&>(*target_type);
    if (source_type.scale() == decimal_type.scale()) {
      const auto& decimal_value = std::get<Decimal>(value.value());
      if (!decimal_value.FitsInPrecision(decimal_type.precision())) {
        return InvalidArgument("Cannot cast default value to {}: {}", *target_type,
                               value);
      }
      return Literal::Decimal(decimal_value.value(), decimal_type.precision(),
                              decimal_type.scale());
    }
  }
  return value.CastTo(target_type);
}

int32_t SchemaField::field_id() const { return field_id_; }

std::string_view SchemaField::name() const { return name_; }

const std::shared_ptr<Type>& SchemaField::type() const { return type_; }

bool SchemaField::optional() const { return optional_; }

std::string_view SchemaField::doc() const { return doc_; }

const std::shared_ptr<const Literal>& SchemaField::initial_default() const {
  return initial_default_;
}

const std::shared_ptr<const Literal>& SchemaField::write_default() const {
  return write_default_;
}

namespace {

Status ValidateDefault(const SchemaField& field, const Literal& value,
                       std::string_view kind) {
  // Null defaults are dropped at construction.
  if (value.IsAboveMax() || value.IsBelowMin()) {
    return InvalidSchema("Invalid {} value for {}: value is out of range", kind,
                         field.name());
  }
  if (field.type() == nullptr) {
    return InvalidSchema("Invalid {} value for {}: field has no type", kind,
                         field.name());
  }
  // These types can only use null defaults.
  switch (field.type()->type_id()) {
    case TypeId::kUnknown:
    case TypeId::kVariant:
    case TypeId::kGeometry:
    case TypeId::kGeography:
      return InvalidSchema("Invalid {} value for {}: type {} cannot have a default value",
                           kind, field.name(), *field.type());
    default:
      break;
  }
  // Nested defaults are not supported yet.
  if (!field.type()->is_primitive()) {
    return InvalidSchema(
        "Invalid {} value for {}: default values are only supported for primitive types",
        kind, field.name());
  }
  // Stored defaults must already match the field type.
  if (*value.type() != *field.type()) {
    return InvalidSchema("{} of field {} has type {} but expected {}", kind, field.name(),
                         *value.type(), *field.type());
  }
  return {};
}

}  // namespace

Status SchemaField::Validate() const {
  if (name_.empty()) [[unlikely]] {
    return InvalidSchema("SchemaField cannot have empty name");
  }
  if (type_ == nullptr) [[unlikely]] {
    return InvalidSchema("SchemaField cannot have null type");
  }
  if (initial_default_ != nullptr) {
    ICEBERG_RETURN_UNEXPECTED(
        ValidateDefault(*this, *initial_default_, "initial-default"));
  }
  if (write_default_ != nullptr) {
    ICEBERG_RETURN_UNEXPECTED(ValidateDefault(*this, *write_default_, "write-default"));
  }
  return {};
}

std::string SchemaField::ToString() const {
  std::string result = std::format("{} ({}): {} ({}){}", name_, field_id_, *type_,
                                   optional_ ? "optional" : "required",
                                   !doc_.empty() ? std::format(" - {}", doc_) : "");
  return result;
}

namespace {

bool DefaultEquals(const std::shared_ptr<const Literal>& lhs,
                   const std::shared_ptr<const Literal>& rhs) {
  if (lhs == nullptr || rhs == nullptr) {
    return lhs == rhs;
  }
  return *lhs == *rhs;
}

}  // namespace

bool SchemaField::Equals(const SchemaField& other) const {
  return field_id_ == other.field_id_ && name_ == other.name_ && *type_ == *other.type_ &&
         optional_ == other.optional_ &&
         DefaultEquals(initial_default_, other.initial_default_) &&
         DefaultEquals(write_default_, other.write_default_);
}

}  // namespace iceberg
