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

#include "iceberg/expression/literal.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// Normalizes a default value to the field type. A cross-type default (e.g. an int
// literal on a long field) is accepted, so it is cast to the field type up front;
// otherwise projection, JSON round-trip and equality would observe a literal whose type
// differs from the field. A value that already matches the field type is returned as-is
// (no needless copy), and a value that cannot be cast is left unchanged so Validate()
// can report it.
std::shared_ptr<const Literal> NormalizeDefault(std::shared_ptr<const Literal> value,
                                                const std::shared_ptr<Type>& field_type) {
  if (value == nullptr || field_type == nullptr || !field_type->is_primitive()) {
    return value;
  }
  if (*value->type() == *field_type) {
    return value;
  }
  auto cast = value->CastTo(internal::checked_pointer_cast<PrimitiveType>(field_type));
  if (!cast.has_value() || cast->IsAboveMax() || cast->IsBelowMin()) {
    return value;
  }
  return std::make_shared<const Literal>(std::move(cast).value());
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
      initial_default_(NormalizeDefault(std::move(initial_default), type_)),
      write_default_(NormalizeDefault(std::move(write_default), type_)) {}

SchemaField SchemaField::MakeOptional(int32_t field_id, std::string_view name,
                                      std::shared_ptr<Type> type, std::string_view doc) {
  return {field_id, name, std::move(type), true, doc};
}

SchemaField SchemaField::MakeRequired(int32_t field_id, std::string_view name,
                                      std::shared_ptr<Type> type, std::string_view doc) {
  return {field_id, name, std::move(type), false, doc};
}

int32_t SchemaField::field_id() const { return field_id_; }

std::string_view SchemaField::name() const { return name_; }

const std::shared_ptr<Type>& SchemaField::type() const { return type_; }

bool SchemaField::optional() const { return optional_; }

std::string_view SchemaField::doc() const { return doc_; }

std::optional<std::reference_wrapper<const Literal>> SchemaField::initial_default()
    const {
  if (initial_default_ == nullptr) {
    return std::nullopt;
  }
  return std::cref(*initial_default_);
}

std::optional<std::reference_wrapper<const Literal>> SchemaField::write_default() const {
  if (write_default_ == nullptr) {
    return std::nullopt;
  }
  return std::cref(*write_default_);
}

const std::shared_ptr<const Literal>& SchemaField::initial_default_ptr() const {
  return initial_default_;
}

const std::shared_ptr<const Literal>& SchemaField::write_default_ptr() const {
  return write_default_;
}

namespace {

Status ValidateDefault(const SchemaField& field, const Literal& value,
                       std::string_view kind) {
  if (value.IsNull() || value.IsAboveMax() || value.IsBelowMin()) {
    return InvalidSchema("Invalid {} value for {}: must be a non-null value", kind,
                         field.name());
  }
  // Defaults are only supported on primitive fields. The spec also permits JSON
  // single-value defaults for struct/list/map (e.g. an empty struct `{}` whose
  // sub-field defaults live in field metadata); that matches the current Java model's
  // gap and is left as a follow-up.
  if (field.type() == nullptr || !field.type()->is_primitive()) {
    return InvalidSchema(
        "Invalid {} value for {}: default values are only supported for primitive types",
        kind, field.name());
  }
  // Match Java (Types.NestedField), which casts the default literal to the field type
  // instead of requiring an exact type match (e.g. an int default on a long field, or
  // a string default on a date/timestamp/uuid field). Reject only defaults that cannot
  // be cast to the field type or fall outside its range (CastTo signals out-of-range as
  // an above-max/below-min sentinel).
  auto field_type = internal::checked_pointer_cast<PrimitiveType>(field.type());
  ICEBERG_ASSIGN_OR_RAISE(auto cast, value.CastTo(field_type));
  if (cast.IsAboveMax() || cast.IsBelowMin()) {
    return InvalidSchema("{} of field {} ({}) is out of range for {}", kind, field.name(),
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
