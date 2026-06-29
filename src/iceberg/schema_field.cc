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
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// A null default value is modeled as the absence of a default (matching Java), so it is
// not stored.
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
  // A null default is modeled as absence and dropped at construction, so it never reaches
  // here; only the out-of-range cast sentinels need rejecting.
  if (value.IsAboveMax() || value.IsBelowMin()) {
    return InvalidSchema("Invalid {} value for {}: value is out of range", kind,
                         field.name());
  }
  if (field.type() == nullptr) {
    return InvalidSchema("Invalid {} value for {}: field has no type", kind,
                         field.name());
  }
  // The spec requires unknown/variant/geometry/geography columns to default to null, so a
  // non-null default on them is invalid (a null default was already dropped as absence).
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
  // Defaults are otherwise only supported on primitive fields. The spec also permits JSON
  // single-value defaults for struct/list/map (e.g. an empty struct `{}` whose sub-field
  // defaults live in field metadata); that matches the current Java model's gap and is
  // left as a follow-up.
  if (!field.type()->is_primitive()) {
    return InvalidSchema(
        "Invalid {} value for {}: default values are only supported for primitive types",
        kind, field.name());
  }
  // Defaults are stored verbatim (no implicit cast), so a default whose literal type does
  // not match the field type is invalid.
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
