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

#include "iceberg/type.h"

#include <format>
#include <iterator>
#include <memory>
#include <utility>

#include "iceberg/exception.h"
#include "iceberg/schema.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

Result<std::optional<NestedType::SchemaFieldConstRef>> NestedType::GetFieldByName(
    std::string_view name) const {
  return GetFieldByName(name, /*case_sensitive=*/true);
}

StructType::StructType(std::vector<SchemaField> fields) : fields_(std::move(fields)) {}

TypeId StructType::type_id() const { return kTypeId; }

std::string StructType::ToString() const {
  std::string repr = "struct<\n";
  for (const auto& field : fields_) {
    std::format_to(std::back_inserter(repr), "  {}\n", field);
  }
  repr += ">";
  return repr;
}

std::span<const SchemaField> StructType::fields() const { return fields_; }
Result<std::optional<NestedType::SchemaFieldConstRef>> StructType::GetFieldById(
    int32_t field_id) const {
  ICEBERG_ASSIGN_OR_RAISE(auto field_by_id, field_by_id_.Get(*this));
  auto it = field_by_id.get().find(field_id);
  if (it == field_by_id.get().end()) return std::nullopt;
  return it->second;
}

Result<std::optional<NestedType::SchemaFieldConstRef>> StructType::GetFieldByIndex(
    int32_t index) const {
  if (index < 0 || static_cast<size_t>(index) >= fields_.size()) {
    return InvalidArgument("Invalid index {} to get field from struct", index);
  }
  return fields_[index];
}

Result<std::optional<NestedType::SchemaFieldConstRef>> StructType::GetFieldByName(
    std::string_view name, bool case_sensitive) const {
  if (case_sensitive) {
    ICEBERG_ASSIGN_OR_RAISE(auto field_by_name, field_by_name_.Get(*this));
    auto it = field_by_name.get().find(name);
    if (it != field_by_name.get().end()) {
      return it->second;
    }
    return std::nullopt;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto field_by_lowercase_name,
                          field_by_lowercase_name_.Get(*this));
  auto it = field_by_lowercase_name.get().find(StringUtils::ToLower(name));
  if (it != field_by_lowercase_name.get().end()) {
    return it->second;
  }
  return std::nullopt;
}

std::unique_ptr<Schema> StructType::ToSchema() const {
  return std::make_unique<Schema>(fields_);
}

bool StructType::Equals(const Type& other) const {
  if (other.type_id() != TypeId::kStruct) {
    return false;
  }
  const auto& struct_ = static_cast<const StructType&>(other);
  return fields_ == struct_.fields_;
}

Result<std::unordered_map<int32_t, StructType::SchemaFieldConstRef>>
StructType::InitFieldById(const StructType& self) {
  std::unordered_map<int32_t, SchemaFieldConstRef> field_by_id;
  for (const auto& field : self.fields_) {
    auto it = field_by_id.try_emplace(field.field_id(), field);
    if (!it.second) {
      return InvalidSchema("Duplicate field id found: {} (prev name: {}, curr name: {})",
                           field.field_id(), it.first->second.get().name(), field.name());
    }
  }
  return field_by_id;
}

Result<std::unordered_map<std::string_view, StructType::SchemaFieldConstRef>>
StructType::InitFieldByName(const StructType& self) {
  std::unordered_map<std::string_view, StructType::SchemaFieldConstRef> field_by_name;
  for (const auto& field : self.fields_) {
    auto it = field_by_name.try_emplace(field.name(), field);
    if (!it.second) {
      return InvalidSchema("Duplicate field name found: {} (prev id: {}, curr id: {})",
                           it.first->first, it.first->second.get().field_id(),
                           field.field_id());
    }
  }
  return field_by_name;
}

Result<std::unordered_map<std::string, StructType::SchemaFieldConstRef>>
StructType::InitFieldByLowerCaseName(const StructType& self) {
  std::unordered_map<std::string, SchemaFieldConstRef> field_by_lowercase_name;
  for (const auto& field : self.fields_) {
    auto it =
        field_by_lowercase_name.try_emplace(StringUtils::ToLower(field.name()), field);
    if (!it.second) {
      return InvalidSchema(
          "Duplicate lowercase field name found: {} (prev id: {}, curr id: {})",
          it.first->first, it.first->second.get().field_id(), field.field_id());
    }
  }
  return field_by_lowercase_name;
}

ListType::ListType(SchemaField element) : element_(std::move(element)) {
  ICEBERG_CHECK_OR_DIE(element_.name() == kElementName,
                       "ListType: child field name should be '{}', was '{}'",
                       kElementName, element_.name());
}

ListType::ListType(int32_t field_id, std::shared_ptr<Type> type, bool optional)
    : element_(field_id, std::string(kElementName), std::move(type), optional) {}

TypeId ListType::type_id() const { return kTypeId; }
std::string ListType::ToString() const { return std::format("list<{}>", element_); }

std::span<const SchemaField> ListType::fields() const { return {&element_, 1}; }
Result<std::optional<NestedType::SchemaFieldConstRef>> ListType::GetFieldById(
    int32_t field_id) const {
  if (field_id == element_.field_id()) {
    return std::cref(element_);
  }
  return std::nullopt;
}

Result<std::optional<NestedType::SchemaFieldConstRef>> ListType::GetFieldByIndex(
    int index) const {
  if (index == 0) {
    return std::cref(element_);
  }
  return InvalidArgument("Invalid index {} to get field from list", index);
}

Result<std::optional<NestedType::SchemaFieldConstRef>> ListType::GetFieldByName(
    std::string_view name, bool case_sensitive) const {
  if (case_sensitive) {
    if (name == kElementName) {
      return std::cref(element_);
    }
    return std::nullopt;
  }
  if (StringUtils::ToLower(name) == kElementName) {
    return std::cref(element_);
  }
  return std::nullopt;
}

bool ListType::Equals(const Type& other) const {
  if (other.type_id() != TypeId::kList) {
    return false;
  }
  const auto& list = static_cast<const ListType&>(other);
  return element_ == list.element_;
}

MapType::MapType(SchemaField key, SchemaField value)
    : fields_{std::move(key), std::move(value)} {
  ICEBERG_CHECK_OR_DIE(this->key().name() == kKeyName,
                       "MapType: key field name should be '{}', was '{}'", kKeyName,
                       this->key().name());
  ICEBERG_CHECK_OR_DIE(this->value().name() == kValueName,
                       "MapType: value field name should be '{}', was '{}'", kValueName,
                       this->value().name());
}

const SchemaField& MapType::key() const { return fields_[0]; }
const SchemaField& MapType::value() const { return fields_[1]; }
TypeId MapType::type_id() const { return kTypeId; }

std::string MapType::ToString() const {
  return std::format("map<{}: {}>", key(), value());
}

std::span<const SchemaField> MapType::fields() const { return fields_; }
Result<std::optional<NestedType::SchemaFieldConstRef>> MapType::GetFieldById(
    int32_t field_id) const {
  if (field_id == key().field_id()) {
    return key();
  } else if (field_id == value().field_id()) {
    return value();
  }
  return std::nullopt;
}

Result<std::optional<NestedType::SchemaFieldConstRef>> MapType::GetFieldByIndex(
    int32_t index) const {
  if (index == 0) {
    return key();
  } else if (index == 1) {
    return value();
  }
  return InvalidArgument("Invalid index {} to get field from map", index);
}

Result<std::optional<NestedType::SchemaFieldConstRef>> MapType::GetFieldByName(
    std::string_view name, bool case_sensitive) const {
  if (case_sensitive) {
    if (name == kKeyName) {
      return key();
    } else if (name == kValueName) {
      return value();
    }
    return std::nullopt;
  }
  const auto lower_case_name = StringUtils::ToLower(name);
  if (lower_case_name == kKeyName) {
    return key();
  } else if (lower_case_name == kValueName) {
    return value();
  }
  return std::nullopt;
}

bool MapType::Equals(const Type& other) const {
  if (other.type_id() != TypeId::kMap) {
    return false;
  }
  const auto& map = static_cast<const MapType&>(other);
  return fields_ == map.fields_;
}

TypeId BooleanType::type_id() const { return kTypeId; }
std::string BooleanType::ToString() const { return "boolean"; }
bool BooleanType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

TypeId IntType::type_id() const { return kTypeId; }
std::string IntType::ToString() const { return "int"; }
bool IntType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

TypeId LongType::type_id() const { return kTypeId; }
std::string LongType::ToString() const { return "long"; }
bool LongType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

TypeId FloatType::type_id() const { return kTypeId; }
std::string FloatType::ToString() const { return "float"; }
bool FloatType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

TypeId DoubleType::type_id() const { return kTypeId; }
std::string DoubleType::ToString() const { return "double"; }
bool DoubleType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

DecimalType::DecimalType(int32_t precision, int32_t scale)
    : precision_(precision), scale_(scale) {
  ICEBERG_CHECK_OR_DIE(precision >= 0 && precision <= kMaxPrecision,
                       "DecimalType: precision must be in [0, 38], was {}", precision);
}

int32_t DecimalType::precision() const { return precision_; }
int32_t DecimalType::scale() const { return scale_; }
TypeId DecimalType::type_id() const { return kTypeId; }
std::string DecimalType::ToString() const {
  return std::format("decimal({}, {})", precision_, scale_);
}
bool DecimalType::Equals(const Type& other) const {
  if (other.type_id() != kTypeId) {
    return false;
  }
  const auto& decimal = static_cast<const DecimalType&>(other);
  return precision_ == decimal.precision_ && scale_ == decimal.scale_;
}

TypeId DateType::type_id() const { return kTypeId; }
std::string DateType::ToString() const { return "date"; }
bool DateType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

TypeId TimeType::type_id() const { return kTypeId; }
std::string TimeType::ToString() const { return "time"; }
bool TimeType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

bool TimestampType::is_zoned() const { return false; }
TimeUnit TimestampType::time_unit() const { return TimeUnit::kMicrosecond; }
TypeId TimestampType::type_id() const { return kTypeId; }
std::string TimestampType::ToString() const { return "timestamp"; }
bool TimestampType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

bool TimestampTzType::is_zoned() const { return true; }
TimeUnit TimestampTzType::time_unit() const { return TimeUnit::kMicrosecond; }
TypeId TimestampTzType::type_id() const { return kTypeId; }
std::string TimestampTzType::ToString() const { return "timestamptz"; }
bool TimestampTzType::Equals(const Type& other) const {
  return other.type_id() == kTypeId;
}

TypeId StringType::type_id() const { return kTypeId; }
std::string StringType::ToString() const { return "string"; }
bool StringType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

TypeId UuidType::type_id() const { return kTypeId; }
std::string UuidType::ToString() const { return "uuid"; }
bool UuidType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

FixedType::FixedType(int32_t length) : length_(length) {
  ICEBERG_CHECK_OR_DIE(length >= 0, "FixedType: length must be >= 0, was {}", length);
}

int32_t FixedType::length() const { return length_; }
TypeId FixedType::type_id() const { return kTypeId; }
std::string FixedType::ToString() const { return std::format("fixed({})", length_); }
bool FixedType::Equals(const Type& other) const {
  if (other.type_id() != kTypeId) {
    return false;
  }
  const auto& fixed = static_cast<const FixedType&>(other);
  return length_ == fixed.length_;
}

TypeId BinaryType::type_id() const { return kTypeId; }
std::string BinaryType::ToString() const { return "binary"; }
bool BinaryType::Equals(const Type& other) const { return other.type_id() == kTypeId; }

// ----------------------------------------------------------------------
// Factory functions for creating primitive data types

#define TYPE_FACTORY(NAME, KLASS)                                     \
  const std::shared_ptr<KLASS>& NAME() {                              \
    static std::shared_ptr<KLASS> result = std::make_shared<KLASS>(); \
    return result;                                                    \
  }

TYPE_FACTORY(boolean, BooleanType)
TYPE_FACTORY(int32, IntType)
TYPE_FACTORY(int64, LongType)
TYPE_FACTORY(float32, FloatType)
TYPE_FACTORY(float64, DoubleType)
TYPE_FACTORY(date, DateType)
TYPE_FACTORY(time, TimeType)
TYPE_FACTORY(timestamp, TimestampType)
TYPE_FACTORY(timestamp_tz, TimestampTzType)
TYPE_FACTORY(binary, BinaryType)
TYPE_FACTORY(string, StringType)
TYPE_FACTORY(uuid, UuidType)

#undef TYPE_FACTORY

std::shared_ptr<DecimalType> decimal(int32_t precision, int32_t scale) {
  return std::make_shared<DecimalType>(precision, scale);
}

std::shared_ptr<FixedType> fixed(int32_t length) {
  return std::make_shared<FixedType>(length);
}

std::shared_ptr<MapType> map(SchemaField key, SchemaField value) {
  return std::make_shared<MapType>(key, value);
}

std::shared_ptr<ListType> list(SchemaField element) {
  return std::make_shared<ListType>(std::move(element));
}

std::shared_ptr<StructType> struct_(std::vector<SchemaField> fields) {
  return std::make_shared<StructType>(std::move(fields));
}

std::string_view ToString(TypeId id) {
  switch (id) {
    case TypeId::kStruct:
      return "struct";
    case TypeId::kList:
      return "list";
    case TypeId::kMap:
      return "map";
    case TypeId::kBoolean:
      return "boolean";
    case TypeId::kInt:
      return "int";
    case TypeId::kLong:
      return "long";
    case TypeId::kFloat:
      return "float";
    case TypeId::kDouble:
      return "double";
    case TypeId::kDecimal:
      return "decimal";
    case TypeId::kDate:
      return "date";
    case TypeId::kTime:
      return "time";
    case TypeId::kTimestamp:
      return "timestamp";
    case TypeId::kTimestampTz:
      return "timestamptz";
    case TypeId::kString:
      return "string";
    case TypeId::kUuid:
      return "uuid";
    case TypeId::kFixed:
      return "fixed";
    case TypeId::kBinary:
      return "binary";
  }

  std::unreachable();
}

}  // namespace iceberg
