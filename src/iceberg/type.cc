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

#include "iceberg/exception.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

StructType::StructType(std::vector<SchemaField> fields) : fields_(std::move(fields)) {
  size_t index = 0;
  for (const auto& field : fields_) {
    auto [it, inserted] = field_id_to_index_.try_emplace(field.field_id(), index);
    if (!inserted) {
      throw IcebergError(
          std::format("StructType: duplicate field ID {} (field indices {} and {})",
                      field.field_id(), it->second, index));
    }

    ++index;
  }
}

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
std::optional<std::reference_wrapper<const SchemaField>> StructType::GetFieldById(
    int32_t field_id) const {
  auto it = field_id_to_index_.find(field_id);
  if (it == field_id_to_index_.end()) return std::nullopt;
  return fields_[it->second];
}
std::optional<std::reference_wrapper<const SchemaField>> StructType::GetFieldByIndex(
    int32_t index) const {
  if (index < 0 || index >= static_cast<int32_t>(fields_.size())) {
    return std::nullopt;
  }
  return fields_[index];
}
std::optional<std::reference_wrapper<const SchemaField>> StructType::GetFieldByName(
    std::string_view name) const {
  // N.B. duplicate names are not permitted (looking at the Java
  // implementation) so there is nothing in particular we need to do here
  for (const auto& field : fields_) {
    if (field.name() == name) {
      return field;
    }
  }
  return std::nullopt;
}
bool StructType::Equals(const Type& other) const {
  if (other.type_id() != TypeId::kStruct) {
    return false;
  }
  const auto& struct_ = static_cast<const StructType&>(other);
  return fields_ == struct_.fields_;
}

ListType::ListType(SchemaField element) : element_(std::move(element)) {
  if (element_.name() != kElementName) {
    throw IcebergError(std::format("ListType: child field name should be '{}', was '{}'",
                                   kElementName, element_.name()));
  }
}

ListType::ListType(int32_t field_id, std::shared_ptr<Type> type, bool optional)
    : element_(field_id, std::string(kElementName), std::move(type), optional) {}

TypeId ListType::type_id() const { return kTypeId; }
std::string ListType::ToString() const {
  // XXX: work around Clang/libc++: "<{}>" in a format string appears to get
  // parsed as {<>} or something; split up the format string to avoid that
  std::string repr = "list<";
  std::format_to(std::back_inserter(repr), "{}", element_);
  repr += ">";
  return repr;
}
std::span<const SchemaField> ListType::fields() const { return {&element_, 1}; }
std::optional<std::reference_wrapper<const SchemaField>> ListType::GetFieldById(
    int32_t field_id) const {
  if (field_id == element_.field_id()) {
    return std::cref(element_);
  }
  return std::nullopt;
}
std::optional<std::reference_wrapper<const SchemaField>> ListType::GetFieldByIndex(
    int index) const {
  if (index == 0) {
    return std::cref(element_);
  }
  return std::nullopt;
}
std::optional<std::reference_wrapper<const SchemaField>> ListType::GetFieldByName(
    std::string_view name) const {
  if (name == element_.name()) {
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
  if (this->key().name() != kKeyName) {
    throw IcebergError(std::format("MapType: key field name should be '{}', was '{}'",
                                   kKeyName, this->key().name()));
  }
  if (this->value().name() != kValueName) {
    throw IcebergError(std::format("MapType: value field name should be '{}', was '{}'",
                                   kValueName, this->value().name()));
  }
}

const SchemaField& MapType::key() const { return fields_[0]; }
const SchemaField& MapType::value() const { return fields_[1]; }
TypeId MapType::type_id() const { return kTypeId; }
std::string MapType::ToString() const {
  // XXX: work around Clang/libc++: "<{}>" in a format string appears to get
  // parsed as {<>} or something; split up the format string to avoid that
  std::string repr = "map<";

  std::format_to(std::back_inserter(repr), "{}: {}", key(), value());
  repr += ">";
  return repr;
}
std::span<const SchemaField> MapType::fields() const { return fields_; }
std::optional<std::reference_wrapper<const SchemaField>> MapType::GetFieldById(
    int32_t field_id) const {
  if (field_id == key().field_id()) {
    return key();
  } else if (field_id == value().field_id()) {
    return value();
  }
  return std::nullopt;
}
std::optional<std::reference_wrapper<const SchemaField>> MapType::GetFieldByIndex(
    int32_t index) const {
  if (index == 0) {
    return key();
  } else if (index == 1) {
    return value();
  }
  return std::nullopt;
}
std::optional<std::reference_wrapper<const SchemaField>> MapType::GetFieldByName(
    std::string_view name) const {
  if (name == kKeyName) {
    return key();
  } else if (name == kValueName) {
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
  if (precision < 0 || precision > kMaxPrecision) {
    throw IcebergError(
        std::format("DecimalType: precision must be in [0, 38], was {}", precision));
  }
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
  if (length < 0) {
    throw IcebergError(std::format("FixedType: length must be >= 0, was {}", length));
  }
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

}  // namespace iceberg
