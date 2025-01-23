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
#include <stdexcept>

#include "iceberg/util/formatter.h"

namespace iceberg {

TypeId BooleanType::type_id() const { return TypeId::kBoolean; }
std::string BooleanType::ToString() const { return "boolean"; }
bool BooleanType::Equals(const Type& other) const {
  return other.type_id() == TypeId::kBoolean;
}

TypeId Int32Type::type_id() const { return TypeId::kInt32; }
std::string Int32Type::ToString() const { return "int32"; }
bool Int32Type::Equals(const Type& other) const {
  return other.type_id() == TypeId::kInt32;
}

TypeId Int64Type::type_id() const { return TypeId::kInt64; }
std::string Int64Type::ToString() const { return "int64"; }
bool Int64Type::Equals(const Type& other) const {
  return other.type_id() == TypeId::kInt64;
}

TypeId Float32Type::type_id() const { return TypeId::kFloat32; }
std::string Float32Type::ToString() const { return "float32"; }
bool Float32Type::Equals(const Type& other) const {
  return other.type_id() == TypeId::kFloat32;
}

TypeId Float64Type::type_id() const { return TypeId::kFloat64; }
std::string Float64Type::ToString() const { return "float64"; }
bool Float64Type::Equals(const Type& other) const {
  return other.type_id() == TypeId::kFloat64;
}

DecimalType::DecimalType(int32_t precision, int32_t scale)
    : precision_(precision), scale_(scale) {
  if (precision < 0 || precision > kMaxPrecision) {
    throw std::runtime_error(
        std::format("DecimalType: precision must be in [0, 38], was {}", precision));
  }
}

int32_t DecimalType::precision() const { return precision_; }
int32_t DecimalType::scale() const { return scale_; }
TypeId DecimalType::type_id() const { return TypeId::kDecimal; }
std::string DecimalType::ToString() const {
  return std::format("decimal({}, {})", precision_, scale_);
}
bool DecimalType::Equals(const Type& other) const {
  if (other.type_id() != TypeId::kDecimal) {
    return false;
  }
  const auto& decimal = static_cast<const DecimalType&>(other);
  return precision_ == decimal.precision_ && scale_ == decimal.scale_;
}

TypeId TimeType::type_id() const { return TypeId::kTime; }
std::string TimeType::ToString() const { return "time"; }
bool TimeType::Equals(const Type& other) const {
  return other.type_id() == TypeId::kTime;
}

TypeId DateType::type_id() const { return TypeId::kDate; }
std::string DateType::ToString() const { return "date"; }
bool DateType::Equals(const Type& other) const {
  return other.type_id() == TypeId::kDate;
}

bool TimestampType::is_zoned() const { return false; }
TimeUnit TimestampType::time_unit() const { return TimeUnit::kMicrosecond; }
TypeId TimestampType::type_id() const { return TypeId::kTimestamp; }
std::string TimestampType::ToString() const { return "timestamp"; }
bool TimestampType::Equals(const Type& other) const {
  return other.type_id() == TypeId::kTimestamp;
}

bool TimestampTzType::is_zoned() const { return true; }
TimeUnit TimestampTzType::time_unit() const { return TimeUnit::kMicrosecond; }
TypeId TimestampTzType::type_id() const { return TypeId::kTimestampTz; }
std::string TimestampTzType::ToString() const { return "timestamptz"; }
bool TimestampTzType::Equals(const Type& other) const {
  return other.type_id() == TypeId::kTimestampTz;
}

TypeId BinaryType::type_id() const { return TypeId::kBinary; }
std::string BinaryType::ToString() const { return "binary"; }
bool BinaryType::Equals(const Type& other) const {
  return other.type_id() == TypeId::kBinary;
}

TypeId StringType::type_id() const { return TypeId::kString; }
std::string StringType::ToString() const { return "string"; }
bool StringType::Equals(const Type& other) const {
  return other.type_id() == TypeId::kString;
}

FixedType::FixedType(int32_t length) : length_(length) {
  if (length < 0) {
    throw std::runtime_error(
        std::format("FixedType: length must be >= 0, was {}", length));
  }
}

int32_t FixedType::length() const { return length_; }
TypeId FixedType::type_id() const { return TypeId::kFixed; }
std::string FixedType::ToString() const { return std::format("fixed({})", length_); }
bool FixedType::Equals(const Type& other) const {
  if (other.type_id() != TypeId::kFixed) {
    return false;
  }
  const auto& fixed = static_cast<const FixedType&>(other);
  return length_ == fixed.length_;
}

TypeId UuidType::type_id() const { return TypeId::kUuid; }
std::string UuidType::ToString() const { return "uuid"; }
bool UuidType::Equals(const Type& other) const {
  return other.type_id() == TypeId::kUuid;
}

ListType::ListType(SchemaField element) : element_(std::move(element)) {
  if (element_.name() != kElementName) {
    throw std::runtime_error(
        std::format("ListType: child field name should be '{}', was '{}'", kElementName,
                    element_.name()));
  }
}

ListType::ListType(int32_t field_id, std::shared_ptr<Type> type, bool optional)
    : element_(field_id, std::string(kElementName), std::move(type), optional) {}

TypeId ListType::type_id() const { return TypeId::kList; }
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
    throw std::runtime_error(
        std::format("MapType: key field name should be '{}', was '{}'", kKeyName,
                    this->key().name()));
  }
  if (this->value().name() != kValueName) {
    throw std::runtime_error(
        std::format("MapType: value field name should be '{}', was '{}'", kValueName,
                    this->value().name()));
  }
}

const SchemaField& MapType::key() const { return fields_[0]; }
const SchemaField& MapType::value() const { return fields_[1]; }
TypeId MapType::type_id() const { return TypeId::kMap; }
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
  } else if (index == 0) {
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

StructType::StructType(std::vector<SchemaField> fields) : fields_(std::move(fields)) {
  size_t index = 0;
  for (const auto& field : fields_) {
    auto [it, inserted] = field_id_to_index_.try_emplace(field.field_id(), index);
    if (!inserted) {
      throw std::runtime_error(
          std::format("StructType: duplicate field ID {} (field indices {} and {})",
                      field.field_id(), it->second, index));
    }

    index++;
  }
}

TypeId StructType::type_id() const { return TypeId::kStruct; }
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
  if (index < 0 || index >= static_cast<int>(fields_.size())) {
    return std::nullopt;
  }
  return fields_[index];
}
std::optional<std::reference_wrapper<const SchemaField>> StructType::GetFieldByName(
    std::string_view name) const {
  // TODO: what is the right behavior if there are duplicate names? (Are
  // duplicate names permitted?)
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

}  // namespace iceberg
