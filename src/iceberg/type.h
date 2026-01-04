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

/// \file iceberg/type.h
/// Data types for Iceberg.  This header defines the data types, but see
/// iceberg/type_fwd.h for the enum defining the list of types.

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/util/formattable.h"
#include "iceberg/util/lazy.h"

namespace iceberg {

/// \brief Interface for a data type for a field.
class ICEBERG_EXPORT Type : public iceberg::util::Formattable {
 public:
  ~Type() override = default;

  /// \brief Get the type ID.
  [[nodiscard]] virtual TypeId type_id() const = 0;

  /// \brief Is this a primitive type (may not have child fields)?
  [[nodiscard]] virtual bool is_primitive() const = 0;

  /// \brief Is this a nested type (may have child fields)?
  [[nodiscard]] virtual bool is_nested() const = 0;

  /// \brief Compare two types for equality.
  friend bool operator==(const Type& lhs, const Type& rhs) { return lhs.Equals(rhs); }

 protected:
  /// \brief Compare two types for equality.
  [[nodiscard]] virtual bool Equals(const Type& other) const = 0;
};

/// \brief A data type that does not have child fields.
class ICEBERG_EXPORT PrimitiveType : public Type {
 public:
  bool is_primitive() const override { return true; }
  bool is_nested() const override { return false; }
};

/// \brief A data type that has child fields.
class ICEBERG_EXPORT NestedType : public Type {
 public:
  bool is_primitive() const override { return false; }
  bool is_nested() const override { return true; }

  /// \brief Get a view of the child fields.
  [[nodiscard]] virtual std::span<const SchemaField> fields() const = 0;
  using SchemaFieldConstRef = std::reference_wrapper<const SchemaField>;
  /// \brief Get a field by field ID.
  ///
  /// \note This is O(1) complexity.
  [[nodiscard]] virtual Result<std::optional<SchemaFieldConstRef>> GetFieldById(
      int32_t field_id) const = 0;
  /// \brief Get a field by index.
  ///
  /// \note This is O(1) complexity.
  [[nodiscard]] virtual Result<std::optional<SchemaFieldConstRef>> GetFieldByIndex(
      int32_t index) const = 0;
  /// \brief Get a field by name.  Return an error Status if
  ///   the field name is not unique; prefer GetFieldById or GetFieldByIndex
  ///   when possible.
  ///
  /// \note This is O(1) complexity.
  [[nodiscard]] virtual Result<std::optional<SchemaFieldConstRef>> GetFieldByName(
      std::string_view name, bool case_sensitive) const = 0;
  /// \brief Get a field by name (case-sensitive).
  [[nodiscard]] Result<std::optional<SchemaFieldConstRef>> GetFieldByName(
      std::string_view name) const;
};

/// \defgroup type-nested Nested Types
/// Nested types have child fields.
/// @{

/// \brief A data type representing a struct with nested fields.
class ICEBERG_EXPORT StructType : public NestedType {
 public:
  constexpr static TypeId kTypeId = TypeId::kStruct;
  explicit StructType(std::vector<SchemaField> fields);
  ~StructType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

  std::span<const SchemaField> fields() const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldById(
      int32_t field_id) const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldByIndex(
      int32_t index) const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldByName(
      std::string_view name, bool case_sensitive) const override;
  using NestedType::GetFieldByName;

  std::unique_ptr<Schema> ToSchema() const;

 protected:
  bool Equals(const Type& other) const override;

  static Result<std::unordered_map<int32_t, SchemaFieldConstRef>> InitFieldById(
      const StructType&);
  static Result<std::unordered_map<std::string_view, SchemaFieldConstRef>>
  InitFieldByName(const StructType&);
  static Result<std::unordered_map<std::string, SchemaFieldConstRef>>
  InitFieldByLowerCaseName(const StructType&);

  std::vector<SchemaField> fields_;
  Lazy<InitFieldById> field_by_id_;
  Lazy<InitFieldByName> field_by_name_;
  Lazy<InitFieldByLowerCaseName> field_by_lowercase_name_;
};

/// \brief A data type representing a list of values.
class ICEBERG_EXPORT ListType : public NestedType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kList;
  constexpr static const std::string_view kElementName = "element";

  /// \brief Construct a list of the given element.  The name of the child
  ///   field should be "element".
  explicit ListType(SchemaField element);
  /// \brief Construct a list of the given element type.
  ListType(int32_t field_id, std::shared_ptr<Type> type, bool optional);
  ~ListType() override = default;

  TypeId type_id() const override;
  const SchemaField& element() const;
  std::string ToString() const override;

  std::span<const SchemaField> fields() const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldById(
      int32_t field_id) const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldByIndex(
      int32_t index) const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldByName(
      std::string_view name, bool case_sensitive) const override;
  using NestedType::GetFieldByName;

 protected:
  bool Equals(const Type& other) const override;

  SchemaField element_;
};

/// \brief A data type representing a dictionary of values.
class ICEBERG_EXPORT MapType : public NestedType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kMap;
  constexpr static const std::string_view kKeyName = "key";
  constexpr static const std::string_view kValueName = "value";

  /// \brief Construct a map of the given key/value fields.  The field names
  ///   should be "key" and "value", respectively.
  explicit MapType(SchemaField key, SchemaField value);
  ~MapType() override = default;

  const SchemaField& key() const;
  const SchemaField& value() const;

  TypeId type_id() const override;
  std::string ToString() const override;

  std::span<const SchemaField> fields() const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldById(
      int32_t field_id) const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldByIndex(
      int32_t index) const override;
  Result<std::optional<SchemaFieldConstRef>> GetFieldByName(
      std::string_view name, bool case_sensitive) const override;
  using NestedType::GetFieldByName;

 protected:
  bool Equals(const Type& other) const override;

  std::array<SchemaField, 2> fields_;
};

/// @}

/// \defgroup type-primitive Primitive Types
/// Primitive types do not have nested fields.
/// @{

/// \brief A data type representing a boolean (true or false).
class ICEBERG_EXPORT BooleanType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kBoolean;

  BooleanType() = default;
  ~BooleanType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a 32-bit signed integer.
class ICEBERG_EXPORT IntType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kInt;

  IntType() = default;
  ~IntType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a 64-bit signed integer.
class ICEBERG_EXPORT LongType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kLong;

  LongType() = default;
  ~LongType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a 32-bit (single precision) IEEE-754
///   float.
class ICEBERG_EXPORT FloatType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kFloat;

  FloatType() = default;
  ~FloatType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a 64-bit (double precision) IEEE-754
///   float.
class ICEBERG_EXPORT DoubleType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kDouble;

  DoubleType() = default;
  ~DoubleType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a fixed-precision decimal.
class ICEBERG_EXPORT DecimalType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kDecimal;
  constexpr static const int32_t kMaxPrecision = 38;

  /// \brief Construct a decimal type with the given precision and scale.
  DecimalType(int32_t precision, int32_t scale);
  ~DecimalType() override = default;

  /// \brief Get the precision (the number of decimal digits).
  [[nodiscard]] int32_t precision() const;
  /// \brief Get the scale (essentially, the number of decimal digits after
  ///   the decimal point; precisely, the value is scaled by $$10^{-s}$$.).
  [[nodiscard]] int32_t scale() const;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;

 private:
  int32_t precision_;
  int32_t scale_;
};

/// \brief A data type representing a calendar date without reference to a
///   timezone or time.
class ICEBERG_EXPORT DateType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kDate;

  DateType() = default;
  ~DateType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a wall clock time in microseconds without
///   reference to a timezone or date.
class ICEBERG_EXPORT TimeType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kTime;

  TimeType() = default;
  ~TimeType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A base class for any timestamp time (irrespective of unit or
///   timezone).
class ICEBERG_EXPORT TimestampBase : public PrimitiveType {
 public:
  /// \brief Is this type zoned or naive?
  [[nodiscard]] virtual bool is_zoned() const = 0;
  /// \brief The time resolution.
  [[nodiscard]] virtual TimeUnit time_unit() const = 0;
};

/// \brief A data type representing a timestamp in microseconds without
///   reference to a timezone.
class ICEBERG_EXPORT TimestampType : public TimestampBase {
 public:
  constexpr static const TypeId kTypeId = TypeId::kTimestamp;

  TimestampType() = default;
  ~TimestampType() override = default;

  bool is_zoned() const override;
  TimeUnit time_unit() const override;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a timestamp as microseconds since the
///   epoch in UTC.  A time zone or offset is not stored.
class ICEBERG_EXPORT TimestampTzType : public TimestampBase {
 public:
  constexpr static const TypeId kTypeId = TypeId::kTimestampTz;

  TimestampTzType() = default;
  ~TimestampTzType() override = default;

  bool is_zoned() const override;
  TimeUnit time_unit() const override;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing an arbitrary-length byte sequence.
class ICEBERG_EXPORT BinaryType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kBinary;

  BinaryType() = default;
  ~BinaryType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing an arbitrary-length character sequence
///   (encoded in UTF-8).
class ICEBERG_EXPORT StringType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kString;

  StringType() = default;
  ~StringType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a fixed-length bytestring.
class ICEBERG_EXPORT FixedType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kFixed;

  /// \brief Construct a fixed type with the given length.
  explicit FixedType(int32_t length);
  ~FixedType() override = default;

  /// \brief The length (the number of bytes to store).
  [[nodiscard]] int32_t length() const;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;

 private:
  int32_t length_;
};

/// \brief A data type representing a UUID.  While defined as a distinct type,
///   it is effectively a fixed(16).
class ICEBERG_EXPORT UuidType : public PrimitiveType {
 public:
  constexpr static const TypeId kTypeId = TypeId::kUuid;

  UuidType() = default;
  ~UuidType() override = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// @}

/// \defgroup type-factories Factory functions for creating primitive data types
///
/// Factory functions for creating primitive data types
/// @{

/// \brief Return a BooleanType instance.
ICEBERG_EXPORT const std::shared_ptr<BooleanType>& boolean();
/// \brief Return an IntType instance.
ICEBERG_EXPORT const std::shared_ptr<IntType>& int32();
/// \brief Return a LongType instance.
ICEBERG_EXPORT const std::shared_ptr<LongType>& int64();
/// \brief Return a FloatType instance.
ICEBERG_EXPORT const std::shared_ptr<FloatType>& float32();
/// \brief Return a DoubleType instance.
ICEBERG_EXPORT const std::shared_ptr<DoubleType>& float64();
/// \brief Return a DateType instance.
ICEBERG_EXPORT const std::shared_ptr<DateType>& date();
/// \brief Return a TimeType instance.
ICEBERG_EXPORT const std::shared_ptr<TimeType>& time();
/// \brief Return a TimestampType instance.
ICEBERG_EXPORT const std::shared_ptr<TimestampType>& timestamp();
/// \brief Return a TimestampTzType instance.
ICEBERG_EXPORT const std::shared_ptr<TimestampTzType>& timestamp_tz();
/// \brief Return a BinaryType instance.
ICEBERG_EXPORT const std::shared_ptr<BinaryType>& binary();
/// \brief Return a StringType instance.
ICEBERG_EXPORT const std::shared_ptr<StringType>& string();
/// \brief Return a UuidType instance.
ICEBERG_EXPORT const std::shared_ptr<UuidType>& uuid();

/// \brief Create a DecimalType with the given precision and scale.
/// \param precision The number of decimal digits (max 38).
/// \param scale The number of decimal digits after the decimal point.
/// \return A shared pointer to the DecimalType instance.
ICEBERG_EXPORT std::shared_ptr<DecimalType> decimal(int32_t precision, int32_t scale);

/// \brief Create a FixedType with the given length.
/// \param length The number of bytes to store (must be >= 0).
/// \return A shared pointer to the FixedType instance.
ICEBERG_EXPORT std::shared_ptr<FixedType> fixed(int32_t length);

/// \brief Create a StructType with the given fields.
/// \param fields The fields of the struct.
/// \return A shared pointer to the StructType instance.
ICEBERG_EXPORT std::shared_ptr<StructType> struct_(std::vector<SchemaField> fields);

/// \brief Create a ListType with the given element field.
/// \param element The element field of the list.
/// \return A shared pointer to the ListType instance.
ICEBERG_EXPORT std::shared_ptr<ListType> list(SchemaField element);

/// \brief Create a MapType with the given key and value fields.
/// \param key The key field of the map.
/// \param value The value field of the map.
/// \return A shared pointer to the MapType instance.
ICEBERG_EXPORT std::shared_ptr<MapType> map(SchemaField key, SchemaField value);

/// @}

/// \brief Get the lowercase string representation of a TypeId.
///
/// This returns the same lowercase string as used by Type::ToString() methods.
/// For example: TypeId::kBoolean -> "boolean", TypeId::kInt -> "int", etc.
///
/// \param id The TypeId to convert to string
/// \return A string_view containing the lowercase type name
ICEBERG_EXPORT std::string_view ToString(TypeId id);

}  // namespace iceberg
