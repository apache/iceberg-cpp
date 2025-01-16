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
#include "iceberg/schema_field.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief Interface for a data type for a field.
class ICEBERG_EXPORT Type : public iceberg::util::Formattable {
 public:
  virtual ~Type() = default;

  /// \brief Get the type ID.
  [[nodiscard]] virtual TypeId type_id() const = 0;

  /// \brief Is this a primitive type (may not have child fields)?
  [[nodiscard]] virtual bool is_primitive() const = 0;

  /// \brief Is this a nested type (may have child fields)?
  [[nodiscard]] virtual bool is_nested() const = 0;

  /// \brief Compare two types for equality.
  friend bool operator==(const Type& lhs, const Type& rhs) { return lhs.Equals(rhs); }

  /// \brief Compare two types for inequality.
  friend bool operator!=(const Type& lhs, const Type& rhs) { return !(lhs == rhs); }

 protected:
  /// \brief Compare two types for equality.
  [[nodiscard]] virtual bool Equals(const Type& other) const = 0;
};

/// \brief A data type that may not have child fields.
class ICEBERG_EXPORT PrimitiveType : public Type {
 public:
  bool is_primitive() const override { return true; }
  bool is_nested() const override { return false; }
};

/// \brief A data type that may have child fields.
class ICEBERG_EXPORT NestedType : public Type {
 public:
  bool is_primitive() const override { return false; }
  bool is_nested() const override { return true; }

  /// \brief Get a view of the child fields.
  [[nodiscard]] virtual std::span<const SchemaField> fields() const = 0;
  /// \brief Get a field by field ID.
  [[nodiscard]] virtual std::optional<std::reference_wrapper<const SchemaField>>
  GetFieldById(int32_t field_id) const = 0;
  /// \brief Get a field by index.
  [[nodiscard]] virtual std::optional<std::reference_wrapper<const SchemaField>>
  GetFieldByIndex(int i) const = 0;
  /// \brief Get a field by name.
  [[nodiscard]] virtual std::optional<std::reference_wrapper<const SchemaField>>
  GetFieldByName(std::string_view name) const = 0;
};

/// \defgroup type-primitive Primitive Types
/// Primitive types do not have nested fields.
/// @{

/// \brief A data type representing a boolean.
class ICEBERG_EXPORT BooleanType : public PrimitiveType {
 public:
  BooleanType() = default;
  ~BooleanType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a 32-bit signed integer.
class ICEBERG_EXPORT Int32Type : public PrimitiveType {
 public:
  Int32Type() = default;
  ~Int32Type() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a 64-bit signed integer.
class ICEBERG_EXPORT Int64Type : public PrimitiveType {
 public:
  Int64Type() = default;
  ~Int64Type() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a 32-bit (single precision) float.
class ICEBERG_EXPORT Float32Type : public PrimitiveType {
 public:
  Float32Type() = default;
  ~Float32Type() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a 64-bit (double precision) float.
class ICEBERG_EXPORT Float64Type : public PrimitiveType {
 public:
  Float64Type() = default;
  ~Float64Type() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a fixed-precision decimal.
class ICEBERG_EXPORT DecimalType : public PrimitiveType {
 public:
  constexpr static const int32_t kMaxPrecision = 38;

  /// \brief Construct a decimal type with the given precision and scale.
  DecimalType(int32_t precision, int32_t scale);
  ~DecimalType() = default;

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
  DateType() = default;
  ~DateType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a wall clock time in microseconds without
///   reference to a timezone or date.
class ICEBERG_EXPORT TimeType : public PrimitiveType {
 public:
  TimeType() = default;
  ~TimeType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a timestamp in microseconds without
///   reference to a timezone.
class ICEBERG_EXPORT TimestampType : public PrimitiveType {
 public:
  TimestampType() = default;
  ~TimestampType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a timestamp in microseconds in a
///   particular timezone.
class ICEBERG_EXPORT TimestampTzType : public PrimitiveType {
 public:
  TimestampTzType() = default;
  ~TimestampTzType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a bytestring.
class ICEBERG_EXPORT BinaryType : public PrimitiveType {
 public:
  BinaryType() = default;
  ~BinaryType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a string.
class ICEBERG_EXPORT StringType : public PrimitiveType {
 public:
  StringType() = default;
  ~StringType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// \brief A data type representing a fixed-length bytestring.
class ICEBERG_EXPORT FixedType : public PrimitiveType {
 public:
  /// \brief Construct a fixed type with the given length.
  FixedType(int32_t length);
  ~FixedType() = default;

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
  UuidType() = default;
  ~UuidType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

 protected:
  bool Equals(const Type& other) const override;
};

/// @}

/// \defgroup type-nested Nested Types
/// Nested types have nested fields.
/// @{

/// \brief A data type representing a list of values.
class ICEBERG_EXPORT ListType : public NestedType {
 public:
  constexpr static const std::string_view kElementName = "element";

  /// \brief Construct a list of the given element.  The name of the child
  ///   field should be "element".
  explicit ListType(SchemaField element);
  /// \brief Construct a list of the given element type.
  ListType(int32_t field_id, std::shared_ptr<Type> type, bool optional);
  ~ListType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

  std::span<const SchemaField> fields() const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldById(
      int32_t field_id) const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldByIndex(
      int i) const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldByName(
      std::string_view name) const override;

 protected:
  bool Equals(const Type& other) const override;

  SchemaField element_;
};

/// \brief A data type representing a dictionary of values.
class ICEBERG_EXPORT MapType : public NestedType {
 public:
  constexpr static const std::string_view kKeyName = "key";
  constexpr static const std::string_view kValueName = "value";

  /// \brief Construct a map of the given key/value fields.  The field names
  ///   should be "key" and "value", respectively.
  explicit MapType(SchemaField key, SchemaField value);
  ~MapType() = default;

  const SchemaField& key() const;
  const SchemaField& value() const;

  TypeId type_id() const override;
  std::string ToString() const override;

  std::span<const SchemaField> fields() const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldById(
      int32_t field_id) const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldByIndex(
      int i) const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldByName(
      std::string_view name) const override;

 protected:
  bool Equals(const Type& other) const override;

  std::array<SchemaField, 2> fields_;
};

/// \brief A data type representing a struct with nested fields.
class ICEBERG_EXPORT StructType : public NestedType {
 public:
  explicit StructType(std::vector<SchemaField> fields);
  ~StructType() = default;

  TypeId type_id() const override;
  std::string ToString() const override;

  std::span<const SchemaField> fields() const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldById(
      int32_t field_id) const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldByIndex(
      int i) const override;
  std::optional<std::reference_wrapper<const SchemaField>> GetFieldByName(
      std::string_view name) const override;

 protected:
  bool Equals(const Type& other) const override;

  std::vector<SchemaField> fields_;
  std::unordered_map<int32_t, size_t> field_id_to_index_;
};

/// @}

// TODO: need to specialize std::format (ideally via a trait?)

}  // namespace iceberg
