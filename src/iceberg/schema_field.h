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

/// \file iceberg/schema_field.h
/// A (schema) field is a name and a type and is part of a schema or nested
/// type (e.g. a struct).

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief A type combined with a name.
class ICEBERG_EXPORT SchemaField : public iceberg::util::Formattable {
 public:
  static constexpr int32_t kInvalidFieldId = -1;

  /// \brief Construct a field.
  /// \param[in] field_id The field ID.
  /// \param[in] name The field name.
  /// \param[in] type The field type.
  /// \param[in] optional Whether values of this field are required or nullable.
  /// \param[in] doc Optional documentation string for the field.
  SchemaField(int32_t field_id, std::string_view name, std::shared_ptr<Type> type,
              bool optional, std::string_view doc = {});

  /// \brief Construct an optional (nullable) field.
  static SchemaField MakeOptional(int32_t field_id, std::string_view name,
                                  std::shared_ptr<Type> type, std::string_view doc = {});
  /// \brief Construct a required (non-null) field.
  static SchemaField MakeRequired(int32_t field_id, std::string_view name,
                                  std::shared_ptr<Type> type, std::string_view doc = {});

  /// \brief Get the field ID.
  [[nodiscard]] int32_t field_id() const;

  /// \brief Get the field name.
  [[nodiscard]] std::string_view name() const;

  /// \brief Get the field type.
  [[nodiscard]] const std::shared_ptr<Type>& type() const;

  /// \brief Get whether the field is optional.
  [[nodiscard]] bool optional() const;

  /// \brief Get the field documentation.
  std::string_view doc() const;

  /// \brief Get the default value for this field used when reading rows written
  /// before the field existed (v3 `initial-default`). Empty if absent.
  ///
  /// The returned reference is a non-owning view into a value owned by this field;
  /// it remains valid for the lifetime of this SchemaField.
  [[nodiscard]] std::optional<std::reference_wrapper<const Literal>> initial_default()
      const;

  /// \brief Get the default value for this field used when a writer does not
  /// supply a value (v3 `write-default`). Empty if absent.
  ///
  /// The returned reference is a non-owning view into a value owned by this field;
  /// it remains valid for the lifetime of this SchemaField.
  [[nodiscard]] std::optional<std::reference_wrapper<const Literal>> write_default()
      const;

  [[nodiscard]] std::string ToString() const override;

  Status Validate() const;

  friend bool operator==(const SchemaField& lhs, const SchemaField& rhs) {
    return lhs.Equals(rhs);
  }

  SchemaField AsRequired() const {
    auto copy = *this;
    copy.optional_ = false;
    return copy;
  }

  SchemaField AsOptional() const {
    auto copy = *this;
    copy.optional_ = true;
    return copy;
  }

  /// \brief Return a copy of this field with a different field id and type, preserving
  /// every other attribute (name, nullability, doc, and default values).
  ///
  /// Used when fields are rebuilt during id reassignment or type rewriting; keeping
  /// the copy semantics in one place ensures no attribute is silently dropped.
  [[nodiscard]] SchemaField WithIdAndType(int32_t field_id,
                                          std::shared_ptr<Type> type) const;

  /// \brief Return a copy of this field with the given `initial-default` value.
  ///
  /// The returned field takes ownership of the value.
  [[nodiscard]] SchemaField WithInitialDefault(Literal initial_default) const;

  /// \brief Return a copy of this field with the `initial-default` value copied from
  /// `initial_default`, or without one if it is empty.
  ///
  /// The referenced value is copied; no reference to it is retained.
  [[nodiscard]] SchemaField WithInitialDefault(
      std::optional<std::reference_wrapper<const Literal>> initial_default) const;

  /// \brief Return a copy of this field with the given `write-default` value.
  ///
  /// The returned field takes ownership of the value.
  [[nodiscard]] SchemaField WithWriteDefault(Literal write_default) const;

  /// \brief Return a copy of this field with the `write-default` value copied from
  /// `write_default`, or without one if it is empty.
  ///
  /// The referenced value is copied; no reference to it is retained.
  [[nodiscard]] SchemaField WithWriteDefault(
      std::optional<std::reference_wrapper<const Literal>> write_default) const;

 private:
  /// \brief Compare two fields for equality.
  [[nodiscard]] bool Equals(const SchemaField& other) const;

  int32_t field_id_;
  std::string name_;
  std::shared_ptr<Type> type_;
  bool optional_;
  std::string doc_;
  // Default values are owned by this field and never mutated after being set; copies
  // of the field share the same payload (reference-counted) instead of deep-copying,
  // like `type_` above. Sharing is unobservable because the payload is immutable.
  std::shared_ptr<const Literal> initial_default_;
  std::shared_ptr<const Literal> write_default_;
};

}  // namespace iceberg
