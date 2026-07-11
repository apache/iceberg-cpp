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
#include <memory>
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
  /// \param[in] initial_default The v3 `initial-default`, or null if absent.
  /// \param[in] write_default The v3 `write-default`, or null if absent.
  SchemaField(int32_t field_id, std::string_view name, std::shared_ptr<Type> type,
              bool optional, std::string_view doc = {},
              std::shared_ptr<const Literal> initial_default = nullptr,
              std::shared_ptr<const Literal> write_default = nullptr);

  /// \brief Construct an optional (nullable) field.
  static SchemaField MakeOptional(int32_t field_id, std::string_view name,
                                  std::shared_ptr<Type> type, std::string_view doc = {});
  /// \brief Construct a required (non-null) field.
  static SchemaField MakeRequired(int32_t field_id, std::string_view name,
                                  std::shared_ptr<Type> type, std::string_view doc = {});

  /// \brief Return a copy with a new name.
  SchemaField WithName(std::string_view name) const;

  /// \brief Return a copy with a new type.
  SchemaField WithType(std::shared_ptr<Type> type) const;

  /// \brief Return a copy with new documentation.
  SchemaField WithDoc(std::string_view doc) const;

  /// \brief Return a copy with a new `initial-default`.
  SchemaField WithInitialDefault(std::shared_ptr<const Literal> initial_default) const;

  /// \brief Return a copy with a new `write-default`.
  SchemaField WithWriteDefault(std::shared_ptr<const Literal> write_default) const;

  /// \brief Cast a default literal to a field type.
  static Result<Literal> CastDefaultValue(
      const Literal& value, const std::shared_ptr<PrimitiveType>& target_type);

  /// \brief Get the field ID.
  int32_t field_id() const;

  /// \brief Get the field name.
  std::string_view name() const;

  /// \brief Get the field type.
  const std::shared_ptr<Type>& type() const;

  /// \brief Get whether the field is optional.
  [[nodiscard]] bool optional() const;

  /// \brief Get the field documentation.
  std::string_view doc() const;

  /// \brief Get the v3 `initial-default`, or null if absent.
  const std::shared_ptr<const Literal>& initial_default() const;

  /// \brief Get the v3 `write-default`, or null if absent.
  const std::shared_ptr<const Literal>& write_default() const;

  std::string ToString() const override;

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

 private:
  /// \brief Compare two fields for equality.
  bool Equals(const SchemaField& other) const;

  int32_t field_id_;
  std::string name_;
  std::shared_ptr<Type> type_;
  bool optional_;
  std::string doc_;
  std::shared_ptr<const Literal> initial_default_;
  std::shared_ptr<const Literal> write_default_;
};

}  // namespace iceberg
