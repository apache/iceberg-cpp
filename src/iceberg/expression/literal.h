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

#include <compare>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/type.h"

namespace iceberg {

/// \brief Literal is a literal value that is associated with a primitive type.
class ICEBERG_EXPORT Literal {
 private:
  /// \brief Sentinel value to indicate that the literal value is below the valid range
  /// of a specific primitive type. It can happen when casting a literal to a narrower
  /// primitive type.
  struct BelowMin {
    bool operator==(const BelowMin&) const = default;
    std::strong_ordering operator<=>(const BelowMin&) const = default;
  };

  /// \brief Sentinel value to indicate that the literal value is above the valid range
  /// of a specific primitive type. It can happen when casting a literal to a narrower
  /// primitive type.
  struct AboveMax {
    bool operator==(const AboveMax&) const = default;
    std::strong_ordering operator<=>(const AboveMax&) const = default;
  };

  using Value = std::variant<bool,         // for boolean
                             int32_t,      // for int, date
                             int64_t,      // for long, timestamp, timestamp_tz, time
                             float,        // for float
                             double,       // for double
                             std::string,  // for string
                             std::vector<uint8_t>,     // for binary, fixed
                             std::array<uint8_t, 16>,  // for uuid and decimal
                             BelowMin, AboveMax>;

 public:
  /// \brief Factory methods for primitive types
  static Literal Boolean(bool value);
  static Literal Int(int32_t value);
  static Literal Long(int64_t value);
  static Literal Float(float value);
  static Literal Double(double value);
  static Literal String(std::string value);
  static Literal Binary(std::vector<uint8_t> value);

  /// \brief Restore a literal from single-value serialization.
  ///
  /// See [this spec](https://iceberg.apache.org/spec/#binary-single-value-serialization)
  /// for reference.
  static Result<Literal> Deserialize(std::span<const uint8_t> data,
                                     std::shared_ptr<PrimitiveType> type);

  /// \brief Perform single-value serialization.
  ///
  /// See [this spec](https://iceberg.apache.org/spec/#binary-single-value-serialization)
  /// for reference.
  Result<std::vector<uint8_t>> Serialize() const;

  /// \brief Get the literal type.
  const std::shared_ptr<PrimitiveType>& type() const;

  /// \brief Converts this literal to a literal of the given type.
  ///
  /// When a predicate is bound to a concrete data column, literals are converted to match
  /// the bound column's type. This conversion process is more narrow than a cast and is
  /// only intended for cases where substituting one type is a common mistake (e.g. 34
  /// instead of 34L) or where this API avoids requiring a concrete class (e.g., dates).
  ///
  /// If conversion to a target type is not supported, this method returns an error.
  ///
  /// This method may return BelowMin or AboveMax when the target type is not as wide as
  /// the original type. These values indicate that the containing predicate can be
  /// simplified. For example, std::numeric_limits<int>::max()+1 converted to an int will
  /// result in AboveMax and can simplify a < std::numeric_limits<int>::max()+1 to always
  /// true.
  ///
  /// \param target_type A primitive PrimitiveType
  /// \return A Result containing a literal of the given type or an error if conversion
  /// was not valid
  Result<Literal> CastTo(const std::shared_ptr<PrimitiveType>& target_type) const;

  bool operator==(const Literal& other) const;

  /// \brief Compare two PrimitiveLiterals. Both literals must have the same type
  /// and should not be AboveMax or BelowMin.
  std::partial_ordering operator<=>(const Literal& other) const;

  /// Check if this literal represents a value above the maximum allowed value
  /// for its type. This occurs when casting from a wider type to a narrower type
  /// and the value exceeds the target type's maximum.
  /// \return true if this literal represents an AboveMax value, false otherwise
  bool IsAboveMax() const;

  /// Check if this literal represents a value below the minimum allowed value
  /// for its type. This occurs when casting from a wider type to a narrower type
  /// and the value is less than the target type's minimum.
  /// \return true if this literal represents a BelowMin value, false otherwise
  bool IsBelowMin() const;

  std::string ToString() const;

 private:
  Literal(Value value, std::shared_ptr<PrimitiveType> type);

  friend class LiteralCaster;

 private:
  Value value_;
  std::shared_ptr<PrimitiveType> type_;
};

}  // namespace iceberg
