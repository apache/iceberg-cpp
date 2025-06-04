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

#include "iceberg/type.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Exception type for values that are below the minimum allowed value for a primitive type.
///
/// When casting a value to a narrow primitive type, if the value exceeds the maximum of dest type,
/// it might be above the maximum allowed value for that type.
struct BelowMin {
  bool operator==(const BelowMin&) const = default;
  std::strong_ordering operator<=>(const BelowMin&) const = default;
};

/// \brief Exception type for values that are above the maximum allowed value for a primitive type.
///
/// When casting a value to a narrow primitive type, if the value exceeds the maximum of dest type,
/// it might be above the maximum allowed value for that type.
struct AboveMax {
  bool operator==(const AboveMax&) const = default;
  std::strong_ordering operator<=>(const AboveMax&) const = default;
};

// TODO(mwish): Supports More types
using PrimitiveLiteralValue =
    std::variant<bool, int32_t, int64_t, float, double, std::string, std::vector<uint8_t>, BelowMin, AboveMax>;

/// \brief PrimitiveLiteral is owned literal of a primitive type.
class PrimitiveLiteral {
 public:
  explicit PrimitiveLiteral(PrimitiveLiteralValue value, std::shared_ptr<PrimitiveType> type);

  // Factory methods for primitive types
  static PrimitiveLiteral Boolean(bool value);
  static PrimitiveLiteral Integer(int32_t value);
  static PrimitiveLiteral Long(int64_t value);
  static PrimitiveLiteral Float(float value);
  static PrimitiveLiteral Double(double value);
  static PrimitiveLiteral String(std::string value);
  static PrimitiveLiteral Binary(std::vector<uint8_t> value);

  /// Create iceberg value from bytes.
  ///
  /// See [this spec](https://iceberg.apache.org/spec/#binary-single-value-serialization) for reference.
  static Result<PrimitiveLiteral> Deserialize(std::span<const uint8_t> data);
  /// Serialize iceberg value to bytes.
  ///
  /// See [this spec](https://iceberg.apache.org/spec/#binary-single-value-serialization) for reference.
  Result<std::vector<uint8_t>> Serialize() const;

  // Get the value as a variant
  const PrimitiveLiteralValue& value() const;

  // Get the Iceberg Type of the literal
  const std::shared_ptr<PrimitiveType>& type() const;

  // Cast the literal to a specific type
  Result<PrimitiveLiteral> CastTo(const std::shared_ptr<PrimitiveType>& target_type) const;

  std::partial_ordering operator<=>(const PrimitiveLiteral& other) const;

 private:
  PrimitiveLiteralValue value_;
  std::shared_ptr<PrimitiveType> type_;
};

}  // namespace iceberg

