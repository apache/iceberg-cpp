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

#include "iceberg/expression/literal.h"

#include <cmath>
#include <concepts>

#include "iceberg/exception.h"
#include "iceberg/util/conversions.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief LiteralCaster handles type casting operations for Literal.
/// This is an internal implementation class.
class LiteralCaster {
 public:
  /// Cast a Literal to the target type.
  static Result<Literal> CastTo(const Literal& literal,
                                const std::shared_ptr<PrimitiveType>& target_type);

  /// Create a literal representing a value below the minimum for the given type.
  static Literal BelowMinLiteral(std::shared_ptr<PrimitiveType> type);

  /// Create a literal representing a value above the maximum for the given type.
  static Literal AboveMaxLiteral(std::shared_ptr<PrimitiveType> type);

 private:
  /// Cast from Int type to target type.
  static Result<Literal> CastFromInt(const Literal& literal,
                                     const std::shared_ptr<PrimitiveType>& target_type);

  /// Cast from Long type to target type.
  static Result<Literal> CastFromLong(const Literal& literal,
                                      const std::shared_ptr<PrimitiveType>& target_type);

  /// Cast from Float type to target type.
  static Result<Literal> CastFromFloat(const Literal& literal,
                                       const std::shared_ptr<PrimitiveType>& target_type);
};

Literal LiteralCaster::BelowMinLiteral(std::shared_ptr<PrimitiveType> type) {
  return Literal(Literal::BelowMin{}, std::move(type));
}

Literal LiteralCaster::AboveMaxLiteral(std::shared_ptr<PrimitiveType> type) {
  return Literal(Literal::AboveMax{}, std::move(type));
}

Result<Literal> LiteralCaster::CastFromInt(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto int_val = std::get<int32_t>(literal.value_);
  auto target_type_id = target_type->type_id();

  switch (target_type_id) {
    case TypeId::kLong:
      return Literal::Long(static_cast<int64_t>(int_val));
    case TypeId::kFloat:
      return Literal::Float(static_cast<float>(int_val));
    case TypeId::kDouble:
      return Literal::Double(static_cast<double>(int_val));
    default:
      return NotSupported("Cast from Int to {} is not implemented",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromLong(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto long_val = std::get<int64_t>(literal.value_);
  auto target_type_id = target_type->type_id();

  switch (target_type_id) {
    case TypeId::kInt: {
      // Check for overflow
      if (long_val >= std::numeric_limits<int32_t>::max()) {
        return AboveMaxLiteral(target_type);
      }
      if (long_val <= std::numeric_limits<int32_t>::min()) {
        return BelowMinLiteral(target_type);
      }
      return Literal::Int(static_cast<int32_t>(long_val));
    }
    case TypeId::kFloat:
      return Literal::Float(static_cast<float>(long_val));
    case TypeId::kDouble:
      return Literal::Double(static_cast<double>(long_val));
    default:
      return NotSupported("Cast from Long to {} is not supported",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromFloat(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto float_val = std::get<float>(literal.value_);
  auto target_type_id = target_type->type_id();

  switch (target_type_id) {
    case TypeId::kDouble:
      return Literal::Double(static_cast<double>(float_val));
    default:
      return NotSupported("Cast from Float to {} is not supported",
                          target_type->ToString());
  }
}

// Constructor
Literal::Literal(Value value, std::shared_ptr<PrimitiveType> type)
    : value_(std::move(value)), type_(std::move(type)) {}

// Factory methods
Literal Literal::Boolean(bool value) { return {Value{value}, boolean()}; }

Literal Literal::Int(int32_t value) { return {Value{value}, int32()}; }

Literal Literal::Date(int32_t value) { return {Value{value}, date()}; }

Literal Literal::Long(int64_t value) { return {Value{value}, int64()}; }

Literal Literal::Time(int64_t value) { return {Value{value}, time()}; }

Literal Literal::Timestamp(int64_t value) { return {Value{value}, timestamp()}; }

Literal Literal::TimestampTz(int64_t value) { return {Value{value}, timestamp_tz()}; }

Literal Literal::Float(float value) { return {Value{value}, float32()}; }

Literal Literal::Double(double value) { return {Value{value}, float64()}; }

Literal Literal::String(std::string value) { return {Value{std::move(value)}, string()}; }

Literal Literal::Binary(std::vector<uint8_t> value) {
  return {Value{std::move(value)}, binary()};
}

Literal Literal::Fixed(std::vector<uint8_t> value) {
  auto length = static_cast<int32_t>(value.size());
  return {Value{std::move(value)}, fixed(length)};
}

Result<Literal> Literal::Deserialize(std::span<const uint8_t> data,
                                     std::shared_ptr<PrimitiveType> type) {
  return Conversions::FromBytes(std::move(type), data);
}

Result<std::vector<uint8_t>> Literal::Serialize() const {
  return Conversions::ToBytes(*this);
}

// Getters

const std::shared_ptr<PrimitiveType>& Literal::type() const { return type_; }

// Cast method
Result<Literal> Literal::CastTo(const std::shared_ptr<PrimitiveType>& target_type) const {
  return LiteralCaster::CastTo(*this, target_type);
}

// Template function for floating point comparison following Iceberg rules:
// -NaN < NaN, but all NaN values (qNaN, sNaN) are treated as equivalent within their sign
template <std::floating_point T>
std::strong_ordering CompareFloat(T lhs, T rhs) {
  // If both are NaN, check their signs
  bool all_nan = std::isnan(lhs) && std::isnan(rhs);
  if (!all_nan) {
    // If not both NaN, use strong ordering
    return std::strong_order(lhs, rhs);
  }
  // Same sign NaN values are equivalent (no qNaN vs sNaN distinction),
  // and -NAN < NAN.
  bool lhs_is_negative = std::signbit(lhs);
  bool rhs_is_negative = std::signbit(rhs);
  return lhs_is_negative <=> rhs_is_negative;
}

bool Literal::operator==(const Literal& other) const { return (*this <=> other) == 0; }

// Three-way comparison operator
std::partial_ordering Literal::operator<=>(const Literal& other) const {
  // If types are different, comparison is unordered
  if (*type_ != *other.type_) {
    return std::partial_ordering::unordered;
  }

  // If either value is AboveMax, BelowMin or null, comparison is unordered
  if (IsAboveMax() || IsBelowMin() || other.IsAboveMax() || other.IsBelowMin() ||
      IsNull() || other.IsNull()) {
    return std::partial_ordering::unordered;
  }

  // Same type comparison for normal values
  switch (type_->type_id()) {
    case TypeId::kBoolean: {
      auto this_val = std::get<bool>(value_);
      auto other_val = std::get<bool>(other.value_);
      if (this_val == other_val) return std::partial_ordering::equivalent;
      return this_val ? std::partial_ordering::greater : std::partial_ordering::less;
    }

    case TypeId::kInt:
    case TypeId::kDate: {
      auto this_val = std::get<int32_t>(value_);
      auto other_val = std::get<int32_t>(other.value_);
      return this_val <=> other_val;
    }

    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      auto this_val = std::get<int64_t>(value_);
      auto other_val = std::get<int64_t>(other.value_);
      return this_val <=> other_val;
    }

    case TypeId::kFloat: {
      auto this_val = std::get<float>(value_);
      auto other_val = std::get<float>(other.value_);
      // Use strong_ordering for floating point as spec requests
      return CompareFloat(this_val, other_val);
    }

    case TypeId::kDouble: {
      auto this_val = std::get<double>(value_);
      auto other_val = std::get<double>(other.value_);
      // Use strong_ordering for floating point as spec requests
      return CompareFloat(this_val, other_val);
    }

    case TypeId::kString: {
      auto& this_val = std::get<std::string>(value_);
      auto& other_val = std::get<std::string>(other.value_);
      return this_val <=> other_val;
    }

    case TypeId::kBinary: {
      auto& this_val = std::get<std::vector<uint8_t>>(value_);
      auto& other_val = std::get<std::vector<uint8_t>>(other.value_);
      return this_val <=> other_val;
    }

    case TypeId::kFixed: {
      auto& this_val = std::get<std::vector<uint8_t>>(value_);
      auto& other_val = std::get<std::vector<uint8_t>>(other.value_);
      return this_val <=> other_val;
    }

    default:
      // For unsupported types, return unordered
      return std::partial_ordering::unordered;
  }
}

std::string Literal::ToString() const {
  if (std::holds_alternative<BelowMin>(value_)) {
    return "belowMin";
  }
  if (std::holds_alternative<AboveMax>(value_)) {
    return "aboveMax";
  }
  if (std::holds_alternative<std::monostate>(value_)) {
    return "null";
  }

  switch (type_->type_id()) {
    case TypeId::kBoolean: {
      return std::get<bool>(value_) ? "true" : "false";
    }
    case TypeId::kInt: {
      return std::to_string(std::get<int32_t>(value_));
    }
    case TypeId::kLong: {
      return std::to_string(std::get<int64_t>(value_));
    }
    case TypeId::kFloat: {
      return std::to_string(std::get<float>(value_));
    }
    case TypeId::kDouble: {
      return std::to_string(std::get<double>(value_));
    }
    case TypeId::kString: {
      return std::get<std::string>(value_);
    }
    case TypeId::kBinary: {
      const auto& binary_data = std::get<std::vector<uint8_t>>(value_);
      std::string result;
      result.reserve(binary_data.size() * 2);  // 2 chars per byte
      for (const auto& byte : binary_data) {
        std::format_to(std::back_inserter(result), "{:02X}", byte);
      }
      return result;
    }
    case TypeId::kFixed: {
      const auto& fixed_data = std::get<std::vector<uint8_t>>(value_);
      std::string result;
      result.reserve(fixed_data.size() * 2);  // 2 chars per byte
      for (const auto& byte : fixed_data) {
        std::format_to(std::back_inserter(result), "{:02X}", byte);
      }
      return result;
    }
    case TypeId::kDecimal:
    case TypeId::kUuid:
    case TypeId::kDate:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      throw IcebergError("Not implemented: ToString for " + type_->ToString());
    }
    default: {
      throw IcebergError("Unknown type: " + type_->ToString());
    }
  }
}

bool Literal::IsBelowMin() const { return std::holds_alternative<BelowMin>(value_); }

bool Literal::IsAboveMax() const { return std::holds_alternative<AboveMax>(value_); }

bool Literal::IsNull() const { return std::holds_alternative<std::monostate>(value_); }

// LiteralCaster implementation

Result<Literal> LiteralCaster::CastTo(const Literal& literal,
                                      const std::shared_ptr<PrimitiveType>& target_type) {
  if (*literal.type_ == *target_type) {
    // If types are the same, return a copy of the current literal
    return Literal(literal.value_, target_type);
  }

  // Handle special values
  if (std::holds_alternative<Literal::BelowMin>(literal.value_) ||
      std::holds_alternative<Literal::AboveMax>(literal.value_) ||
      std::holds_alternative<std::monostate>(literal.value_)) {
    // Cannot cast type for special values
    return NotSupported("Cannot cast type for {}", literal.ToString());
  }

  auto source_type_id = literal.type_->type_id();

  // Delegate to specific cast functions based on source type
  switch (source_type_id) {
    case TypeId::kInt:
      return CastFromInt(literal, target_type);
    case TypeId::kLong:
      return CastFromLong(literal, target_type);
    case TypeId::kFloat:
      return CastFromFloat(literal, target_type);
    case TypeId::kDouble:
    case TypeId::kBoolean:
    case TypeId::kString:
    case TypeId::kBinary:
      break;
    default:
      break;
  }

  return NotSupported("Cast from {} to {} is not implemented", literal.type_->ToString(),
                      target_type->ToString());
}

}  // namespace iceberg
