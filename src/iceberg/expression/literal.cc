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
#include <cstdint>
#include <string>

#include "iceberg/util/checked_cast.h"
#include "iceberg/util/conversions.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/temporal_util.h"

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

  /// Cast from Double type to target type.
  static Result<Literal> CastFromDouble(
      const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type);

  /// Cast from String type to target type.
  static Result<Literal> CastFromString(
      const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type);

  /// Cast from Timestamp type to target type.
  static Result<Literal> CastFromTimestamp(
      const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type);

  /// Cast from TimestampTz type to target type.
  static Result<Literal> CastFromTimestampTz(
      const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type);

  /// Cast from Binary type to target type.
  static Result<Literal> CastFromBinary(
      const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type);

  /// Cast from Fixed type to target type.
  static Result<Literal> CastFromFixed(const Literal& literal,
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
    case TypeId::kDate:
      return Literal::Date(int_val);
    default:
      return NotSupported("Cast from Int to {} is not implemented",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromLong(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto long_val = std::get<int64_t>(literal.value_);

  switch (target_type->type_id()) {
    case TypeId::kInt: {
      // Check for overflow
      if (long_val > std::numeric_limits<int32_t>::max()) {
        return AboveMaxLiteral(target_type);
      }
      if (long_val < std::numeric_limits<int32_t>::min()) {
        return BelowMinLiteral(target_type);
      }
      return Literal::Int(static_cast<int32_t>(long_val));
    }
    case TypeId::kFloat:
      return Literal::Float(static_cast<float>(long_val));
    case TypeId::kDouble:
      return Literal::Double(static_cast<double>(long_val));
    case TypeId::kDate: {
      if (long_val > std::numeric_limits<int32_t>::max()) {
        return AboveMaxLiteral(target_type);
      }
      if (long_val < std::numeric_limits<int32_t>::min()) {
        return BelowMinLiteral(target_type);
      }
      return Literal::Date(static_cast<int32_t>(long_val));
    }
    case TypeId::kTime:
      return Literal::Time(long_val);
    case TypeId::kTimestamp:
      return Literal::Timestamp(long_val);
    case TypeId::kTimestampTz:
      return Literal::TimestampTz(long_val);
    default:
      return NotSupported("Cast from Long to {} is not supported",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromFloat(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto float_val = std::get<float>(literal.value_);

  switch (target_type->type_id()) {
    case TypeId::kDouble:
      return Literal::Double(static_cast<double>(float_val));
    default:
      return NotSupported("Cast from Float to {} is not supported",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromDouble(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto double_val = std::get<double>(literal.value_);

  switch (target_type->type_id()) {
    case TypeId::kFloat: {
      if (double_val > static_cast<double>(std::numeric_limits<float>::max())) {
        return AboveMaxLiteral(target_type);
      }
      if (double_val < static_cast<double>(std::numeric_limits<float>::lowest())) {
        return BelowMinLiteral(target_type);
      }
      return Literal::Float(static_cast<float>(double_val));
    }
    default:
      return NotSupported("Cast from Double to {} is not supported",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromString(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  const auto& str_val = std::get<std::string>(literal.value_);

  switch (target_type->type_id()) {
    case TypeId::kUuid: {
      ICEBERG_ASSIGN_OR_RAISE(auto uuid, Uuid::FromString(str_val));
      return Literal::UUID(uuid);
    }
    case TypeId::kDate:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      return NotImplemented("Cast from String to {} is not implemented yet",
                            target_type->ToString());
    default:
      return NotSupported("Cast from String to {} is not supported",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromTimestamp(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto timestamp_val = std::get<int64_t>(literal.value_);

  switch (target_type->type_id()) {
    case TypeId::kDate: {
      ICEBERG_ASSIGN_OR_RAISE(auto days, TemporalUtils::ExtractDay(literal));
      return Literal::Date(std::get<int32_t>(days.value()));
    }
    case TypeId::kTimestampTz:
      return Literal::TimestampTz(timestamp_val);
    default:
      return NotSupported("Cast from Timestamp to {} is not supported",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromTimestampTz(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto micros = std::get<int64_t>(literal.value_);

  switch (target_type->type_id()) {
    case TypeId::kDate: {
      ICEBERG_ASSIGN_OR_RAISE(auto days, TemporalUtils::ExtractDay(literal));
      return Literal::Date(std::get<int32_t>(days.value()));
    }
    case TypeId::kTimestamp:
      return Literal::Timestamp(micros);
    default:
      return NotSupported("Cast from TimestampTz to {} is not supported",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromBinary(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  auto binary_val = std::get<std::vector<uint8_t>>(literal.value_);
  switch (target_type->type_id()) {
    case TypeId::kFixed: {
      auto target_fixed_type = internal::checked_pointer_cast<FixedType>(target_type);
      if (binary_val.size() == target_fixed_type->length()) {
        return Literal::Fixed(std::move(binary_val));
      }
      return InvalidArgument("Failed to cast Binary with length {} to Fixed({})",
                             binary_val.size(), target_fixed_type->length());
    }
    default:
      return NotSupported("Cast from Binary to {} is not supported",
                          target_type->ToString());
  }
}

Result<Literal> LiteralCaster::CastFromFixed(
    const Literal& literal, const std::shared_ptr<PrimitiveType>& target_type) {
  switch (target_type->type_id()) {
    case TypeId::kBinary:
      return Literal::Binary(std::get<std::vector<uint8_t>>(literal.value_));
    default:
      return NotSupported("Cast from Fixed to {} is not supported",
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

Literal Literal::UUID(Uuid value) { return {Value{std::move(value)}, uuid()}; }

Literal Literal::Binary(std::vector<uint8_t> value) {
  return {Value{std::move(value)}, binary()};
}

Literal Literal::Fixed(std::vector<uint8_t> value) {
  const auto size = value.size();
  return {Value{std::move(value)}, fixed(size)};
}

Literal Literal::Decimal(int128_t value, int32_t precision, int32_t scale) {
  return {Value{::iceberg::Decimal(value)}, decimal(precision, scale)};
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
  if (type_->type_id() != other.type_->type_id()) {
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

    case TypeId::kDecimal: {
      auto& this_val = std::get<::iceberg::Decimal>(value_);
      auto& other_val = std::get<::iceberg::Decimal>(other.value_);
      const auto& this_decimal_type = internal::checked_cast<DecimalType&>(*type_);
      const auto& other_decimal_type = internal::checked_cast<DecimalType&>(*other.type_);
      return ::iceberg::Decimal::Compare(this_val, other_val, this_decimal_type.scale(),
                                         other_decimal_type.scale());
    }

    case TypeId::kString: {
      auto& this_val = std::get<std::string>(value_);
      auto& other_val = std::get<std::string>(other.value_);
      return this_val <=> other_val;
    }

    case TypeId::kUuid: {
      auto& this_val = std::get<Uuid>(value_);
      auto& other_val = std::get<Uuid>(other.value_);
      if (this_val == other_val) {
        return std::partial_ordering::equivalent;
      }
      return std::partial_ordering::unordered;
    }

    case TypeId::kBinary:
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
    case TypeId::kDecimal: {
      const auto& decimal_type = internal::checked_cast<DecimalType&>(*type_);
      const auto& decimal = std::get<::iceberg::Decimal>(value_);
      return decimal.ToString(decimal_type.scale())
          .value_or("invalid literal of type decimal");
    }
    case TypeId::kString: {
      return "\"" + std::get<std::string>(value_) + "\"";
    }
    case TypeId::kUuid: {
      return std::get<Uuid>(value_).ToString();
    }
    case TypeId::kBinary:
    case TypeId::kFixed: {
      const auto& binary_data = std::get<std::vector<uint8_t>>(value_);
      std::string result = "X'";
      result.reserve(/*prefix*/ 2 + /*suffix*/ 1 + /*data*/ binary_data.size() * 2);
      for (const auto& byte : binary_data) {
        std::format_to(std::back_inserter(result), "{:02X}", byte);
      }
      result.push_back('\'');
      return result;
    }
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      return std::to_string(std::get<int64_t>(value_));
    }
    case TypeId::kDate: {
      return std::to_string(std::get<int32_t>(value_));
    }
    default: {
      return std::format("invalid literal of type {}", type_->ToString());
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
    case TypeId::kBoolean:
      // No casts defined for Boolean, other than to itself.
      break;
    case TypeId::kInt:
      return CastFromInt(literal, target_type);
    case TypeId::kLong:
      return CastFromLong(literal, target_type);
    case TypeId::kFloat:
      return CastFromFloat(literal, target_type);
    case TypeId::kDouble:
      return CastFromDouble(literal, target_type);
    case TypeId::kString:
      return CastFromString(literal, target_type);
    case TypeId::kBinary:
      return CastFromBinary(literal, target_type);
    case TypeId::kFixed:
      return CastFromFixed(literal, target_type);
    case TypeId::kTimestamp:
      return CastFromTimestamp(literal, target_type);
    case TypeId::kTimestampTz:
      return CastFromTimestampTz(literal, target_type);
    default:
      break;
  }

  return NotSupported("Cast from {} to {} is not supported", literal.type_->ToString(),
                      target_type->ToString());
}

// LiteralValueHash implementation
std::size_t LiteralValueHash::operator()(const Literal::Value& value) const noexcept {
  return std::visit(
      [](const auto& v) -> std::size_t {
        using T = std::decay_t<decltype(v)>;

        constexpr size_t kHashPrime = 0x9e3779b9;

        if constexpr (std::is_same_v<T, std::monostate>) {
          return 0;
        } else if constexpr (std::is_same_v<T, Literal::BelowMin>) {
          return std::numeric_limits<std::size_t>::min();
        } else if constexpr (std::is_same_v<T, Literal::AboveMax>) {
          return std::numeric_limits<std::size_t>::max();
        } else if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, int32_t> ||
                             std::is_same_v<T, int64_t> || std::is_same_v<T, float> ||
                             std::is_same_v<T, double> ||
                             std::is_same_v<T, std::string>) {
          return std::hash<T>{}(v);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
          std::size_t hash = 0;
          for (size_t i = 0; i < v.size(); ++i) {
            hash ^= std::hash<uint8_t>{}(v[i]) + kHashPrime + (hash << 6) + (hash >> 2);
          }
          return hash;
        } else if constexpr (std::is_same_v<T, Decimal>) {
          const int128_t& val = v.value();
          std::size_t hash = std::hash<uint64_t>{}(static_cast<uint64_t>(val >> 64));
          hash ^= std::hash<uint64_t>{}(static_cast<uint64_t>(val)) + kHashPrime +
                  (hash << 6) + (hash >> 2);
          return hash;
        } else if constexpr (std::is_same_v<T, Uuid>) {
          std::size_t hash = 0;
          const auto& bytes = v.bytes();
          for (size_t i = 0; i < bytes.size(); ++i) {
            hash ^=
                std::hash<uint8_t>{}(bytes[i]) + kHashPrime + (hash << 6) + (hash >> 2);
          }
          return hash;
        } else {
          static_assert(sizeof(T) == 0, "Unhandled variant type in LiteralValueHash");
          return 0;
        }
      },
      value);
}

}  // namespace iceberg
