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

#include <algorithm>
#include <bit>
#include <cmath>
#include <concepts>
#include <cstring>

#include "iceberg/exception.h"

namespace iceberg {

namespace {
/// \brief Write a value in little-endian format to the buffer.
template <typename T>
void WriteLittleEndian(std::vector<uint8_t>& buffer, T value) {
  static_assert(std::is_trivially_copyable_v<T>, "Type must be trivially copyable");

  if constexpr (std::endian::native == std::endian::little) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
    buffer.insert(buffer.end(), bytes, bytes + sizeof(T));
  } else {
    if constexpr (sizeof(T) > 1) {
      T le_value = std::byteswap(value);
      const auto* bytes = reinterpret_cast<const uint8_t*>(&le_value);
      buffer.insert(buffer.end(), bytes, bytes + sizeof(T));
    } else {
      // For single byte types, no byteswap needed
      buffer.push_back(static_cast<uint8_t>(value));
    }
  }
}

/// \brief Read a value in little-endian format from the data.
template <typename T>
Result<T> ReadLittleEndian(std::span<const uint8_t> data) {
  static_assert(std::is_trivially_copyable_v<T>, "Type must be trivially copyable");

  if (data.size() < sizeof(T)) {
    return InvalidArgument("Insufficient data to read {} bytes, got {}", sizeof(T),
                           data.size());
  }

  T value;
  std::memcpy(&value, data.data(), sizeof(T));

  if constexpr (std::endian::native != std::endian::little && sizeof(T) > 1) {
    value = std::byteswap(value);
  }
  return value;
}

/// \brief Write a 16-byte value in big-endian format (for UUID and Decimal).
void WriteBigEndian16(std::vector<uint8_t>& buffer,
                      const std::array<uint8_t, 16>& value) {
  buffer.insert(buffer.end(), value.begin(), value.end());
}

/// \brief Read a 16-byte value in big-endian format (for UUID and Decimal).
Result<std::array<uint8_t, 16>> ReadBigEndian16(std::span<const uint8_t> data) {
  if (data.size() < 16) {
    return InvalidArgument("Insufficient data to read 16 bytes, got {}", data.size());
  }

  std::array<uint8_t, 16> result;
  std::copy(data.begin(), data.begin() + 16, result.begin());
  return result;
}

/// \brief Create a literal from int64 value and long-type TypeId.
Result<Literal> CreateLongTypeLiteral(int64_t value, TypeId type_id) {
  switch (type_id) {
    case TypeId::kLong:
      return Literal::Long(value);
    case TypeId::kTime:
      return Literal::Time(value);
    case TypeId::kTimestamp:
      return Literal::Timestamp(value);
    case TypeId::kTimestampTz:
      return Literal::TimestampTz(value);
    default:
      return InvalidArgument("Unsupported long type: {}", static_cast<int>(type_id));
  }
}

/// \brief Get type name for long-compatible types.
constexpr const char* GetLongTypeName(TypeId type_id) {
  switch (type_id) {
    case TypeId::kLong:
      return "Long";
    case TypeId::kTime:
      return "Time";
    case TypeId::kTimestamp:
      return "Timestamp";
    case TypeId::kTimestampTz:
      return "TimestampTz";
    default:
      return "Unknown";
  }
}

}  // namespace

/// \brief LiteralSerializer handles serialization/deserialization operations for Literal.
/// This is an internal implementation class.
class LiteralSerializer {
 public:
  /// \brief Serialize a literal value to binary format.
  static Result<std::vector<uint8_t>> ToBytes(const Literal& literal);

  /// \brief Deserialize binary data to a literal value.
  static Result<Literal> FromBytes(std::span<const uint8_t> data,
                                   std::shared_ptr<PrimitiveType> type);
};

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

Result<Literal> Literal::Deserialize(std::span<const uint8_t> data,
                                     std::shared_ptr<PrimitiveType> type) {
  return LiteralSerializer::FromBytes(data, type);
}

Result<std::vector<uint8_t>> Literal::Serialize() const {
  return LiteralSerializer::ToBytes(*this);
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
    case TypeId::kDecimal:
    case TypeId::kUuid:
    case TypeId::kFixed:
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

// LiteralSerializer implementation

Result<std::vector<uint8_t>> LiteralSerializer::ToBytes(const Literal& literal) {
  // Cannot serialize special values
  if (literal.IsAboveMax()) {
    return InvalidArgument("Cannot serialize AboveMax literal");
  }
  if (literal.IsBelowMin()) {
    return InvalidArgument("Cannot serialize BelowMin literal");
  }

  std::vector<uint8_t> result;

  // Null values serialize to empty buffer
  if (literal.IsNull()) {
    return result;
  }

  const auto& value = literal.value();
  const auto type_id = literal.type()->type_id();

  switch (type_id) {
    case TypeId::kBoolean: {
      // 0x00 for false, 0x01 for true
      result.push_back(std::get<bool>(value) ? 0x01 : 0x00);
      return result;
    }

    case TypeId::kInt: {
      // Stored as 4-byte little-endian
      WriteLittleEndian(result, std::get<int32_t>(value));
      return result;
    }

    case TypeId::kDate: {
      // Stores days from 1970-01-01 in a 4-byte little-endian int
      WriteLittleEndian(result, std::get<int32_t>(value));
      return result;
    }

    case TypeId::kLong: {
      // Stored as 8-byte little-endian
      WriteLittleEndian(result, std::get<int64_t>(value));
      return result;
    }

    case TypeId::kTime: {
      // Stores microseconds from midnight in an 8-byte little-endian long
      WriteLittleEndian(result, std::get<int64_t>(value));
      return result;
    }

    case TypeId::kTimestamp: {
      // Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian
      // long
      WriteLittleEndian(result, std::get<int64_t>(value));
      return result;
    }

    case TypeId::kTimestampTz: {
      // Stores microseconds from 1970-01-01 00:00:00.000000 UTC in an 8-byte
      // little-endian long
      WriteLittleEndian(result, std::get<int64_t>(value));
      return result;
    }

    case TypeId::kFloat: {
      // Stored as 4-byte little-endian
      WriteLittleEndian(result, std::get<float>(value));
      return result;
    }

    case TypeId::kDouble: {
      // Stored as 8-byte little-endian
      WriteLittleEndian(result, std::get<double>(value));
      return result;
    }

    case TypeId::kString: {
      // UTF-8 bytes (without length)
      const auto& str = std::get<std::string>(value);
      result.insert(result.end(), str.begin(), str.end());
      return result;
    }

    case TypeId::kBinary: {
      // Binary value (without length)
      const auto& binary_data = std::get<std::vector<uint8_t>>(value);
      result = binary_data;
      return result;
    }

    case TypeId::kFixed: {
      // Fixed(L) - Binary value, could be stored in std::array<uint8_t, 16> or
      // std::vector<uint8_t>
      if (std::holds_alternative<std::array<uint8_t, 16>>(value)) {
        const auto& fixed_bytes = std::get<std::array<uint8_t, 16>>(value);
        result.insert(result.end(), fixed_bytes.begin(), fixed_bytes.end());
      } else if (std::holds_alternative<std::vector<uint8_t>>(value)) {
        result = std::get<std::vector<uint8_t>>(value);
      } else {
        return InvalidArgument("Invalid value type for Fixed literal");
      }
      return result;
    }

    case TypeId::kUuid: {
      // 16-byte big-endian value
      const auto& uuid_bytes = std::get<std::array<uint8_t, 16>>(value);
      WriteBigEndian16(result, uuid_bytes);
      return result;
    }

    case TypeId::kDecimal: {
      // Stores unscaled value as two's-complement big-endian binary
      const auto& decimal_bytes = std::get<std::array<uint8_t, 16>>(value);
      WriteBigEndian16(result, decimal_bytes);
      return result;
    }

    default:
      return NotImplemented("Serialization for type {} is not supported",
                            literal.type()->ToString());
  }
}

Result<Literal> LiteralSerializer::FromBytes(std::span<const uint8_t> data,
                                             std::shared_ptr<PrimitiveType> type) {
  if (!type) {
    return InvalidArgument("Type cannot be null");
  }

  // Empty data represents null value
  if (data.empty()) {
    return Literal::Null(type);
  }

  const auto type_id = type->type_id();

  switch (type_id) {
    case TypeId::kBoolean: {
      auto result = ReadLittleEndian<uint8_t>(data);
      if (!result) {
        return std::unexpected<Error>(result.error());
      }
      // 0x00 for false, non-zero byte for true
      return Literal::Boolean(*result != 0x00);
    }

    case TypeId::kInt: {
      auto result = ReadLittleEndian<int32_t>(data);
      if (!result) {
        return std::unexpected<Error>(result.error());
      }
      return Literal::Int(*result);
    }

    case TypeId::kDate: {
      auto result = ReadLittleEndian<int32_t>(data);
      if (!result) {
        return std::unexpected<Error>(result.error());
      }
      return Literal::Date(*result);
    }

    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      int64_t value;

      if (data.size() == 4) {
        // Type was promoted from int to long
        auto int_result = ReadLittleEndian<int32_t>(data);
        if (!int_result) {
          return std::unexpected<Error>(int_result.error());
        }
        value = static_cast<int64_t>(*int_result);
      } else if (data.size() == 8) {
        // Standard 8-byte long
        auto long_result = ReadLittleEndian<int64_t>(data);
        if (!long_result) {
          return std::unexpected<Error>(long_result.error());
        }
        value = *long_result;
      } else {
        return InvalidArgument("{} requires 4 or 8 bytes, got {}",
                               GetLongTypeName(type_id), data.size());
      }

      return CreateLongTypeLiteral(value, type_id);
    }

    case TypeId::kFloat: {
      auto result = ReadLittleEndian<float>(data);
      if (!result) {
        return std::unexpected<Error>(result.error());
      }
      return Literal::Float(*result);
    }

    case TypeId::kDouble: {
      if (data.size() == 4) {
        // Type was promoted from float to double
        auto float_result = ReadLittleEndian<float>(data);
        if (!float_result) {
          return std::unexpected<Error>(float_result.error());
        }
        return Literal::Double(static_cast<double>(*float_result));
      } else if (data.size() == 8) {
        // Standard 8-byte double
        auto double_result = ReadLittleEndian<double>(data);
        if (!double_result) {
          return std::unexpected<Error>(double_result.error());
        }
        return Literal::Double(*double_result);
      } else {
        return InvalidArgument("Double requires 4 or 8 bytes, got {}", data.size());
      }
    }

    case TypeId::kString: {
      return Literal::String(
          std::string(reinterpret_cast<const char*>(data.data()), data.size()));
    }

    case TypeId::kBinary: {
      return Literal::Binary(std::vector<uint8_t>(data.begin(), data.end()));
    }

    case TypeId::kFixed: {
      if (data.size() == 16) {
        std::array<uint8_t, 16> fixed_bytes;
        std::ranges::copy(data, fixed_bytes.begin());
        return Literal(Literal::Value{fixed_bytes}, type);
      } else {
        return Literal(Literal::Value{std::vector<uint8_t>(data.begin(), data.end())},
                       type);
      }
    }

    case TypeId::kUuid: {
      auto uuid_result = ReadBigEndian16(data);
      if (!uuid_result) {
        return std::unexpected<Error>(uuid_result.error());
      }
      return Literal(Literal::Value{*uuid_result}, type);
    }

    case TypeId::kDecimal: {
      if (data.size() > 16) {
        return InvalidArgument(
            "Decimal data too large, maximum 16 bytes supported, got {}", data.size());
      }

      std::array<uint8_t, 16> decimal_bytes{};
      // Copy data to the end of the array (big-endian format for decimals)
      // This handles variable-length decimals by right-aligning them
      std::ranges::copy(data, decimal_bytes.end() - data.size());
      return Literal(Literal::Value{decimal_bytes}, type);
    }

    default:
      return NotImplemented("Deserialization for type {} is not supported",
                            type->ToString());
  }

  // Should not reach here
  return InvalidArgument("Unexpected error in deserialization");
}

}  // namespace iceberg
