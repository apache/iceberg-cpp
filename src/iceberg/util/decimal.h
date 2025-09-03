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

/// \file iceberg/util/decimal.h
/// \brief 128-bit fixed-point decimal numbers.
/// Adapted from Apache Arrow with only Decimal128 support.
/// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/decimal.h

#include <array>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <string_view>
#include <type_traits>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/formattable.h"
#include "iceberg/util/int128.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief Represents 128-bit fixed-point decimal numbers.
/// The max decimal precision that can be safely represented is
/// 38 significant digits.
class ICEBERG_EXPORT Decimal : public util::Formattable {
 public:
  static constexpr int32_t kBitWidth = 128;
  static constexpr int32_t kByteWidth = kBitWidth / 8;
  static constexpr int32_t kMaxPrecision = 38;
  static constexpr int32_t kMaxScale = 38;

  /// \brief Default constructor initializes to zero.
  constexpr Decimal() noexcept = default;

  /// \brief Create a Decimal from a 128-bit integer.
  constexpr Decimal(int128_t value) noexcept  // NOLINT(google-explicit-constructor)
      : data_(value) {}

  /// \brief Create a Decimal from any integer not wider than 64 bits.
  template <typename T>
    requires(std::is_integral_v<T> && (sizeof(T) <= sizeof(uint64_t)))
  constexpr Decimal(T value) noexcept  // NOLINT(google-explicit-constructor)
  {
    data_ = static_cast<int128_t>(value);
  }

  /// \brief Parse a Decimal from a string representation.
  explicit Decimal(std::string_view str);

  /// \brief Create a Decimal from two 64-bit integers.
  constexpr Decimal(int64_t high, uint64_t low) noexcept {
    data_ = (static_cast<int128_t>(high) << 64) | low;
  }

  /// \brief Negate the current Decimal value (in place)
  Decimal& Negate();

  /// \brief Absolute value of the current Decimal value (in place)
  Decimal& Abs();

  /// \brief Absolute value of the current Decimal value
  static Decimal Abs(const Decimal& value);

  /// \brief Add a number to this one. The result is truncated to 128 bits.
  Decimal& operator+=(const Decimal& other);

  /// \brief Subtract a number from this one. The result is truncated to 128 bits.
  Decimal& operator-=(const Decimal& other);

  /// \brief Multiply this number by another. The result is truncated to 128 bits.
  Decimal& operator*=(const Decimal& other);

  /// \brief Divide this number by another.
  ///
  /// The operation does not modify the current Decimal value.
  /// The answer rounds towards zero. Signs work like:
  ///   21 /  5 ->  4,  1
  ///  -21 /  5 -> -4, -1
  ///   21 / -5 -> -4,  1
  ///  -21 / -5 ->  4, -1
  /// \param[in] divisor the number to divide by
  /// \return the pair of the quotient and the remainder
  Result<std::pair<Decimal, Decimal>> Divide(const Decimal& divisor) const;

  /// \brief In place division.
  Decimal& operator/=(const Decimal& other);

  /// \brief Bitwise OR operation.
  Decimal& operator|=(const Decimal& other);

  /// \brief Bitwise AND operation.
  Decimal& operator&=(const Decimal& other);

  /// \brief Shift left by the given number of bits (in place).
  Decimal& operator<<=(uint32_t shift);

  /// \brief Shift left by the given number of bits.
  Decimal operator<<(uint32_t shift) const {
    Decimal result(*this);
    result <<= shift;
    return result;
  }

  /// \brief Shift right by the given number of bits (in place).
  Decimal& operator>>=(uint32_t shift);

  /// \brief Shift right by the given number of bits.
  Decimal operator>>(uint32_t shift) const {
    Decimal result(*this);
    result >>= shift;
    return result;
  }

  /// \brief Get the underlying 128-bit integer representation of the number.
  constexpr int128_t value() const { return data_; }

  /// \brief Get the high bits of the two's complement representation of the number.
  constexpr int64_t high() const { return static_cast<int64_t>(data_ >> 64); }

  /// \brief Get the low bits of the two's complement representation of the number.
  constexpr uint64_t low() const { return static_cast<uint64_t>(data_); }

  /// \brief Convert the Decimal value to a base 10 decimal string with the given scale.
  /// \param scale The scale to use for the string representation.
  /// \return The string representation of the Decimal value.
  Result<std::string> ToString(int32_t scale = 0) const;

  /// \brief Convert the Decimal value to an integer string.
  std::string ToIntegerString() const;

  /// \brief Returns an integer string representation of the decimal value.
  std::string ToString() const override { return ToIntegerString(); }

  /// \brief Convert the decimal string to a Decimal value, optionally including precision
  /// and scale if they are provided not null.
  /// \param str The string representation of the Decimal value.
  /// \param[out] precision Optional pointer to store the precision of the parsed value.
  /// \param[out] scale Optional pointer to store the scale of the parsed value.
  /// \return The Decimal value.
  static Result<Decimal> FromString(std::string_view str, int32_t* precision = nullptr,
                                    int32_t* scale = nullptr);

  /// \brief Convert the floating-point value to a Decimal value with the given
  /// precision and scale.
  static Result<Decimal> FromReal(double real, int32_t precision, int32_t scale);
  static Result<Decimal> FromReal(float real, int32_t precision, int32_t scale);

  /// \brief Convert from a big-endian byte representation. The length must be
  ///        between 1 and 16.
  /// \return error status if the length is an invalid value
  static Result<Decimal> FromBigEndian(const uint8_t* data, int32_t length);

  /// \brief separate the integer and fractional parts for the given scale.
  Result<std::pair<Decimal, Decimal>> GetWholeAndFraction(int32_t scale) const;

  /// \brief Convert Decimal from one scale to another.
  Result<Decimal> Rescale(int32_t orig_scale, int32_t new_scale) const;

  /// \brief Whether this number fits in the given precision
  ///
  /// Returns true if the number of significant digits is less or equal to `precision`.
  bool FitsInPrecision(int32_t precision) const;

  /// \brief Spaceship operator for three-way comparison.
  std::strong_ordering operator<=>(const Decimal& other) const {
    if (high() != other.high()) {
      return high() <=> other.high();
    }
    return low() <=> other.low();
  }

  /// \brief Convert to a signed integer
  template <typename T>
    requires std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
  Result<T> ToInteger() const {
    constexpr auto min_value = std::numeric_limits<T>::min();
    constexpr auto max_value = std::numeric_limits<T>::max();
    const auto& self = *this;
    if (self < min_value || self > max_value) {
      return Invalid("Invalid cast from Decimal to {} byte integer", sizeof(T));
    }
    return static_cast<T>(low());
  }

  /// \brief Convert to a signed integer
  template <typename T>
    requires std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
  Status ToInteger(T* out) const {
    ICEBERG_ASSIGN_OR_RAISE(auto result, ToInteger<T>());
    *out = result;
    return {};
  }

  /// \brief Convert to a floating-point number (scaled)
  float ToFloat(int32_t scale) const;
  /// \brief Convert to a floating-point number (scaled)
  double ToDouble(int32_t scale) const;

  /// \brief Convert the Decimal value to a floating-point value with the given scale.
  /// \param scale The scale to use for the conversion.
  /// \return The floating-point value.
  template <typename T>
    requires std::is_floating_point_v<T>
  T ToReal(int32_t scale) const {
    if constexpr (std::is_same_v<T, float>) {
      return ToFloat(scale);
    } else {
      return ToDouble(scale);
    }
  }

  const uint8_t* native_endian_bytes() const {
    return reinterpret_cast<const uint8_t*>(&data_);
  }

  /// \brief Returns the raw bytes of the value in native-endian byte order.
  std::array<uint8_t, kByteWidth> ToBytes() const;

  /// \brief Returns 1 if positive or zero, -1 if strictly negative.
  int64_t Sign() const { return 1 | (high() >> 63); }

  /// \brief Check if the Decimal value is negative.
  bool IsNegative() const { return (high() >> 63) < 0; }

  explicit operator bool() const { return data_ != 0; }

  friend bool operator==(const Decimal& lhs, const Decimal& rhs) {
    return lhs.data_ == rhs.data_;
  }

  friend bool operator!=(const Decimal& lhs, const Decimal& rhs) {
    return lhs.data_ != rhs.data_;
  }

  friend Decimal operator-(const Decimal& operand);
  friend Decimal operator~(const Decimal& operand);

  friend Decimal operator+(const Decimal& lhs, const Decimal& rhs);
  friend Decimal operator-(const Decimal& lhs, const Decimal& rhs);
  friend Decimal operator*(const Decimal& lhs, const Decimal& rhs);
  friend Decimal operator/(const Decimal& lhs, const Decimal& rhs);
  friend Decimal operator%(const Decimal& lhs, const Decimal& rhs);

 private:
  int128_t data_{0};
};

ICEBERG_EXPORT std::ostream& operator<<(std::ostream& os, const Decimal& decimal);

}  // namespace iceberg
