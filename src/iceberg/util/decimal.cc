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

/// \file iceberg/util/decimal.cc
/// \brief 128-bit fixed-point decimal numbers.
/// Adapted from Apache Arrow with only Decimal128 support.
/// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/decimal.cc

#include "iceberg/util/decimal.h"

#include <algorithm>
#include <bit>
#include <charconv>
#include <climits>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <utility>

#include "iceberg/exception.h"
#include "iceberg/result.h"
#include "iceberg/util/int128.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

constexpr int32_t kMinDecimalBytes = 1;
constexpr int32_t kMaxDecimalBytes = 16;

// The maximum decimal value that can be represented with kMaxPrecision digits.
// 10^38 - 1
constexpr Decimal kMaxDecimalValue(5421010862427522170LL, 687399551400673279ULL);
// The mininum decimal value that can be represented with kMaxPrecision digits.
// - (10^38 - 1)
constexpr Decimal kMinDecimalValue(-5421010862427522171LL, 17759344522308878337ULL);

struct DecimalComponents {
  std::string_view while_digits;
  std::string_view fractional_digits;
  int32_t exponent{0};
  char sign{0};
  bool has_exponent{false};
};

inline bool IsSign(char c) { return c == '+' || c == '-'; }

inline bool IsDigit(char c) { return c >= '0' && c <= '9'; }

inline bool IsDot(char c) { return c == '.'; }

inline bool StartsExponent(char c) { return c == 'e' || c == 'E'; }

inline size_t ParseDigitsRun(std::string_view str, size_t pos, std::string_view* out) {
  size_t start = pos;
  while (pos < str.size() && IsDigit(str[pos])) {
    ++pos;
  }
  *out = str.substr(start, pos - start);
  return pos;
}

bool ParseDecimalComponents(std::string_view str, DecimalComponents* out) {
  size_t pos = 0;

  if (str.empty()) {
    return false;
  }

  // Sign of the number
  if (IsSign(str[pos])) {
    out->sign = str[pos++];
  }
  // First run of digits
  pos = ParseDigitsRun(str, pos, &out->while_digits);
  if (pos == str.size()) {
    return !out->while_digits.empty();
  }

  // Optional dot
  if (IsDot(str[pos])) {
    // Second run of digits after the dot
    pos = ParseDigitsRun(str, ++pos, &out->fractional_digits);
  }
  if (out->fractional_digits.empty() && out->while_digits.empty()) {
    // Need at least some digits (whole or fractional)
    return false;
  }
  if (pos == str.size()) {
    return true;
  }

  // Optional exponent part
  if (StartsExponent(str[pos])) {
    ++pos;
    // Skip '+' sign, '-' sign will be handled by from_chars
    if (pos < str.size() && str[pos] == '+') {
      ++pos;
    }
    out->has_exponent = true;
    auto [ptr, ec] =
        std::from_chars(str.data() + pos, str.data() + str.size(), out->exponent);
    if (ec != std::errc()) {
      return false;  // Failed to parse exponent
    }
    pos = ptr - str.data();
  }

  return pos == str.size();
}

constexpr auto kInt64DecimalDigits =
    static_cast<size_t>(std::numeric_limits<int64_t>::digits10);

constexpr std::array<uint64_t, kInt64DecimalDigits + 1> kUInt64PowersOfTen = {
    // clang-format off
    1ULL,
    10ULL,
    100ULL,
    1000ULL,
    10000ULL,
    100000ULL,
    1000000ULL,
    10000000ULL,
    100000000ULL,
    1000000000ULL,
    10000000000ULL,
    100000000000ULL,
    1000000000000ULL,
    10000000000000ULL,
    100000000000000ULL,
    1000000000000000ULL,
    10000000000000000ULL,
    100000000000000000ULL,
    1000000000000000000ULL
    // clang-format on
};

/// \brief Powers of ten for Decimal with scale from 0 to 38.
constexpr std::array<Decimal, Decimal::kMaxScale + 1> kDecimal128PowersOfTen = {
    Decimal(1LL),
    Decimal(10LL),
    Decimal(100LL),
    Decimal(1000LL),
    Decimal(10000LL),
    Decimal(100000LL),
    Decimal(1000000LL),
    Decimal(10000000LL),
    Decimal(100000000LL),
    Decimal(1000000000LL),
    Decimal(10000000000LL),
    Decimal(100000000000LL),
    Decimal(1000000000000LL),
    Decimal(10000000000000LL),
    Decimal(100000000000000LL),
    Decimal(1000000000000000LL),
    Decimal(10000000000000000LL),
    Decimal(100000000000000000LL),
    Decimal(1000000000000000000LL),
    Decimal(0LL, 10000000000000000000ULL),
    Decimal(5LL, 7766279631452241920ULL),
    Decimal(54LL, 3875820019684212736ULL),
    Decimal(542LL, 1864712049423024128ULL),
    Decimal(5421LL, 200376420520689664ULL),
    Decimal(54210LL, 2003764205206896640ULL),
    Decimal(542101LL, 1590897978359414784ULL),
    Decimal(5421010LL, 15908979783594147840ULL),
    Decimal(54210108LL, 11515845246265065472ULL),
    Decimal(542101086LL, 4477988020393345024ULL),
    Decimal(5421010862LL, 7886392056514347008ULL),
    Decimal(54210108624LL, 5076944270305263616ULL),
    Decimal(542101086242LL, 13875954555633532928ULL),
    Decimal(5421010862427LL, 9632337040368467968ULL),
    Decimal(54210108624275LL, 4089650035136921600ULL),
    Decimal(542101086242752LL, 4003012203950112768ULL),
    Decimal(5421010862427522LL, 3136633892082024448ULL),
    Decimal(54210108624275221LL, 12919594847110692864ULL),
    Decimal(542101086242752217LL, 68739955140067328ULL),
    Decimal(5421010862427522170LL, 687399551400673280ULL)};

inline void ShiftAndAdd(std::string_view input, uint128_t& out) {
  for (size_t pos = 0; pos < input.size();) {
    const size_t group_size = std::min(kInt64DecimalDigits, input.size() - pos);
    const uint64_t multiple = kUInt64PowersOfTen[group_size];
    uint64_t value = 0;

    auto [_, ec] =
        std::from_chars(input.data() + pos, input.data() + pos + group_size, value);
    ICEBERG_DCHECK(ec == std::errc(), "Failed to parse digits in ShiftAndAdd");

    out = out * multiple + value;
    pos += group_size;
  }
}

void AdjustIntegerStringWithScale(int32_t scale, std::string* str) {
  if (scale == 0) {
    return;
  }
  ICEBERG_DCHECK(str != nullptr && !str->empty(), "str must not be null or empty");
  const bool is_negative = str->front() == '-';
  const auto is_negative_offset = static_cast<int32_t>(is_negative);
  const auto len = static_cast<int32_t>(str->size());
  const int32_t num_digits = len - is_negative_offset;
  const int32_t adjusted_exponent = num_digits - 1 - scale;

  // Note that the -6 is taken from the Java BigDecimal documentation.
  if (scale < 0 || adjusted_exponent < -6) {
    // Example 1:
    // Precondition: *str = "123", is_negative_offset = 0, num_digits = 3, scale = -2,
    //               adjusted_exponent = 4
    // After inserting decimal point: *str = "1.23"
    // After appending exponent: *str = "1.23E+4"
    // Example 2:
    // Precondition: *str = "-123", is_negative_offset = 1, num_digits = 3, scale = 9,
    //               adjusted_exponent = -7
    // After inserting decimal point: *str = "-1.23"
    // After appending exponent: *str = "-1.23E-7"
    // Example 3:
    // Precondition: *str = "0", is_negative_offset = 0, num_digits = 1, scale = -1,
    //               adjusted_exponent = 1
    // After inserting decimal point: *str = "0" // Not inserted
    // After appending exponent: *str = "0E+1"
    if (num_digits > 1) {
      str->insert(str->begin() + 1 + is_negative_offset, '.');
    }
    str->push_back('E');
    if (adjusted_exponent >= 0) {
      str->push_back('+');
    }
    // Append the adjusted exponent as a string.
    str->append(std::to_string(adjusted_exponent));
    return;
  }

  if (num_digits > scale) {
    const auto n = static_cast<size_t>(len - scale);
    // Example 1:
    // Precondition: *str = "123", len = num_digits = 3, scale = 1, n = 2
    // After inserting decimal point: *str = "12.3"
    // Example 2:
    // Precondition: *str = "-123", len = 4, num_digits = 3, scale = 1, n = 3
    // After inserting decimal point: *str = "-12.3"
    str->insert(str->begin() + n, '.');
    return;
  }

  // Example 1:
  // Precondition: *str = "123", is_negative_offset = 0, num_digits = 3, scale = 4
  // After insert: *str = "000123"
  // After setting decimal point: *str = "0.0123"
  // Example 2:
  // Precondition: *str = "-123", is_negative_offset = 1, num_digits = 3, scale = 4
  // After insert: *str = "-000123"
  // After setting decimal point: *str = "-0.0123"
  str->insert(is_negative_offset, scale - num_digits + 2, '0');
  str->at(is_negative_offset + 1) = '.';
}

bool RescaleWouldCauseDataLoss(const Decimal& value, int32_t delta_scale,
                               const Decimal& multiplier, Decimal* result) {
  if (delta_scale < 0) {
    auto res = value.Divide(multiplier);
    ICEBERG_DCHECK(res, "Decimal::Divide failed");
    *result = res->first;
    return res->second != 0;
  }

  auto max_safe_value = kMaxDecimalValue / multiplier;
  auto min_safe_value = kMinDecimalValue / multiplier;
  if (value > max_safe_value || value < min_safe_value) {
    // Overflow would happen — treat as data loss
    return true;
  }

  *result = value * multiplier;
  return false;
}

}  // namespace

Decimal::Decimal(std::string_view str) {
  auto result = Decimal::FromString(str);
  ICEBERG_CHECK_OR_DIE(result, "Failed to parse Decimal from string: {}, error: {}", str,
                       result.error().message);
  *this = std::move(result.value());
}

Decimal& Decimal::Negate() {
  uint128_t u = ~static_cast<uint128_t>(data_) + 1;
  data_ = static_cast<int128_t>(u);
  return *this;
}

Decimal& Decimal::Abs() { return *this < 0 ? Negate() : *this; }

Decimal Decimal::Abs(const Decimal& value) {
  Decimal result(value);
  return result.Abs();
}

Decimal& Decimal::operator+=(const Decimal& other) {
  data_ += other.data_;
  return *this;
}

Decimal& Decimal::operator-=(const Decimal& other) {
  data_ -= other.data_;
  return *this;
}

Decimal& Decimal::operator*=(const Decimal& other) {
  data_ *= other.data_;
  return *this;
}

Result<std::pair<Decimal, Decimal>> Decimal::Divide(const Decimal& divisor) const {
  std::pair<Decimal, Decimal> result;
  if (divisor == 0) {
    return Invalid("Cannot divide by zero in Decimal::Divide");
  }
  return std::make_pair(*this / divisor, *this % divisor);
}

Decimal& Decimal::operator/=(const Decimal& other) {
  data_ /= other.data_;
  return *this;
}

Decimal& Decimal::operator|=(const Decimal& other) {
  data_ |= other.data_;
  return *this;
}

Decimal& Decimal::operator&=(const Decimal& other) {
  data_ &= other.data_;
  return *this;
}

Decimal& Decimal::operator<<=(uint32_t bits) {
  if (bits != 0) {
    data_ = static_cast<int128_t>(static_cast<uint128_t>(data_) << bits);
  }

  return *this;
}

Decimal& Decimal::operator>>=(uint32_t bits) {
  if (bits != 0) {
    data_ >>= bits;
  }

  return *this;
}

Result<std::string> Decimal::ToString(int32_t scale) const {
  if (scale < -kMaxScale || scale > kMaxScale) {
    return InvalidArgument(
        "Decimal::ToString: scale must be in the range [-{}, {}], was {}", kMaxScale,
        kMaxScale, scale);
  }
  std::string str(ToIntegerString());
  AdjustIntegerStringWithScale(scale, &str);
  return str;
}

std::string Decimal::ToIntegerString() const {
  if (data_ == 0) {
    return "0";
  }

  bool negative = data_ < 0;
  uint128_t uval =
      negative ? -static_cast<uint128_t>(data_) : static_cast<uint128_t>(data_);

  constexpr uint32_t k1e9 = 1000000000U;
  constexpr size_t kNumBits = 128;
  // Segments will contain the array split into groups that map to decimal digits, in
  // little endian order. Each segment will hold at most 9 decimal digits. For example, if
  // the input represents 9876543210123456789, then segments will be [123456789,
  // 876543210, 9].
  // The max number of segments needed = ceil(kNumBits * log(2) / log(1e9))
  // = ceil(kNumBits / 29.897352854) <= ceil(kNumBits / 29).
  std::array<uint32_t, (kNumBits + 28) / 29> segments;
  size_t num_segments = 0;

  while (uval > 0) {
    // Compute remainder = uval % 1e9 and uval = uval / 1e9.
    auto remainder = static_cast<uint32_t>(uval % k1e9);
    uval /= k1e9;
    segments[num_segments++] = remainder;
  }

  std::ostringstream oss;
  if (negative) {
    oss << '-';
  }

  // First segment is formatted as-is.
  oss << segments[num_segments - 1];

  // Remaining segments are formatted with leading zeros to fill 9 digits. e.g. 123 is
  // formatted as "000000123"
  for (size_t i = num_segments - 1; i-- > 0;) {
    oss << std::setw(9) << std::setfill('0') << segments[i];
  }

  return oss.str();
}

Result<Decimal> Decimal::FromString(std::string_view str, int32_t* precision,
                                    int32_t* scale) {
  if (str.empty()) {
    return InvalidArgument("Empty string is not a valid Decimal");
  }
  DecimalComponents dec;
  if (!ParseDecimalComponents(str, &dec)) {
    return InvalidArgument("Invalid decimal string '{}'", str);
  }

  // Count number of significant digits (without leading zeros)
  size_t first_non_zero = dec.while_digits.find_first_not_of('0');
  size_t significant_digits = dec.fractional_digits.size();
  if (first_non_zero != std::string_view::npos) {
    significant_digits += dec.while_digits.size() - first_non_zero;
  }

  auto parsed_precision = static_cast<int32_t>(significant_digits);

  int32_t parsed_scale = 0;
  if (dec.has_exponent) {
    auto adjusted_exponent = dec.exponent;
    parsed_scale = static_cast<int32_t>(dec.fractional_digits.size()) - adjusted_exponent;
  } else {
    parsed_scale = static_cast<int32_t>(dec.fractional_digits.size());
  }

  uint128_t value = 0;
  ShiftAndAdd(dec.while_digits, value);
  ShiftAndAdd(dec.fractional_digits, value);
  Decimal result(static_cast<int128_t>(value));

  if (dec.sign == '-') {
    result.Negate();
  }

  if (parsed_scale < 0) {
    // For the scale to 0, to avoid negative scales (due to compatibility issues with
    // external systems such as databases)
    if (parsed_scale < -kMaxScale) {
      return InvalidArgument("scale must be in the range [-{}, {}], was {}", kMaxScale,
                             kMaxScale, parsed_scale);
    }

    result *= kDecimal128PowersOfTen[-parsed_scale];
    parsed_precision -= parsed_scale;
    parsed_scale = 0;
  }

  if (precision != nullptr) {
    *precision = parsed_precision;
  }
  if (scale != nullptr) {
    *scale = parsed_scale;
  }

  return result;
}

Result<Decimal> Decimal::FromBigEndian(const uint8_t* bytes, int32_t length) {
  if (length < kMinDecimalBytes || length > kMaxDecimalBytes) {
    return InvalidArgument(
        "Decimal::FromBigEndian: length must be in the range [{}, {}], was {}",
        kMinDecimalBytes, kMaxDecimalBytes, length);
  }

  // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  // sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  uint128_t result = 0;
  std::memcpy(reinterpret_cast<uint8_t*>(&result) + kMaxDecimalBytes - length, bytes,
              length);

  if constexpr (std::endian::native == std::endian::little) {
    auto high = static_cast<uint64_t>(result >> 64);
    auto low = static_cast<uint64_t>(result);
    high = std::byteswap(high);
    low = std::byteswap(low);
    // also need to swap the two halves
    result = (static_cast<uint128_t>(low) << 64) | high;
  }

  if (is_negative && length < kMaxDecimalBytes) {
    // Sign extend the high bits
    result |= (static_cast<uint128_t>(-1) << (length * CHAR_BIT));
  }

  return Decimal(static_cast<int128_t>(result));
}

std::vector<uint8_t> Decimal::ToBigEndian() const {
  std::vector<uint8_t> bytes(kMaxDecimalBytes);

  auto uvalue = static_cast<uint128_t>(data_);
  std::memcpy(bytes.data(), &uvalue, kMaxDecimalBytes);

  if constexpr (std::endian::native == std::endian::little) {
    std::ranges::reverse(bytes);
  }

  auto is_negative = data_ < 0;
  int keep = kMaxDecimalBytes;
  for (int32_t i = 0; i < kMaxDecimalBytes - 1; ++i) {
    uint8_t byte = bytes[i];
    uint8_t next = bytes[i + 1];
    // For negative numbers, keep the leading 0xff byte if the next byte has its sign bit
    // unset. For positive numbers, keep the leading 0x00 byte if the next byte has its
    // sign bit set.
    if ((is_negative && byte == 0xff && (next & 0x80)) ||
        (!is_negative && byte == 0x00 && !(next & 0x80))) {
      --keep;
    } else {
      break;
    }
  }

  bytes.erase(bytes.begin(), bytes.begin() + (kMaxDecimalBytes - keep));
  return bytes;
}

Result<Decimal> Decimal::Rescale(int32_t orig_scale, int32_t new_scale) const {
  if (orig_scale == new_scale) {
    return *this;
  }

  const int32_t delta_scale = new_scale - orig_scale;
  const int32_t abs_delta_scale = std::abs(delta_scale);
  Decimal out;

  ICEBERG_DCHECK(abs_delta_scale <= kMaxScale, "");

  auto& multiplier = kDecimal128PowersOfTen[abs_delta_scale];

  if (RescaleWouldCauseDataLoss(*this, delta_scale, multiplier, &out)) [[unlikely]] {
    return Invalid("Rescale {} from {} to {} would cause data loss", ToIntegerString(),
                   orig_scale, new_scale);
  }

  return out;
}

bool Decimal::FitsInPrecision(int32_t precision) const {
  ICEBERG_DCHECK(precision >= 1 && precision <= kMaxPrecision, "");
  return Decimal::Abs(*this) < kDecimal128PowersOfTen[precision];
}

std::partial_ordering Decimal::Compare(const Decimal& lhs, const Decimal& rhs,
                                       int32_t lhs_scale, int32_t rhs_scale) {
  if (lhs_scale == rhs_scale || lhs.data_ == 0 || rhs.data_ == 0) {
    return lhs <=> rhs;
  }

  // If one is negative and the other is positive, the positive is greater.
  if (lhs.data_ < 0 && rhs.data_ > 0) {
    return std::partial_ordering::less;
  }
  if (lhs.data_ > 0 && rhs.data_ < 0) {
    return std::partial_ordering::greater;
  }

  // Both are negative
  bool negative = lhs.data_ < 0 && rhs.data_ < 0;

  const int32_t delta_scale = lhs_scale - rhs_scale;
  const int32_t abs_delta_scale = std::abs(delta_scale);

  ICEBERG_DCHECK(abs_delta_scale <= kMaxScale, "");

  const auto& multiplier = kDecimal128PowersOfTen[abs_delta_scale];

  Decimal adjusted_lhs;
  Decimal adjusted_rhs;

  if (delta_scale < 0) {
    // lhs_scale < rhs_scale
    if (RescaleWouldCauseDataLoss(lhs, -delta_scale, multiplier, &adjusted_lhs))
        [[unlikely]] {
      return negative ? std::partial_ordering::less : std::partial_ordering::greater;
    }
    adjusted_rhs = rhs;
  } else {
    // lhs_scale > rhs_scale
    if (RescaleWouldCauseDataLoss(rhs, delta_scale, multiplier, &adjusted_rhs))
        [[unlikely]] {
      return negative ? std::partial_ordering::greater : std::partial_ordering::less;
    }
    adjusted_lhs = lhs;
  }

  return adjusted_lhs <=> adjusted_rhs;
}

std::array<uint8_t, Decimal::kByteWidth> Decimal::ToBytes() const {
  std::array<uint8_t, kByteWidth> out{{0}};
  std::memcpy(out.data(), &data_, kByteWidth);
  return out;
}

std::ostream& operator<<(std::ostream& os, const Decimal& decimal) {
  os << decimal.ToIntegerString();
  return os;
}

// Unary operators
Decimal operator-(const Decimal& operand) {
  Decimal result(operand.data_);
  return result.Negate();
}

Decimal operator~(const Decimal& operand) { return {~operand.data_}; }

// Binary operators
Decimal operator+(const Decimal& lhs, const Decimal& rhs) {
  Decimal result(lhs);
  result += rhs;
  return result;
}

Decimal operator-(const Decimal& lhs, const Decimal& rhs) {
  Decimal result(lhs);
  result -= rhs;
  return result;
}

Decimal operator*(const Decimal& lhs, const Decimal& rhs) {
  Decimal result(lhs);
  result *= rhs;
  return result;
}

Decimal operator/(const Decimal& lhs, const Decimal& rhs) {
  return lhs.data_ / rhs.data_;
}

Decimal operator%(const Decimal& lhs, const Decimal& rhs) {
  return lhs.data_ % rhs.data_;
}

}  // namespace iceberg
