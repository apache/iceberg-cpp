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

#include "iceberg/expression/decimal.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <charconv>
#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <iomanip>
#include <iostream>
#include <limits>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

static constexpr uint64_t kInt64Mask = 0xFFFFFFFFFFFFFFFF;

// Convenience wrapper type over 128 bit unsigned integers. This class merely provides the
// minimum necessary set of functions to perform 128 bit multiplication operations.
struct Uint128 {
  Uint128() = default;
  Uint128(uint64_t high, uint64_t low)
      : val_((static_cast<uint128_t>(high) << 64) | low) {}

  explicit Uint128(uint64_t value) : val_(value) {}
  explicit Uint128(const Decimal& decimal)
      : val_((static_cast<uint128_t>(decimal.high()) << 64) | decimal.low()) {}

  uint64_t high() const { return static_cast<uint64_t>(val_ >> 64); }
  uint64_t low() const { return static_cast<uint64_t>(val_ & kInt64Mask); }

  Uint128& operator+=(const Uint128& other) {
    val_ += other.val_;
    return *this;
  }

  Uint128& operator*=(const Uint128& other) {
    val_ *= other.val_;
    return *this;
  }

  uint128_t val_{};
};

// Signed addition with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
  requires std::is_signed_v<SignedInt>
constexpr SignedInt SafeSignedAdd(SignedInt u, SignedInt v) {
  using UnsignedInt = std::make_unsigned_t<SignedInt>;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) +
                                static_cast<UnsignedInt>(v));
}

// Signed subtraction with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
  requires std::is_signed_v<SignedInt>
constexpr SignedInt SafeSignedSubtract(SignedInt u, SignedInt v) {
  using UnsignedInt = std::make_unsigned_t<SignedInt>;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) -
                                static_cast<UnsignedInt>(v));
}

// Signed negation with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
  requires std::is_signed_v<SignedInt>
constexpr SignedInt SafeSignedNegate(SignedInt u) {
  using UnsignedInt = std::make_unsigned_t<SignedInt>;
  return static_cast<SignedInt>(~static_cast<UnsignedInt>(u) + 1);
}

// Signed left shift with well-defined behaviour on negative numbers or overflow
template <typename SignedInt, typename Shift>
  requires std::is_signed_v<SignedInt> && std::is_integral_v<Shift>
constexpr SignedInt SafeLeftShift(SignedInt u, Shift shift) {
  using UnsignedInt = std::make_unsigned_t<SignedInt>;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) << shift);
}

/// \brief Expands the given value into a big endian array of ints so that we can work on
/// it. The array will be converted to an absolute value. The array will remove leading
/// zeros from the value.
/// \param array a big endian array of length 4 to set with the value
/// \result the output length of the array
static inline int64_t FillInArray(const Decimal& value, uint32_t* array) {
  Decimal abs_value = Decimal::Abs(value);
  auto high = static_cast<uint64_t>(abs_value.high());
  auto low = abs_value.low();

  if (high != 0) {
    if (high > std::numeric_limits<uint32_t>::max()) {
      array[0] = static_cast<uint32_t>(high >> 32);
      array[1] = static_cast<uint32_t>(high);
      array[2] = static_cast<uint32_t>(low >> 32);
      array[3] = static_cast<uint32_t>(low);
      return 4;
    }

    array[0] = static_cast<uint32_t>(high);
    array[1] = static_cast<uint32_t>(low >> 32);
    array[2] = static_cast<uint32_t>(low);
    return 3;
  }

  if (low > std::numeric_limits<uint32_t>::max()) {
    array[0] = static_cast<uint32_t>(low >> 32);
    array[1] = static_cast<uint32_t>(low);
    return 2;
  }

  if (low == 0) {
    return 0;
  }

  array[0] = static_cast<uint32_t>(low);
  return 1;
}

/// \brief Shift the number in the array left by bits positions.
static inline void ShiftArrayLeft(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits > 0) {
    for (int32_t i = 0; i < length - 1; ++i) {
      array[i] = (array[i] << bits) | (array[i + 1] >> (32 - bits));
    }
    array[length - 1] <<= bits;
  }
}

/// \brief Shift the number in the array right by bits positions.
static inline void ShiftArrayRight(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits > 0) {
    for (int32_t i = length - 1; i > 0; --i) {
      array[i] = (array[i] >> bits) | (array[i - 1] << (32 - bits));
    }
    array[0] >>= bits;
  }
}

/// \brief Fix the signs of the result and remainder after a division operation.
static inline void FixDivisionSigns(Decimal* result, Decimal* remainder,
                                    bool dividend_negative, bool divisor_negative) {
  if (dividend_negative != divisor_negative) {
    result->Negate();
  }
  if (dividend_negative) {
    remainder->Negate();
  }
}

/// \brief Build a Decimal from a big-endian array of uint32_t.
static Status BuildFromArray(Decimal* result, const uint32_t* array, int64_t length) {
  // result_array[0] is low, result_array[1] is high
  std::array<uint64_t, 2> result_array = {0, 0};
  for (int64_t i = length - 2 * 2 - 1; i >= 0; i--) {
    if (array[i] != 0) {
      return Invalid("Decimal division overflow");
    }
  }

  // Construct the array in reverse order
  int64_t next_index = length - 1;
  for (size_t i = 0; i < 2 && next_index >= 0; i++) {
    uint64_t lower_bits = array[next_index--];
    result_array[i] =
        (next_index < 0)
            ? lower_bits
            : (static_cast<uint64_t>(array[next_index--]) << 32) | lower_bits;
  }

  *result = Decimal(result_array[1], result_array[0]);
  return {};
}

/// \brief Do a division where the divisor fits into a single 32-bit integer.
static inline Status SingleDivide(const uint32_t* dividend, int64_t dividend_length,
                                  uint32_t divisor, bool dividend_negative,
                                  bool divisor_negative, Decimal* result,
                                  Decimal* remainder) {
  uint64_t r = 0;
  constexpr int64_t kDecimalArrayLength = Decimal::kBitWidth / sizeof(uint32_t) + 1;
  std::array<uint32_t, kDecimalArrayLength> quotient;
  for (int64_t i = 0; i < dividend_length; ++i) {
    r <<= 32;
    r |= dividend[i];
    if (r < divisor) {
      quotient[i] = 0;
    } else {
      quotient[i] = static_cast<uint32_t>(r / divisor);
      r -= static_cast<uint64_t>(quotient[i]) * divisor;
    }
  }

  ICEBERG_RETURN_UNEXPECTED(BuildFromArray(result, quotient.data(), dividend_length));

  *remainder = static_cast<int64_t>(r);
  FixDivisionSigns(result, remainder, dividend_negative, divisor_negative);
  return {};
}

/// \brief Divide two Decimal values and return the result and remainder.
static inline Status DecimalDivide(const Decimal& dividend, const Decimal& divisor,
                                   Decimal* result, Decimal* remainder) {
  constexpr int64_t kDecimalArrayLength = Decimal::kBitWidth / sizeof(uint32_t);
  // Split the dividend and divisor into 32-bit integer pieces so that we can work on
  // them.
  std::array<uint32_t, kDecimalArrayLength + 1> dividend_array;
  std::array<uint32_t, kDecimalArrayLength> divisor_array;
  bool dividend_negative = dividend.high() < 0;
  bool divisor_negative = divisor.high() < 0;

  // leave an extra zero before the dividend
  dividend_array[0] = 0;
  int64_t dividend_length = FillInArray(dividend, dividend_array.data() + 1) + 1;
  int64_t divisor_length = FillInArray(divisor, divisor_array.data());

  if (dividend_length <= divisor_length) {
    *remainder = dividend;
    *result = 0;
    return {};
  }

  if (divisor_length == 0) {
    return Invalid("Cannot divide by zero in DecimalDivide");
  }

  if (divisor_length == 1) {
    ICEBERG_RETURN_UNEXPECTED(SingleDivide(dividend_array.data(), dividend_length,
                                           divisor_array[0], dividend_negative,
                                           divisor_negative, result, remainder));
    return {};
  }

  int64_t result_length = dividend_length - divisor_length;
  std::array<uint32_t, kDecimalArrayLength> result_array;

  assert(result_length < kDecimalArrayLength);

  // Normalize by shifting both by a multiple of 2 so that the digit guessing is easier.
  // The requirement is that divisor_array[0] is greater than 2**31.
  int64_t normalize_bits = std::countl_zero(divisor_array[0]);
  ShiftArrayLeft(divisor_array.data(), divisor_length, normalize_bits);
  ShiftArrayLeft(dividend_array.data(), dividend_length, normalize_bits);

  // compute each digit in the result
  for (int64_t i = 0; i < result_length; ++i) {
    // Guess the next digit. At worst it is two too large
    uint32_t guess = std::numeric_limits<uint32_t>::max();
    const auto high_dividend =
        static_cast<uint64_t>(dividend_array[i]) << 32 | dividend_array[i + 1];
    if (dividend_array[i] != divisor_array[0]) {
      guess = static_cast<uint32_t>(high_dividend / divisor_array[0]);
    }

    // catch all the cases where the guess is two too large and most of the cases where it
    // is one too large
    auto rhat = static_cast<uint32_t>(high_dividend -
                                      guess * static_cast<uint64_t>(divisor_array[0]));
    while (static_cast<uint64_t>(divisor_array[1]) * guess >
           (static_cast<uint64_t>(rhat) << 32) + dividend_array[i + 2]) {
      guess--;
      rhat += divisor_array[0];
      if (static_cast<uint64_t>(rhat) < divisor_array[1]) {
        break;
      }
    }

    // substract off the guess * divisor from the dividend
    uint64_t mult = 0;
    for (int64_t j = divisor_length - 1; j >= 0; --j) {
      mult += static_cast<uint64_t>(guess) * divisor_array[j];
      uint32_t prev = dividend_array[i + j + 1];
      dividend_array[i + j + 1] -= static_cast<uint32_t>(mult);
      mult >>= 32;
      if (dividend_array[i + j + 1] > prev) {
        ++mult;
      }
    }
    uint32_t prev = dividend_array[i];
    dividend_array[i] -= static_cast<uint32_t>(mult);

    // if guess was too big,  we add back divisor
    if (dividend_array[i] > prev) {
      guess--;
      uint32_t carry = 0;
      for (int64_t j = divisor_length - 1; j >= 0; --j) {
        const auto sum =
            static_cast<uint64_t>(divisor_array[j]) + dividend_array[i + j + 1] + carry;
        dividend_array[i + j + 1] = static_cast<uint32_t>(sum);
        carry = static_cast<uint32_t>(sum >> 32);
      }
      dividend_array[i] += carry;
    }

    result_array[i] = guess;
  }

  // denormalize the remainder
  ShiftArrayRight(dividend_array.data(), dividend_length, normalize_bits);

  // return result and remainder
  ICEBERG_RETURN_UNEXPECTED(BuildFromArray(result, result_array.data(), result_length));
  ICEBERG_RETURN_UNEXPECTED(
      BuildFromArray(remainder, dividend_array.data(), dividend_length));

  FixDivisionSigns(result, remainder, dividend_negative, divisor_negative);
  return {};
}

}  // namespace

Decimal::Decimal(std::string_view str) {
  auto result = Decimal::FromString(str);
  if (!result) {
    throw std::runtime_error(
        std::format("Failed to parse Decimal from string: {}, error: {}", str,
                    result.error().message));
  }
  *this = std::move(result.value());
}

Decimal& Decimal::Negate() {
  uint64_t result_low = ~low() + 1;
  int64_t result_high = ~high();
  if (result_low == 0) {
    result_high = SafeSignedAdd<int64_t>(result_high, 1);
  }
  *this = Decimal(result_high, result_low);
  return *this;
}

Decimal& Decimal::Abs() { return *this < 0 ? Negate() : *this; }

Decimal Decimal::Abs(const Decimal& value) {
  Decimal result(value);
  return result.Abs();
}

Decimal& Decimal::operator+=(const Decimal& other) {
  int64_t result_high = SafeSignedAdd(high(), other.high());
  uint64_t result_low = low() + other.low();
  result_high = SafeSignedAdd<int64_t>(result_high, result_low < low());
  *this = Decimal(result_high, result_low);
  return *this;
}

Decimal& Decimal::operator-=(const Decimal& other) {
  int64_t result_high = SafeSignedSubtract(high(), other.high());
  uint64_t result_low = low() - other.low();
  result_high = SafeSignedSubtract<int64_t>(result_high, result_low > low());
  *this = Decimal(result_high, result_low);
  return *this;
}

Decimal& Decimal::operator*=(const Decimal& other) {
  // Since the max value of Decimal is supposed to be 1e38 - 1 and the min the
  // negation taking the abosolute values here should aways be safe.
  const bool negate = Sign() != other.Sign();
  Decimal x = Decimal::Abs(*this);
  Decimal y = Decimal::Abs(other);

  Uint128 r(x);
  r *= Uint128(y);

  *this = Decimal(static_cast<int64_t>(r.high()), r.low());
  if (negate) {
    Negate();
  }
  return *this;
}

Result<std::pair<Decimal, Decimal>> Decimal::Divide(const Decimal& divisor) const {
  std::pair<Decimal, Decimal> result;
  ICEBERG_RETURN_UNEXPECTED(DecimalDivide(*this, divisor, &result.first, &result.second));
  return result;
}

Decimal& Decimal::operator/=(const Decimal& other) {
  Decimal remainder;
  auto s = DecimalDivide(*this, other, this, &remainder);
  assert(s);
  return *this;
}

Decimal& Decimal::operator|=(const Decimal& other) {
  data_[0] |= other.data_[0];
  data_[1] |= other.data_[1];
  return *this;
}

Decimal& Decimal::operator&=(const Decimal& other) {
  data_[0] &= other.data_[0];
  data_[1] &= other.data_[1];
  return *this;
}

Decimal& Decimal::operator<<=(uint32_t shift) {
  if (shift != 0) {
    uint64_t low_;
    int64_t high_;
    if (shift < 64) {
      high_ = SafeLeftShift<int64_t>(high(), shift);
      high_ |= low() >> (64 - shift);
      low_ = low() << shift;
    } else if (shift < 128) {
      high_ = static_cast<int64_t>(low() << (shift - 64));
      low_ = 0;
    } else {
      high_ = 0;
      low_ = 0;
    }
    *this = Decimal(high_, low_);
  }

  return *this;
}

Decimal& Decimal::operator>>=(uint32_t shift) {
  if (shift != 0) {
    uint64_t low_;
    int64_t high_;
    if (shift < 64) {
      low_ = low() >> shift;
      low_ |= static_cast<uint64_t>(high()) << (64 - shift);
      high_ = high() >> shift;
    } else if (shift < 128) {
      low_ = static_cast<uint64_t>(high() >> (shift - 64));
      high_ = high() >> 63;
    } else {
      high_ = high() >> 63;
      low_ = static_cast<uint64_t>(high_);
    }
    *this = Decimal(high_, low_);
  }

  return *this;
}

namespace {

struct DecimalComponents {
  std::string_view while_digits;
  std::string_view fractional_digits;
  int32_t exponent;
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
    ++pos;
    // Second run of digits after the dot
    pos = ParseDigitsRun(str, pos, &out->fractional_digits);
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

static inline void ShiftAndAdd(std::string_view input, std::array<uint64_t, 2>& out) {
  for (size_t pos = 0; pos < input.size();) {
    const size_t group_size = std::min(kInt64DecimalDigits, input.size() - pos);
    const uint64_t multiple = kUInt64PowersOfTen[group_size];
    uint64_t value = 0;

    std::from_chars(input.data() + pos, input.data() + pos + group_size, value);

    for (size_t i = 0; i < 2; ++i) {
      Uint128 tmp(out[i]);
      tmp *= Uint128(multiple);
      tmp += Uint128(value);
      out[i] = tmp.low();
      value = tmp.high();
    }
    pos += group_size;
  }
}

// Returns a mask for the bit_index lower order bits.
// Only valid for bit_index in the range [0, 64).
constexpr uint64_t LeastSignificantBitMask(int64_t bit_index) {
  return (static_cast<uint64_t>(1) << bit_index) - 1;
}

static void AppendLittleEndianArrayToString(const std::array<uint64_t, 2>& array,
                                            std::string* out) {
  const auto most_significant_non_zero = std::ranges::find_if(
      array.rbegin(), array.rend(), [](uint64_t v) { return v != 0; });
  if (most_significant_non_zero == array.rend()) {
    out->push_back('0');
    return;
  }

  size_t most_significant_elem_idx = &*most_significant_non_zero - array.data();
  std::array<uint64_t, 2> copy = array;
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
  uint64_t* most_significant_elem = &copy[most_significant_elem_idx];

  do {
    // Compute remainder = copy % 1e9 and copy = copy / 1e9.
    uint32_t remainder = 0;
    uint64_t* elem = most_significant_elem;
    do {
      // Compute dividend = (remainder << 32) | *elem  (a virtual 96-bit integer);
      // *elem = dividend / 1e9;
      // remainder = dividend % 1e9.
      auto hi = static_cast<uint32_t>(*elem >> 32);
      auto lo = static_cast<uint32_t>(*elem & LeastSignificantBitMask(32));
      uint64_t dividend_hi = (static_cast<uint64_t>(remainder) << 32) | hi;
      uint64_t quotient_hi = dividend_hi / k1e9;
      remainder = static_cast<uint32_t>(dividend_hi % k1e9);
      uint64_t dividend_lo = (static_cast<uint64_t>(remainder) << 32) | lo;
      uint64_t quotient_lo = dividend_lo / k1e9;
      remainder = static_cast<uint32_t>(dividend_lo % k1e9);
      *elem = (quotient_hi << 32) | quotient_lo;
    } while (elem-- != copy.data());

    segments[num_segments++] = remainder;
  } while (*most_significant_elem != 0 || most_significant_elem-- != copy.data());

  const uint32_t* segment = &segments[num_segments - 1];
  std::stringstream oss;
  // First segment is formatted as-is.
  oss << *segment;
  // Remaining segments are formatted with leading zeros to fill 9 digits. e.g. 123 is
  // formatted as "000000123"
  while (segment != segments.data()) {
    --segment;
    oss << std::setfill('0') << std::setw(9) << *segment;
  }
  out->append(oss.str());
}

static void AdjustIntegerStringWithScale(std::string* str, int32_t scale) {
  if (scale == 0) {
    return;
  }
  assert(str != nullptr);
  assert(!str->empty());
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

}  // namespace

Result<std::string> Decimal::ToString(int32_t scale) const {
  if (scale < -kMaxScale || scale > kMaxScale) {
    return InvalidArgument(
        "Decimal::ToString: scale must be in the range [-{}, {}], was {}", kMaxScale,
        kMaxScale, scale);
  }
  std::string str(ToIntegerString());
  AdjustIntegerStringWithScale(&str, scale);
  return str;
}

std::string Decimal::ToIntegerString() const {
  std::string result;
  if (high() < 0) {
    result.push_back('-');
    Decimal abs = *this;
    abs.Negate();
    AppendLittleEndianArrayToString({abs.low(), static_cast<uint64_t>(abs.high())},
                                    &result);
  } else {
    AppendLittleEndianArrayToString({low(), static_cast<uint64_t>(high())}, &result);
  }
  return result;
}

Result<Decimal> Decimal::FromString(std::string_view str, int32_t* precision,
                                    int32_t* scale) {
  if (str.empty()) {
    return InvalidArgument("Decimal::FromString: empty string is not a valid Decimal");
  }
  DecimalComponents dec;
  if (!ParseDecimalComponents(str, &dec)) {
    return InvalidArgument("Decimal::FromString: invalid decimal string '{}'", str);
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

  // Index 1 is the high part, index 0 is the low part
  std::array<uint64_t, 2> result_array = {0, 0};
  ShiftAndAdd(dec.while_digits, result_array);
  ShiftAndAdd(dec.fractional_digits, result_array);
  Decimal result(static_cast<int64_t>(result_array[1]), result_array[0]);

  if (dec.sign == '-') {
    result.Negate();
  }

  if (parsed_scale < 0) {
    // For the scale to 0, to avoid negative scales (due to compatibility issues with
    // external systems such as databases)
    if (parsed_scale < -kMaxScale) {
      return InvalidArgument(
          "Decimal::FromString: scale must be in the range [-{}, {}], was {}", kMaxScale,
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

namespace {

constexpr float kFloatInf = std::numeric_limits<float>::infinity();

// Attention: these pre-computed constants might not exactly represent their
// decimal counterparts:
//   >>> int32_t(1e38)
//   99999999999999997748809823456034029568

constexpr int32_t kPrecomputedPowersOfTen = 76;

constexpr std::array<float, 2 * kPrecomputedPowersOfTen + 1> kFloatPowersOfTen = {
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         1e-45f,    1e-44f,    1e-43f,    1e-42f,
    1e-41f,    1e-40f,    1e-39f,    1e-38f,    1e-37f,    1e-36f,    1e-35f,
    1e-34f,    1e-33f,    1e-32f,    1e-31f,    1e-30f,    1e-29f,    1e-28f,
    1e-27f,    1e-26f,    1e-25f,    1e-24f,    1e-23f,    1e-22f,    1e-21f,
    1e-20f,    1e-19f,    1e-18f,    1e-17f,    1e-16f,    1e-15f,    1e-14f,
    1e-13f,    1e-12f,    1e-11f,    1e-10f,    1e-9f,     1e-8f,     1e-7f,
    1e-6f,     1e-5f,     1e-4f,     1e-3f,     1e-2f,     1e-1f,     1e0f,
    1e1f,      1e2f,      1e3f,      1e4f,      1e5f,      1e6f,      1e7f,
    1e8f,      1e9f,      1e10f,     1e11f,     1e12f,     1e13f,     1e14f,
    1e15f,     1e16f,     1e17f,     1e18f,     1e19f,     1e20f,     1e21f,
    1e22f,     1e23f,     1e24f,     1e25f,     1e26f,     1e27f,     1e28f,
    1e29f,     1e30f,     1e31f,     1e32f,     1e33f,     1e34f,     1e35f,
    1e36f,     1e37f,     1e38f,     kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf};

constexpr std::array<double, 2 * kPrecomputedPowersOfTen + 1> kDoublePowersOfTen = {
    1e-76, 1e-75, 1e-74, 1e-73, 1e-72, 1e-71, 1e-70, 1e-69, 1e-68, 1e-67, 1e-66, 1e-65,
    1e-64, 1e-63, 1e-62, 1e-61, 1e-60, 1e-59, 1e-58, 1e-57, 1e-56, 1e-55, 1e-54, 1e-53,
    1e-52, 1e-51, 1e-50, 1e-49, 1e-48, 1e-47, 1e-46, 1e-45, 1e-44, 1e-43, 1e-42, 1e-41,
    1e-40, 1e-39, 1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30, 1e-29,
    1e-28, 1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21, 1e-20, 1e-19, 1e-18, 1e-17,
    1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9,  1e-8,  1e-7,  1e-6,  1e-5,
    1e-4,  1e-3,  1e-2,  1e-1,  1e0,   1e1,   1e2,   1e3,   1e4,   1e5,   1e6,   1e7,
    1e8,   1e9,   1e10,  1e11,  1e12,  1e13,  1e14,  1e15,  1e16,  1e17,  1e18,  1e19,
    1e20,  1e21,  1e22,  1e23,  1e24,  1e25,  1e26,  1e27,  1e28,  1e29,  1e30,  1e31,
    1e32,  1e33,  1e34,  1e35,  1e36,  1e37,  1e38,  1e39,  1e40,  1e41,  1e42,  1e43,
    1e44,  1e45,  1e46,  1e47,  1e48,  1e49,  1e50,  1e51,  1e52,  1e53,  1e54,  1e55,
    1e56,  1e57,  1e58,  1e59,  1e60,  1e61,  1e62,  1e63,  1e64,  1e65,  1e66,  1e67,
    1e68,  1e69,  1e70,  1e71,  1e72,  1e73,  1e74,  1e75,  1e76};

// ceil(log2(10 ^ k)) for k in [0...76]
constexpr std::array<int32_t, 76 + 1> kCeilLog2PowersOfTen = {
    0,   4,   7,   10,  14,  17,  20,  24,  27,  30,  34,  37,  40,  44,  47,  50,
    54,  57,  60,  64,  67,  70,  74,  77,  80,  84,  87,  90,  94,  97,  100, 103,
    107, 110, 113, 117, 120, 123, 127, 130, 133, 137, 140, 143, 147, 150, 153, 157,
    160, 163, 167, 170, 173, 177, 180, 183, 187, 190, 193, 196, 200, 203, 206, 210,
    213, 216, 220, 223, 226, 230, 233, 236, 240, 243, 246, 250, 253};

template <typename Real>
struct RealTraits {};

template <>
struct RealTraits<float> {
  static constexpr const float* powers_of_ten() { return kFloatPowersOfTen.data(); }

  static constexpr float two_to_64(float x) { return x * 1.8446744e+19f; }
  static constexpr float two_to_128(float x) { return x == 0 ? 0 : kFloatInf; }
  static constexpr float two_to_192(float x) { return x == 0 ? 0 : kFloatInf; }

  static constexpr int32_t kMantissaBits = 24;
  // ceil(log10(2 ^ kMantissaBits))
  static constexpr int32_t kMantissaDigits = 8;
  // Integers between zero and kMaxPreciseInteger can be precisely represented
  static constexpr uint64_t kMaxPreciseInteger = (1ULL << kMantissaBits) - 1;
};

template <>
struct RealTraits<double> {
  static constexpr const double* powers_of_ten() { return kDoublePowersOfTen.data(); }

  static constexpr double two_to_64(double x) { return x * 1.8446744073709552e+19; }
  static constexpr double two_to_128(double x) { return x * 3.402823669209385e+38; }
  static constexpr double two_to_192(double x) { return x * 6.277101735386681e+57; }

  static constexpr int32_t kMantissaBits = 53;
  // ceil(log10(2 ^ kMantissaBits))
  static constexpr int32_t kMantissaDigits = 16;
  // Integers between zero and kMaxPreciseInteger can be precisely represented
  static constexpr uint64_t kMaxPreciseInteger = (1ULL << kMantissaBits) - 1;
};

struct DecimalRealConversion {
  // Return 10**exp, with a fast lookup, assuming `exp` is within bounds
  template <typename Real>
  static Real PowerOfTen(int32_t exp) {
    constexpr int32_t N = kPrecomputedPowersOfTen;
    assert(exp >= -N && exp <= N);
    return RealTraits<Real>::powers_of_ten()[N + exp];
  }

  // Return 10**exp, with a fast lookup if possible
  template <typename Real>
  static Real LargePowerOfTen(int32_t exp) {
    constexpr int32_t N = kPrecomputedPowersOfTen;
    if (exp >= -N && exp <= N) {
      return RealTraits<Real>::powers_of_ten()[N + exp];
    } else {
      return std::pow(static_cast<Real>(10.0), static_cast<Real>(exp));
    }
  }

  static constexpr int32_t kMaxPrecision = Decimal::kMaxPrecision;
  static constexpr int32_t kMaxScale = Decimal::kMaxScale;

  static constexpr auto& DecimalPowerOfTen(int32_t exp) {
    assert(exp >= 0 && exp <= kMaxPrecision);
    return kDecimal128PowersOfTen[exp];
  }

  // Right shift positive `x` by positive `bits`, rounded half to even
  static Decimal RoundedRightShift(const Decimal& x, int32_t bits) {
    if (bits == 0) {
      return x;
    }
    int64_t result_hi = x.high();
    uint64_t result_lo = x.low();
    uint64_t shifted = 0;
    while (bits >= 64) {
      // Retain the information that set bits were shifted right.
      // This is important to detect an exact half.
      shifted = result_lo | (shifted > 0);
      result_lo = result_hi;
      result_hi >>= 63;  // for sign
      bits -= 64;
    }
    if (bits > 0) {
      shifted = (result_lo << (64 - bits)) | (shifted > 0);
      result_lo >>= bits;
      result_lo |= static_cast<uint64_t>(result_hi) << (64 - bits);
      result_hi >>= bits;
    }
    // We almost have our result, but now do the rounding.
    constexpr uint64_t kHalf = 0x8000000000000000ULL;
    if (shifted > kHalf) {
      // Strictly more than half => round up
      result_lo += 1;
      result_hi += (result_lo == 0);
    } else if (shifted == kHalf) {
      // Exactly half => round to even
      if ((result_lo & 1) != 0) {
        result_lo += 1;
        result_hi += (result_lo == 0);
      }
    } else {
      // Strictly less than half => round down
    }
    return Decimal{result_hi, result_lo};
  }

  template <typename Real>
  static Result<Decimal> FromPositiveApprox(Real real, int32_t precision, int32_t scale) {
    // Approximate algorithm that operates in the FP domain (thus subject
    // to precision loss).
    const auto x = std::nearbyint(real * PowerOfTen<double>(scale));
    const auto max_abs = PowerOfTen<double>(precision);
    if (x <= -max_abs || x >= max_abs) {
      return Invalid("Cannot convert {} to Decimal(precision = {}, scale = {}): overflow",
                     real, precision, scale);
    }

    // Extract high and low bits
    const auto high = std::floor(std::ldexp(x, -64));
    const auto low = x - std::ldexp(high, 64);

    assert(high >= 0);
    assert(high < 9.223372036854776e+18);  // 2**63
    assert(low >= 0);
    assert(low < 1.8446744073709552e+19);  // 2**64
    return Decimal(static_cast<int64_t>(high), static_cast<uint64_t>(low));
  }

  template <typename Real>
  static Result<Decimal> FromPositiveReal(Real real, int32_t precision, int32_t scale) {
    constexpr int32_t kMantissaBits = RealTraits<Real>::kMantissaBits;
    constexpr int32_t kMantissaDigits = RealTraits<Real>::kMantissaDigits;

    // Problem statement: construct the Decimal with the value
    // closest to `real * 10^scale`.
    if (scale < 0) {
      // Negative scales are not handled below, fall back to approx algorithm
      return FromPositiveApprox(real, precision, scale);
    }

    // 1. Check that `real` is within acceptable bounds.
    const Real limit = PowerOfTen<Real>(precision - scale);
    if (real > limit) {
      // Checking the limit early helps ensure the computations below do not
      // overflow.
      // NOTE: `limit` is allowed here as rounding can make it smaller than
      // the theoretical limit (for example, 1.0e23 < 10^23).
      return Invalid("Cannot convert {} to Decimal(precision = {}, scale = {}): overflow",
                     real, precision, scale);
    }

    // 2. Losslessly convert `real` to `mant * 2**k`
    int32_t binary_exp = 0;
    const Real real_mant = std::frexp(real, &binary_exp);
    // `real_mant` is within 0.5 and 1 and has M bits of precision.
    // Multiply it by 2^M to get an exact integer.
    const auto mant = static_cast<uint64_t>(std::ldexp(real_mant, kMantissaBits));
    const int32_t k = binary_exp - kMantissaBits;
    // (note that `real = mant * 2^k`)

    // 3. Start with `mant`.
    // We want to end up with `real * 10^scale` i.e. `mant * 2^k * 10^scale`.
    Decimal x(mant);

    if (k < 0) {
      // k < 0 (i.e. binary_exp < kMantissaBits), is probably the common case
      // when converting to decimal. It implies right-shifting by -k bits,
      // while multiplying by 10^scale. We also must avoid overflow (losing
      // bits on the left) and precision loss (losing bits on the right).
      int32_t right_shift_by = -k;
      int32_t mul_by_ten_to = scale;

      // At this point, `x` has kMantissaDigits significant digits but it can
      // fit kMaxPrecision (excluding sign). We can therefore multiply by up
      // to 10^(kMaxPrecision - kMantissaDigits).
      constexpr int32_t kSafeMulByTenTo = kMaxPrecision - kMantissaDigits;

      if (mul_by_ten_to <= kSafeMulByTenTo) {
        // Scale is small enough, so we can do it all at once.
        x *= DecimalPowerOfTen(mul_by_ten_to);
        x = RoundedRightShift(x, right_shift_by);
      } else {
        // Scale is too large, we cannot multiply at once without overflow.
        // We use an iterative algorithm which alternately shifts left by
        // multiplying by a power of ten, and shifts right by a number of bits.

        // First multiply `x` by as large a power of ten as possible
        // without overflowing.
        x *= DecimalPowerOfTen(kSafeMulByTenTo);
        mul_by_ten_to -= kSafeMulByTenTo;

        // `x` now has full precision. However, we know we'll only
        // keep `precision` digits at the end. Extraneous bits/digits
        // on the right can be safely shifted away, before multiplying
        // again.
        // NOTE: if `precision` is the full precision then the algorithm will
        // lose the last digit. If `precision` is almost the full precision,
        // there can be an off-by-one error due to rounding.
        const int32_t mul_step = std::max(1, kMaxPrecision - precision);

        // The running exponent, useful to compute by how much we must
        // shift right to make place on the left before the next multiply.
        int32_t total_exp = 0;
        int32_t total_shift = 0;
        while (mul_by_ten_to > 0 && right_shift_by > 0) {
          const int32_t exp = std::min(mul_by_ten_to, mul_step);
          total_exp += exp;
          // The supplementary right shift required so that
          // `x * 10^total_exp / 2^total_shift` fits in the decimal.
          assert(static_cast<size_t>(total_exp) < sizeof(kCeilLog2PowersOfTen));
          const int32_t bits =
              std::min(right_shift_by, kCeilLog2PowersOfTen[total_exp] - total_shift);
          total_shift += bits;
          // Right shift to make place on the left, then multiply
          x = RoundedRightShift(x, bits);
          right_shift_by -= bits;
          // Should not overflow thanks to the precautions taken
          x *= DecimalPowerOfTen(exp);
          mul_by_ten_to -= exp;
        }
        if (mul_by_ten_to > 0) {
          x *= DecimalPowerOfTen(mul_by_ten_to);
        }
        if (right_shift_by > 0) {
          x = RoundedRightShift(x, right_shift_by);
        }
      }
    } else {
      // k >= 0 implies left-shifting by k bits and multiplying by 10^scale.
      // The order of these operations therefore doesn't matter. We know
      // we won't overflow because of the limit check above, and we also
      // won't lose any significant bits on the right.
      x *= DecimalPowerOfTen(scale);
      x <<= k;
    }

    // Rounding might have pushed `x` just above the max precision, check again
    if (!x.FitsInPrecision(precision)) {
      return Invalid("Cannot convert {} to Decimal(precision = {}, scale = {}): overflow",
                     real, precision, scale);
    }
    return x;
  }

  template <typename Real>
  static Real ToRealPositiveNoSplit(const Decimal& decimal, int32_t scale) {
    Real x = RealTraits<Real>::two_to_64(static_cast<Real>(decimal.high()));
    x += static_cast<Real>(decimal.low());
    x *= LargePowerOfTen<Real>(-scale);
    return x;
  }

  /// An approximate conversion from Decimal128 to Real that guarantees:
  /// 1. If the decimal is an integer, the conversion is exact.
  /// 2. If the number of fractional digits is <= RealTraits<Real>::kMantissaDigits (e.g.
  ///    8 for float and 16 for double), the conversion is within 1 ULP of the exact
  ///    value.
  /// 3. Otherwise, the conversion is within 2^(-RealTraits<Real>::kMantissaDigits+1)
  ///    (e.g. 2^-23 for float and 2^-52 for double) of the exact value.
  /// Here "exact value" means the closest representable value by Real.
  template <typename Real>
  static Real ToRealPositive(const Decimal& decimal, int32_t scale) {
    if (scale <= 0 ||
        (decimal.high() == 0 && decimal.low() <= RealTraits<Real>::kMaxPreciseInteger)) {
      // No need to split the decimal if it is already an integer (scale <= 0) or if it
      // can be precisely represented by Real
      return ToRealPositiveNoSplit<Real>(decimal, scale);
    }

    // Split decimal into whole and fractional parts to avoid precision loss
    auto s = decimal.GetWholeAndFraction(scale);
    assert(s);

    Real whole = ToRealPositiveNoSplit<Real>(s->first, 0);
    Real fraction = ToRealPositiveNoSplit<Real>(s->second, scale);

    return whole + fraction;
  }

  template <typename Real>
  static Result<Decimal> FromReal(Real value, int32_t precision, int32_t scale) {
    assert(precision >= 1 && precision <= kMaxPrecision);
    assert(scale >= -kMaxScale && scale <= kMaxScale);

    if (!std::isfinite(value)) {
      return InvalidArgument("Cannot convert {} to Decimal", value);
    }

    if (value == 0) {
      return Decimal{};
    }

    if (value < 0) {
      ICEBERG_ASSIGN_OR_RAISE(auto decimal, FromPositiveReal(-value, precision, scale));
      return decimal.Negate();
    } else {
      return FromPositiveReal(value, precision, scale);
    }
  }

  template <typename Real>
  static Real ToReal(const Decimal& decimal, int32_t scale) {
    assert(scale >= -kMaxScale && scale <= kMaxScale);

    if (decimal.IsNegative()) {
      // Convert the absolute value to avoid precision loss
      auto abs = decimal;
      abs.Negate();
      return -ToRealPositive<Real>(abs, scale);
    } else {
      return ToRealPositive<Real>(decimal, scale);
    }
  }
};

// Helper function used by Decimal::FromBigEndian
static inline uint64_t UInt64FromBigEndian(const uint8_t* bytes, int32_t length) {
  // We don't bounds check the length here because this is called by
  // FromBigEndian that has a Decimal128 as its out parameters and
  // that function is already checking the length of the bytes and only
  // passes lengths between zero and eight.
  uint64_t result = 0;
  // Using memcpy instead of special casing for length
  // and doing the conversion in 16, 32 parts, which could
  // possibly create unaligned memory access on certain platforms
  memcpy(reinterpret_cast<uint8_t*>(&result) + 8 - length, bytes, length);
#if ICEBERG_LITTLE_ENDIAN
  return std::byteswap(result);
#else
  return result;
#endif
}

}  // namespace

Result<Decimal> Decimal::FromReal(float x, int32_t precision, int32_t scale) {
  return DecimalRealConversion::FromReal(x, precision, scale);
}

Result<Decimal> Decimal::FromReal(double x, int32_t precision, int32_t scale) {
  return DecimalRealConversion::FromReal(x, precision, scale);
}

Result<std::pair<Decimal, Decimal>> Decimal::GetWholeAndFraction(int32_t scale) const {
  assert(scale >= 0 && scale <= kMaxScale);

  Decimal multiplier(kDecimal128PowersOfTen[scale]);
  return Divide(multiplier);
}

Result<Decimal> Decimal::FromBigEndian(const uint8_t* bytes, int32_t length) {
  static constexpr int32_t kMinDecimalBytes = 1;
  static constexpr int32_t kMaxDecimalBytes = 16;

  int64_t high, low;

  if (length < kMinDecimalBytes || length > kMaxDecimalBytes) {
    return InvalidArgument(
        "Decimal::FromBigEndian: length must be in the range [{}, {}], was {}",
        kMinDecimalBytes, kMaxDecimalBytes, length);
  }

  // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  // sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  // 1. Extract the high bytes
  // Stop byte of the high bytes
  const int32_t high_bits_offset = std::max(0, length - 8);
  const auto high_bits = UInt64FromBigEndian(bytes, high_bits_offset);

  if (high_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    high = high_bits;
  } else {
    high = -1 * (is_negative && length < kMaxDecimalBytes);
    // Shift left enough bits to make room for the incoming int64_t
    high = SafeLeftShift(high, high_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    high |= high_bits;
  }

  // 2. Extract the low bytes
  // Stop byte of the low bytes
  const int32_t low_bits_offset = std::min(length, 8);
  const auto low_bits =
      UInt64FromBigEndian(bytes + high_bits_offset, length - high_bits_offset);

  if (low_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    low = low_bits;
  } else {
    // Sign extend the low bits if necessary
    low = -1 * (is_negative && length < 8);
    // Shift left enough bits to make room for the incoming int64_t
    low = SafeLeftShift(low, low_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    low |= low_bits;
  }

  return Decimal(high, static_cast<uint64_t>(low));
}

Result<Decimal> Decimal::Rescale(int32_t orig_scale, int32_t new_scale) const {
  if (orig_scale < 0 || orig_scale > kMaxScale) {
    return InvalidArgument(
        "Decimal::Rescale: original scale must be in the range [0, {}], "
        "was {}",
        kMaxScale, orig_scale);
  }
  if (new_scale < 0 || new_scale > kMaxScale) {
    return InvalidArgument(
        "Decimal::Rescale: new scale must be in the range [0, {}], "
        "was {}",
        kMaxScale, new_scale);
  }

  if (orig_scale == new_scale) {
    return *this;
  }

  return NotImplemented("Decimal::Rescale is not implemented yet");
}

bool Decimal::FitsInPrecision(int32_t precision) const {
  assert(precision >= 1 && precision <= kMaxPrecision);
  return Decimal::Abs(*this) < kDecimal128PowersOfTen[precision];
}

float Decimal::ToFloat(int32_t scale) const {
  return DecimalRealConversion::ToReal<float>(*this, scale);
}

double Decimal::ToDouble(int32_t scale) const {
  return DecimalRealConversion::ToReal<double>(*this, scale);
}

std::array<uint8_t, Decimal::kByteWidth> Decimal::ToBytes() const {
  std::array<uint8_t, kByteWidth> out{{0}};
  memcpy(out.data(), data_.data(), kByteWidth);
  return out;
}

ICEBERG_EXPORT std::ostream& operator<<(std::ostream& os, const Decimal& decimal) {
  os << decimal.ToIntegerString();
  return os;
}

// Unary operators
ICEBERG_EXPORT Decimal operator-(const Decimal& operand) {
  Decimal result(operand.high(), operand.low());
  return result.Negate();
}

ICEBERG_EXPORT Decimal operator~(const Decimal& operand) {
  return {~operand.high(), ~operand.low()};
}

// Binary operators
ICEBERG_EXPORT Decimal operator+(const Decimal& lhs, const Decimal& rhs) {
  Decimal result(lhs);
  result += rhs;
  return result;
}

ICEBERG_EXPORT Decimal operator-(const Decimal& lhs, const Decimal& rhs) {
  Decimal result(lhs);
  result -= rhs;
  return result;
}

ICEBERG_EXPORT Decimal operator*(const Decimal& lhs, const Decimal& rhs) {
  Decimal result(lhs);
  result *= rhs;
  return result;
}

ICEBERG_EXPORT Decimal operator/(const Decimal& lhs, const Decimal& rhs) {
  return lhs.Divide(rhs).value().first;
}

ICEBERG_EXPORT Decimal operator%(const Decimal& lhs, const Decimal& rhs) {
  return lhs.Divide(rhs).value().second;
}

ICEBERG_EXPORT bool operator<(const Decimal& lhs, const Decimal& rhs) {
  return (lhs.high() < rhs.high()) || (lhs.high() == rhs.high() && lhs.low() < rhs.low());
}

ICEBERG_EXPORT bool operator<=(const Decimal& lhs, const Decimal& rhs) {
  return !operator>(lhs, rhs);
}

ICEBERG_EXPORT bool operator>(const Decimal& lhs, const Decimal& rhs) {
  return operator<(rhs, lhs);
}

ICEBERG_EXPORT bool operator>=(const Decimal& lhs, const Decimal& rhs) {
  return !operator<(lhs, rhs);
}

}  // namespace iceberg
