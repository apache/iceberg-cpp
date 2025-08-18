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
#include <cstddef>
#include <cstdint>
#include <format>
#include <iomanip>
#include <iostream>
#include <limits>
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
    for (int i = 0; i < length - 1; ++i) {
      array[i] = (array[i] << bits) | (array[i + 1] >> (32 - bits));
    }
    array[length - 1] <<= bits;
  }
}

/// \brief Shift the number in the array right by bits positions.
static inline void ShiftArrayRight(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits > 0) {
    for (int i = length - 1; i > 0; --i) {
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
      return Overflow("Decimal division overflow");
    }
  }

  // Construct the array in reverse order
  int64_t next_index = length - 1;
  for (size_t i = 0; i < 2 && next_index >= 0; i++) {
    uint64_t lower_bits = array[next_index--];
    result_array[i] = (next_index < 0)
                          ? lower_bits
                          : (static_cast<uint64_t>(lower_bits) << 32) | lower_bits;
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
    return DivideByZero("Cannot divide by zero in DecimalDivide");
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

  std::cout << copy[1] << " " << copy[0] << std::endl;
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

}  // namespace

Result<std::string> Decimal::ToString(int32_t scale) const {
  if (scale < -kMaxScale || scale > kMaxScale) {
    return InvalidArgument(
        "Decimal::ToString: scale must be in the range [-{}, {}], was {}", kMaxScale,
        kMaxScale, scale);
  }
  return NotImplemented("Decimal::ToString is not implemented yet");
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

template <typename T>
  requires std::is_floating_point_v<T>
Result<Decimal> Decimal::FromReal(T value, int32_t precision, int32_t scale) {
  if (precision < 1 || precision > kMaxPrecision) {
    return InvalidArgument(
        "Decimal::FromReal: precision must be in the range [1, {}], "
        "was {}",
        kMaxPrecision, precision);
  }
  if (scale > precision) {
    return InvalidArgument(
        "Decimal::FromReal: scale must be less than or equal to "
        "precision, was scale = {}, precision = {}",
        scale, precision);
  }

  return NotImplemented("Decimal::FromReal is not implemented yet");
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

template <typename T>
  requires std::is_floating_point_v<T>
T Decimal::ToReal(int32_t scale) const {
  if (scale < -kMaxScale || scale > kMaxScale) {
    throw InvalidArgument("Decimal::ToReal: scale must be in the range [-{}, {}], was {}",
                          kMaxScale, kMaxScale, scale);
  }

  // Convert Decimal to floating-point value with the given scale.
  // This is a placeholder implementation.
  return T(0);  // Replace with actual conversion logic.
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
