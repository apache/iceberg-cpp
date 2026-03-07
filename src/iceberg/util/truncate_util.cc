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

#include "iceberg/util/truncate_util.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "iceberg/expression/literal.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg {

namespace {
constexpr uint32_t kUtf8MaxCodePoint = 0x10FFFF;
constexpr uint32_t kUtf8MinSurrogate = 0xD800;
constexpr uint32_t kUtf8MaxSurrogate = 0xDFFF;

bool DecodeUtf8CodePoint(std::string_view source, uint32_t& code_point) {
  const auto size = source.size();
  if (source.empty()) {
    return false;
  }

  auto byte0 = static_cast<uint8_t>(source[0]);
  if (byte0 < 0x80) {
    code_point = byte0;
    return true;
  }

  if ((byte0 & 0xE0) == 0xC0) {
    if (source.size() < 2) {
      return false;
    }
    auto byte1 = static_cast<uint8_t>(source[1]);
    if ((byte1 & 0xC0) != 0x80) {
      return false;
    }
    code_point = ((byte0 & 0x1F) << 6) | (byte1 & 0x3F);
    if (code_point < 0x80) {
      return false;
    }
    return true;
  }

  if ((byte0 & 0xF0) == 0xE0) {
    if (source.size() < 3) {
      return false;
    }
    auto byte1 = static_cast<uint8_t>(source[1]);
    auto byte2 = static_cast<uint8_t>(source[2]);
    if ((byte1 & 0xC0) != 0x80 || (byte2 & 0xC0) != 0x80) {
      return false;
    }
    code_point = ((byte0 & 0x0F) << 12) | ((byte1 & 0x3F) << 6) | (byte2 & 0x3F);
    if (code_point < 0x800 ||
        (code_point >= kUtf8MinSurrogate && code_point <= kUtf8MaxSurrogate)) {
      return false;
    }
    return true;
  }

  if ((byte0 & 0xF8) == 0xF0) {
    if (source.size() < 4) {
      return false;
    }
    auto byte1 = static_cast<uint8_t>(source[1]);
    auto byte2 = static_cast<uint8_t>(source[2]);
    auto byte3 = static_cast<uint8_t>(source[3]);
    if ((byte1 & 0xC0) != 0x80 || (byte2 & 0xC0) != 0x80 || (byte3 & 0xC0) != 0x80) {
      return false;
    }
    code_point = ((byte0 & 0x07) << 18) | ((byte1 & 0x3F) << 12) | ((byte2 & 0x3F) << 6) |
                 (byte3 & 0x3F);
    if (code_point < 0x10000 || code_point > kUtf8MaxCodePoint) {
      return false;
    }
    return true;
  }

  return false;
}

void AppendUtf8CodePoint(uint32_t code_point, std::string& target) {
  if (code_point <= 0x7F) {
    target.push_back(static_cast<char>(code_point));
  } else if (code_point <= 0x7FF) {
    target.push_back(static_cast<char>(0xC0 | (code_point >> 6)));
    target.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  } else if (code_point <= 0xFFFF) {
    target.push_back(static_cast<char>(0xE0 | (code_point >> 12)));
    target.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    target.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  } else {
    target.push_back(static_cast<char>(0xF0 | (code_point >> 18)));
    target.push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
    target.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    target.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  }
}

template <TypeId type_id>
Literal TruncateLiteralImpl(const Literal& literal, int32_t width) {
  std::unreachable();
}

template <>
Literal TruncateLiteralImpl<TypeId::kInt>(const Literal& literal, int32_t width) {
  int32_t v = std::get<int32_t>(literal.value());
  return Literal::Int(TruncateUtils::TruncateInteger(v, width));
}

template <>
Literal TruncateLiteralImpl<TypeId::kLong>(const Literal& literal, int32_t width) {
  int64_t v = std::get<int64_t>(literal.value());
  return Literal::Long(TruncateUtils::TruncateInteger(v, width));
}

template <>
Literal TruncateLiteralImpl<TypeId::kDecimal>(const Literal& literal, int32_t width) {
  const auto& decimal = std::get<Decimal>(literal.value());
  auto type = internal::checked_pointer_cast<DecimalType>(literal.type());
  return Literal::Decimal(TruncateUtils::TruncateDecimal(decimal, width).value(),
                          type->precision(), type->scale());
}

template <>
Literal TruncateLiteralImpl<TypeId::kString>(const Literal& literal, int32_t width) {
  // Strings are truncated to a valid UTF-8 string with no more than `width` code points.
  const auto& str = std::get<std::string>(literal.value());
  return Literal::String(TruncateUtils::TruncateUTF8(str, width));
}

template <>
Literal TruncateLiteralImpl<TypeId::kBinary>(const Literal& literal, int32_t width) {
  // In contrast to strings, binary values do not have an assumed encoding and are
  // truncated to `width` bytes.
  const auto& data = std::get<std::vector<uint8_t>>(literal.value());
  if (data.size() <= width) {
    return literal;
  }
  return Literal::Binary(std::vector<uint8_t>(data.begin(), data.begin() + width));
}

template <TypeId type_id>
Result<Literal> TruncateLiteralMaxImpl(const Literal& literal, int32_t width) {
  std::unreachable();
}

template <>
Result<Literal> TruncateLiteralMaxImpl<TypeId::kString>(const Literal& literal,
                                                        int32_t width) {
  const auto& str = std::get<std::string>(literal.value());
  ICEBERG_ASSIGN_OR_RAISE(std::string truncated,
                          TruncateUtils::TruncateUTF8Max(str, width));
  if (truncated == str) {
    return literal;
  }
  return Literal::String(std::move(truncated));
}

template <>
Result<Literal> TruncateLiteralMaxImpl<TypeId::kBinary>(const Literal& literal,
                                                        int32_t width) {
  const auto& data = std::get<std::vector<uint8_t>>(literal.value());
  if (static_cast<int32_t>(data.size()) <= width) {
    return literal;
  }

  std::vector<uint8_t> truncated(data.begin(), data.begin() + width);
  for (auto it = truncated.rbegin(); it != truncated.rend(); ++it) {
    if (*it < 0xFF) {
      ++(*it);
      truncated.resize(truncated.size() - std::distance(truncated.rbegin(), it));
      return Literal::Binary(std::move(truncated));
    }
  }
  return InvalidArgument("Cannot truncate upper bound for binary: all bytes are 0xFF");
}

}  // namespace

Result<std::string> TruncateUtils::TruncateUTF8Max(const std::string& source, size_t L) {
  std::string truncated = TruncateUTF8(source, L);
  if (truncated == source) {
    return truncated;
  }

  // Try incrementing code points from the end
  size_t last_cp_start = truncated.size();
  while (last_cp_start > 0) {
    size_t cp_start = last_cp_start;
    // Find the start of the previous code point
    do {
      --cp_start;
    } while (cp_start > 0 && (static_cast<uint8_t>(truncated[cp_start]) & 0xC0) == 0x80);

    uint32_t code_point = 0;
    if (!DecodeUtf8CodePoint(
            std::string_view(truncated.data() + cp_start, last_cp_start - cp_start),
            code_point)) {
      return InvalidArgument("Invalid UTF-8 in string literal");
    }

    // Try to increment the code point
    if (code_point < kUtf8MaxCodePoint) {
      uint32_t next_code_point = code_point + 1;
      // Skip surrogate range
      if (next_code_point >= kUtf8MinSurrogate && next_code_point <= kUtf8MaxSurrogate) {
        next_code_point = kUtf8MaxSurrogate + 1;
      }
      if (next_code_point <= kUtf8MaxCodePoint) {
        truncated.resize(cp_start);
        AppendUtf8CodePoint(next_code_point, truncated);
        return truncated;
      }
    }
    last_cp_start = cp_start;
  }
  return InvalidArgument(
      "Cannot truncate upper bound for string: all code points are 0x10FFFF");
}

Decimal TruncateUtils::TruncateDecimal(const Decimal& decimal, int32_t width) {
  return decimal - (((decimal % width) + width) % width);
}

#define DISPATCH_TRUNCATE_LITERAL(TYPE_ID) \
  case TYPE_ID:                            \
    return TruncateLiteralImpl<TYPE_ID>(literal, width);

Result<Literal> TruncateUtils::TruncateLiteral(const Literal& literal, int32_t width) {
  if (literal.IsNull()) [[unlikely]] {
    // Return null as is
    return literal;
  }

  if (literal.IsAboveMax() || literal.IsBelowMin()) [[unlikely]] {
    return NotSupported("Cannot truncate {}", literal.ToString());
  }

  switch (literal.type()->type_id()) {
    DISPATCH_TRUNCATE_LITERAL(TypeId::kInt)
    DISPATCH_TRUNCATE_LITERAL(TypeId::kLong)
    DISPATCH_TRUNCATE_LITERAL(TypeId::kDecimal)
    DISPATCH_TRUNCATE_LITERAL(TypeId::kString)
    DISPATCH_TRUNCATE_LITERAL(TypeId::kBinary)
    default:
      return NotSupported("Truncate is not supported for type: {}",
                          literal.type()->ToString());
  }
}

#define DISPATCH_TRUNCATE_LITERAL_MAX(TYPE_ID) \
  case TYPE_ID:                                \
    return TruncateLiteralMaxImpl<TYPE_ID>(literal, width);

Result<Literal> TruncateUtils::TruncateLiteralMax(const Literal& literal, int32_t width) {
  if (literal.IsNull()) [[unlikely]] {
    // Return null as is
    return literal;
  }

  if (literal.IsAboveMax() || literal.IsBelowMin()) [[unlikely]] {
    return NotSupported("Cannot truncate {}", literal.ToString());
  }

  switch (literal.type()->type_id()) {
    DISPATCH_TRUNCATE_LITERAL_MAX(TypeId::kString);
    DISPATCH_TRUNCATE_LITERAL_MAX(TypeId::kBinary);
    default:
      return NotSupported("Truncate max is not supported for type: {}",
                          literal.type()->ToString());
  }
}

}  // namespace iceberg
