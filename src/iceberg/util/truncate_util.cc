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

}  // namespace

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

}  // namespace iceberg
