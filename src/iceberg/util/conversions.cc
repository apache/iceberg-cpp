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

#include "iceberg/util/conversions.h"

#include <array>
#include <cstring>
#include <span>
#include <string>

#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief Write a value in little-endian format and return as vector.
template <EndianConvertible T>
std::vector<uint8_t> WriteLittleEndian(T value) {
  value = ToLittleEndian(value);
  const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
  std::vector<uint8_t> result;
  result.insert(result.end(), bytes, bytes + sizeof(T));
  return result;
}

/// \brief Read a value in little-endian format from the data.
template <EndianConvertible T>
Result<T> ReadLittleEndian(std::span<const uint8_t> data) {
  if (data.size() < sizeof(T)) [[unlikely]] {
    return InvalidArgument("Insufficient data to read {} bytes, got {}", sizeof(T),
                           data.size());
  }

  T value;
  std::memcpy(&value, data.data(), sizeof(T));
  return FromLittleEndian(value);
}

template <TypeId type_id>
Result<std::vector<uint8_t>> ToBytesImpl(const Literal::Value& value) {
  using CppType = typename LiteralTraits<type_id>::ValueType;
  return WriteLittleEndian(std::get<CppType>(value));
}

#define DISPATCH_LITERAL_TO_BYTES(type_id) \
  case type_id:                            \
    return ToBytesImpl<type_id>(value);

Result<std::vector<uint8_t>> Conversions::ToBytes(const PrimitiveType& type,
                                                  const Literal::Value& value) {
  const auto type_id = type.type_id();

  switch (type_id) {
    DISPATCH_LITERAL_TO_BYTES(TypeId::kInt)
    DISPATCH_LITERAL_TO_BYTES(TypeId::kDate)
    DISPATCH_LITERAL_TO_BYTES(TypeId::kLong)
    DISPATCH_LITERAL_TO_BYTES(TypeId::kTime)
    DISPATCH_LITERAL_TO_BYTES(TypeId::kTimestamp)
    DISPATCH_LITERAL_TO_BYTES(TypeId::kTimestampTz)
    DISPATCH_LITERAL_TO_BYTES(TypeId::kFloat)
    DISPATCH_LITERAL_TO_BYTES(TypeId::kDouble)
    case TypeId::kBoolean: {
      return std::vector<uint8_t>{std::get<bool>(value) ? static_cast<uint8_t>(0x01)
                                                        : static_cast<uint8_t>(0x00)};
    }

    case TypeId::kString: {
      const auto& str = std::get<std::string>(value);
      return std::vector<uint8_t>(str.begin(), str.end());
    }

    case TypeId::kBinary: {
      return std::get<std::vector<uint8_t>>(value);
    }

    case TypeId::kFixed: {
      if (std::holds_alternative<std::array<uint8_t, 16>>(value)) {
        const auto& fixed_bytes = std::get<std::array<uint8_t, 16>>(value);
        return std::vector<uint8_t>(fixed_bytes.begin(), fixed_bytes.end());
      } else if (std::holds_alternative<std::vector<uint8_t>>(value)) {
        return std::get<std::vector<uint8_t>>(value);
      } else {
        std::string actual_type = std::visit(
            [](auto&& arg) -> std::string { return typeid(arg).name(); }, value);
        return InvalidArgument("Invalid value type for Fixed literal, got type: {}",
                               actual_type);
      }
    }
      // TODO(Li Feiyang): Add support for UUID and Decimal

    default:
      return NotSupported("Serialization for type {} is not supported", type.ToString());
  }
}

#undef DISPATCH_LITERAL_TO_BYTES

Result<std::vector<uint8_t>> Conversions::ToBytes(const Literal& literal) {
  // Cannot serialize special values
  if (literal.IsAboveMax()) {
    return NotSupported("Cannot serialize AboveMax");
  }
  if (literal.IsBelowMin()) {
    return NotSupported("Cannot serialize BelowMin");
  }
  if (literal.IsNull()) {
    return NotSupported("Cannot serialize null");
  }

  return ToBytes(*literal.type(), literal.value());
}

Result<Literal::Value> Conversions::FromBytes(const PrimitiveType& type,
                                              std::span<const uint8_t> data) {
  if (data.empty()) {
    return InvalidArgument("Data cannot be empty");
  }

  const auto type_id = type.type_id();

  switch (type_id) {
    case TypeId::kBoolean: {
      if (data.size() != 1) {
        return InvalidArgument("Boolean requires 1 byte, got {}", data.size());
      }
      ICEBERG_ASSIGN_OR_RAISE(auto value, ReadLittleEndian<uint8_t>(data));
      return Literal::Value{static_cast<bool>(value != 0x00)};
    }

    case TypeId::kInt: {
      if (data.size() != sizeof(int32_t)) {
        return InvalidArgument("Int requires {} bytes, got {}", sizeof(int32_t),
                               data.size());
      }
      ICEBERG_ASSIGN_OR_RAISE(auto value, ReadLittleEndian<int32_t>(data));
      return Literal::Value{value};
    }

    case TypeId::kDate: {
      if (data.size() != sizeof(int32_t)) {
        return InvalidArgument("Date requires {} bytes, got {}", sizeof(int32_t),
                               data.size());
      }
      ICEBERG_ASSIGN_OR_RAISE(auto value, ReadLittleEndian<int32_t>(data));
      return Literal::Value{value};
    }

    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      int64_t value;
      if (data.size() == 8) {
        ICEBERG_ASSIGN_OR_RAISE(auto long_value, ReadLittleEndian<int64_t>(data));
        value = long_value;
      } else if (data.size() == 4) {
        // Type was promoted from int to long
        ICEBERG_ASSIGN_OR_RAISE(auto int_value, ReadLittleEndian<int32_t>(data));
        value = static_cast<int64_t>(int_value);
      } else {
        return InvalidArgument("{} requires 4 or 8 bytes, got {}", ToString(type_id),
                               data.size());
      }

      return Literal::Value{value};
    }

    case TypeId::kFloat: {
      if (data.size() != sizeof(float)) {
        return InvalidArgument("Float requires {} bytes, got {}", sizeof(float),
                               data.size());
      }
      ICEBERG_ASSIGN_OR_RAISE(auto value, ReadLittleEndian<float>(data));
      return Literal::Value{value};
    }

    case TypeId::kDouble: {
      if (data.size() == 8) {
        ICEBERG_ASSIGN_OR_RAISE(auto double_value, ReadLittleEndian<double>(data));
        return Literal::Value{double_value};
      } else if (data.size() == 4) {
        // Type was promoted from float to double
        ICEBERG_ASSIGN_OR_RAISE(auto float_value, ReadLittleEndian<float>(data));
        return Literal::Value{static_cast<double>(float_value)};
      } else {
        return InvalidArgument("Double requires 4 or 8 bytes, got {}", data.size());
      }
    }

    case TypeId::kString: {
      return Literal::Value{
          std::string(reinterpret_cast<const char*>(data.data()), data.size())};
    }

    case TypeId::kBinary: {
      return Literal::Value{std::vector<uint8_t>(data.begin(), data.end())};
    }

    case TypeId::kFixed: {
      if (data.size() == 16) {
        std::array<uint8_t, 16> fixed_bytes;
        std::ranges::copy(data, fixed_bytes.begin());
        return Literal::Value{fixed_bytes};
      } else {
        return Literal::Value{std::vector<uint8_t>(data.begin(), data.end())};
      }
    }
      // TODO(Li Feiyang): Add support for UUID and Decimal

    default:
      return NotSupported("Deserialization for type {} is not supported",
                          type.ToString());
  }
}

Result<Literal> Conversions::FromBytes(std::shared_ptr<PrimitiveType> type,
                                       std::span<const uint8_t> data) {
  if (!type) {
    return InvalidArgument("Type cannot be null");
  }

  ICEBERG_ASSIGN_OR_RAISE(auto value, FromBytes(*type, data));

  // If we got a null value (monostate), create a null Literal
  if (std::holds_alternative<std::monostate>(value)) {
    return Literal::Null(std::move(type));
  }

  return Literal(std::move(value), std::move(type));
}

}  // namespace iceberg
