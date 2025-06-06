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

#include "iceberg/literal.h"

#include <sstream>

#include "iceberg/exception.h"

namespace iceberg {

// Constructor
PrimitiveLiteral::PrimitiveLiteral(PrimitiveLiteralValue value,
                                   std::shared_ptr<PrimitiveType> type)
    : value_(std::move(value)), type_(std::move(type)) {}

// Factory methods
PrimitiveLiteral PrimitiveLiteral::Boolean(bool value) {
  return {PrimitiveLiteralValue{value}, std::make_shared<BooleanType>()};
}

PrimitiveLiteral PrimitiveLiteral::Int(int32_t value) {
  return {PrimitiveLiteralValue{value}, std::make_shared<IntType>()};
}

PrimitiveLiteral PrimitiveLiteral::Long(int64_t value) {
  return {PrimitiveLiteralValue{value}, std::make_shared<LongType>()};
}

PrimitiveLiteral PrimitiveLiteral::Float(float value) {
  return {PrimitiveLiteralValue{value}, std::make_shared<FloatType>()};
}

PrimitiveLiteral PrimitiveLiteral::Double(double value) {
  return {PrimitiveLiteralValue{value}, std::make_shared<DoubleType>()};
}

PrimitiveLiteral PrimitiveLiteral::String(std::string value) {
  return {PrimitiveLiteralValue{std::move(value)}, std::make_shared<StringType>()};
}

PrimitiveLiteral PrimitiveLiteral::Binary(std::vector<uint8_t> value) {
  return {PrimitiveLiteralValue{std::move(value)}, std::make_shared<BinaryType>()};
}

PrimitiveLiteral PrimitiveLiteral::BelowMinLiteral(std::shared_ptr<PrimitiveType> type) {
  return PrimitiveLiteral(PrimitiveLiteralValue{BelowMin{}}, std::move(type));
}

PrimitiveLiteral PrimitiveLiteral::AboveMaxLiteral(std::shared_ptr<PrimitiveType> type) {
  return PrimitiveLiteral(PrimitiveLiteralValue{AboveMax{}}, std::move(type));
}

Result<PrimitiveLiteral> PrimitiveLiteral::Deserialize(std::span<const uint8_t> data) {
  return NotImplemented("Deserialization of PrimitiveLiteral is not implemented yet");
}

Result<std::vector<uint8_t>> PrimitiveLiteral::Serialize() const {
  return NotImplemented("Serialization of PrimitiveLiteral is not implemented yet");
}

// Getters

const std::shared_ptr<PrimitiveType>& PrimitiveLiteral::type() const { return type_; }

// Cast method
Result<PrimitiveLiteral> PrimitiveLiteral::CastTo(
    const std::shared_ptr<PrimitiveType>& target_type) const {
  if (*type_ == *target_type) {
    // If types are the same, return a copy of the current literal
    return PrimitiveLiteral(value_, target_type);
  }

  // Handle special values
  if (std::holds_alternative<BelowMin>(value_) ||
      std::holds_alternative<AboveMax>(value_)) {
    // Cannot cast type for special values
    return NotSupported("Cannot cast type for {}", ToString());
  }

  auto source_type_id = type_->type_id();
  auto target_type_id = target_type->type_id();

  // Delegate to specific cast functions based on source type
  switch (source_type_id) {
    case TypeId::kInt:
      return CastFromInt(target_type_id);
    case TypeId::kLong:
      return CastFromLong(target_type_id);
    case TypeId::kFloat:
      return CastFromFloat(target_type_id);
    case TypeId::kDouble:
    case TypeId::kBoolean:
    case TypeId::kString:
    case TypeId::kBinary:
      break;
    default:
      break;
  }

  return NotSupported("Cast from {} to {} is not implemented", type_->ToString(),
                      target_type->ToString());
}

Result<PrimitiveLiteral> PrimitiveLiteral::CastFromInt(TypeId target_type_id) const {
  auto int_val = std::get<int32_t>(value_);

  switch (target_type_id) {
    case TypeId::kLong:
      return PrimitiveLiteral::Long(static_cast<int64_t>(int_val));
    case TypeId::kFloat:
      return PrimitiveLiteral::Float(static_cast<float>(int_val));
    case TypeId::kDouble:
      return PrimitiveLiteral::Double(static_cast<double>(int_val));
    // TODO(mwish): Supports casts to date and literal
    default:
      return NotSupported("Cast from Int to {} is not implemented",
                          static_cast<int>(target_type_id));
  }
}

Result<PrimitiveLiteral> PrimitiveLiteral::CastFromLong(TypeId target_type_id) const {
  auto long_val = std::get<int64_t>(value_);

  switch (target_type_id) {
    case TypeId::kInt: {
      // Check for overflow
      if (long_val >= std::numeric_limits<int32_t>::max()) {
        return PrimitiveLiteral::AboveMaxLiteral(type_);
      }
      if (long_val <= std::numeric_limits<int32_t>::min()) {
        return PrimitiveLiteral::BelowMinLiteral(type_);
      }
      return PrimitiveLiteral::Int(static_cast<int32_t>(long_val));
    }
    case TypeId::kFloat:
      return PrimitiveLiteral::Float(static_cast<float>(long_val));
    case TypeId::kDouble:
      return PrimitiveLiteral::Double(static_cast<double>(long_val));
    default:
      return NotImplemented("Cast from Long to {} is not implemented",
                            static_cast<int>(target_type_id));
  }
}

Result<PrimitiveLiteral> PrimitiveLiteral::CastFromFloat(TypeId target_type_id) const {
  auto float_val = std::get<float>(value_);

  switch (target_type_id) {
    case TypeId::kDouble:
      return PrimitiveLiteral::Double(static_cast<double>(float_val));
    default:
      return NotImplemented("Cast from Float to {} is not implemented",
                            static_cast<int>(target_type_id));
  }
}

// Three-way comparison operator
std::partial_ordering PrimitiveLiteral::operator<=>(const PrimitiveLiteral& other) const {
  // If types are different, comparison is unordered
  if (type_->type_id() != other.type_->type_id()) {
    return std::partial_ordering::unordered;
  }

  // If either value is AboveMax or BelowMin, comparison is unordered
  if (isAboveMax() || isBelowMin() || other.isAboveMax() || other.isBelowMin()) {
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

    case TypeId::kInt: {
      auto this_val = std::get<int32_t>(value_);
      auto other_val = std::get<int32_t>(other.value_);
      return this_val <=> other_val;
    }

    case TypeId::kLong: {
      auto this_val = std::get<int64_t>(value_);
      auto other_val = std::get<int64_t>(other.value_);
      return this_val <=> other_val;
    }

    case TypeId::kFloat: {
      auto this_val = std::get<float>(value_);
      auto other_val = std::get<float>(other.value_);
      // Use strong_ordering for floating point as spec requests
      return std::strong_order(this_val, other_val);
    }

    case TypeId::kDouble: {
      auto this_val = std::get<double>(value_);
      auto other_val = std::get<double>(other.value_);
      // Use strong_ordering for floating point as spec requests
      return std::strong_order(this_val, other_val);
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

std::string PrimitiveLiteral::ToString() const {
  if (std::holds_alternative<BelowMin>(value_)) {
    return "BelowMin";
  }
  if (std::holds_alternative<AboveMax>(value_)) {
    return "AboveMax";
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
        result += std::format("{:02X}", byte);
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

bool PrimitiveLiteral::isBelowMin() const {
  return std::holds_alternative<BelowMin>(value_);
}

bool PrimitiveLiteral::isAboveMax() const {
  return std::holds_alternative<AboveMax>(value_);
}
}  // namespace iceberg
