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

#include "iceberg/datum.h"

#include <sstream>

#include "iceberg/exception.h"

namespace iceberg {

// Constructor
PrimitiveLiteral::PrimitiveLiteral(PrimitiveLiteralValue value,
                                   std::shared_ptr<PrimitiveType> type)
    : value_(std::move(value)), type_(std::move(type)) {}

// Factory methods
PrimitiveLiteral PrimitiveLiteral::Boolean(bool value) {
  return PrimitiveLiteral(value, std::make_shared<BooleanType>());
}

PrimitiveLiteral PrimitiveLiteral::Integer(int32_t value) {
  return PrimitiveLiteral(value, std::make_shared<IntType>());
}

PrimitiveLiteral PrimitiveLiteral::Long(int64_t value) {
  return PrimitiveLiteral(value, std::make_shared<LongType>());
}

PrimitiveLiteral PrimitiveLiteral::Float(float value) {
  return PrimitiveLiteral(value, std::make_shared<FloatType>());
}

PrimitiveLiteral PrimitiveLiteral::Double(double value) {
  return PrimitiveLiteral(value, std::make_shared<DoubleType>());
}

PrimitiveLiteral PrimitiveLiteral::String(std::string value) {
  return PrimitiveLiteral(std::move(value), std::make_shared<StringType>());
}

PrimitiveLiteral PrimitiveLiteral::Binary(std::vector<uint8_t> value) {
  return PrimitiveLiteral(std::move(value), std::make_shared<BinaryType>());
}

Result<PrimitiveLiteral> PrimitiveLiteral::Deserialize(std::span<const uint8_t> data) {
  return NotImplemented("Deserialization of PrimitiveLiteral is not implemented yet");
}

Result<std::vector<uint8_t>> PrimitiveLiteral::Serialize() const {
  return NotImplemented("Serialization of PrimitiveLiteral is not implemented yet");
}

// Getters
const PrimitiveLiteralValue& PrimitiveLiteral::value() const { return value_; }

const std::shared_ptr<PrimitiveType>& PrimitiveLiteral::type() const { return type_; }

// Cast method
Result<PrimitiveLiteral> PrimitiveLiteral::CastTo(
    const std::shared_ptr<PrimitiveType>& target_type) const {
  if (*type_ == *target_type) {
    // If types are the same, return a copy of the current literal
    return PrimitiveLiteral(value_, target_type);
  }

  return NotImplemented("Cast from {} to {} is not implemented", type_->ToString(),
                        target_type->ToString());
}

// Three-way comparison operator
std::partial_ordering PrimitiveLiteral::operator<=>(const PrimitiveLiteral& other) const {
  // If types are different, comparison is unordered
  if (type_->type_id() != other.type_->type_id()) {
    return std::partial_ordering::unordered;
  }
  if (value_ == other.value_) {
    return std::partial_ordering::equivalent;
  }
  throw IcebergError("Not implemented: comparison between different primitive types");
}

std::string PrimitiveLiteral::ToString() const {
  throw NotImplemented("ToString for PrimitiveLiteral is not implemented yet");
}

}  // namespace iceberg
