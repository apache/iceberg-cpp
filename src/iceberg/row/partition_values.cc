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

#include "iceberg/row/partition_values.h"

namespace iceberg {

PartitionValues& PartitionValues::operator=(const PartitionValues& other) {
  if (this != &other) {
    values_ = other.values_;
  }
  return *this;
}

bool PartitionValues::operator==(const PartitionValues& other) const {
  return values_ == other.values_;
}

Result<Scalar> PartitionValues::GetField(size_t pos) const {
  if (pos >= values_.size()) {
    return InvalidArgument(
        "Position {} is out of bounds for PartitionValues with {} fields", pos,
        values_.size());
  }

  const auto& literal = values_[pos];

  // Handle null values
  if (literal.IsNull()) {
    return Scalar{std::monostate{}};
  }

  // Convert Literal to Scalar based on type
  switch (literal.type()->type_id()) {
    case TypeId::kBoolean:
      return Scalar{std::get<bool>(literal.value())};
    case TypeId::kInt:
    case TypeId::kDate:
      return Scalar{std::get<int32_t>(literal.value())};
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      return Scalar{std::get<int64_t>(literal.value())};
    case TypeId::kFloat:
      return Scalar{std::get<float>(literal.value())};
    case TypeId::kDouble:
      return Scalar{std::get<double>(literal.value())};
    case TypeId::kString: {
      const auto& str = std::get<std::string>(literal.value());
      return Scalar{std::string_view(str)};
    }
    case TypeId::kBinary:
    case TypeId::kFixed: {
      const auto& bytes = std::get<std::vector<uint8_t>>(literal.value());
      return Scalar{
          std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size())};
    }
    case TypeId::kDecimal:
      return Scalar{std::get<Decimal>(literal.value())};
    default:
      return NotSupported("Cannot convert literal of type {} to Scalar",
                          literal.type()->ToString());
  }
}

Result<std::reference_wrapper<const Literal>> PartitionValues::ValueAt(size_t pos) const {
  if (pos >= values_.size()) {
    return InvalidArgument("Cannot get partition value at {} from {} fields", pos,
                           values_.size());
  }
  return std::cref(values_[pos]);
}

}  // namespace iceberg
