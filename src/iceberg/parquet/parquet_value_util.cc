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

#include "iceberg/parquet/parquet_value_util_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/decimal.h"
#include "iceberg/util/uuid.h"

namespace iceberg::parquet {
namespace {

std::vector<uint8_t> BytesFromByteArray(const ::parquet::ByteArray& value) {
  return std::vector<uint8_t>{value.ptr, value.ptr + value.len};
}

std::vector<uint8_t> BytesFromFLBA(const ::parquet::FixedLenByteArray& value,
                                   int32_t length) {
  return std::vector<uint8_t>{value.ptr, value.ptr + length};
}

Literal DecimalLiteral(int128_t value, const PrimitiveType& iceberg_type) {
  const auto& decimal_type = internal::checked_cast<const DecimalType&>(iceberg_type);
  return Literal::Decimal(value, decimal_type.precision(), decimal_type.scale());
}

Result<Literal> DecimalLiteralFromBytes(std::span<const uint8_t> bytes,
                                        const PrimitiveType& iceberg_type) {
  ICEBERG_ASSIGN_OR_RAISE(auto decimal,
                          Decimal::FromBigEndian(bytes.data(), bytes.size()));
  return DecimalLiteral(decimal.value(), iceberg_type);
}

Result<Literal> BinaryLiteral(std::vector<uint8_t> bytes,
                              const PrimitiveType& iceberg_type) {
  switch (iceberg_type.type_id()) {
    case TypeId::kString:
      return Literal::String(std::string(bytes.begin(), bytes.end()));
    case TypeId::kBinary:
      return Literal::Binary(std::move(bytes));
    case TypeId::kFixed:
      return Literal::Fixed(std::move(bytes));
    case TypeId::kUuid: {
      ICEBERG_ASSIGN_OR_RAISE(auto uuid, Uuid::FromBytes(bytes));
      return Literal::UUID(std::move(uuid));
    }
    case TypeId::kDecimal:
      return DecimalLiteralFromBytes(bytes, iceberg_type);
    default:
      return InvalidArgument("Cannot convert Parquet binary value to Iceberg type {}",
                             iceberg_type.ToString());
  }
}

}  // namespace

Result<Literal> ParquetValueToLiteral(int32_t value, const PrimitiveType& iceberg_type) {
  switch (iceberg_type.type_id()) {
    case TypeId::kInt:
      return Literal::Int(value);
    case TypeId::kLong:
      return Literal::Long(value);
    case TypeId::kDate:
      return Literal::Date(value);
    case TypeId::kDecimal:
      return DecimalLiteral(value, iceberg_type);
    default:
      return InvalidArgument("Cannot convert Parquet INT32 value to Iceberg type {}",
                             iceberg_type.ToString());
  }
}

Result<Literal> ParquetValueToLiteral(int64_t value, const PrimitiveType& iceberg_type) {
  switch (iceberg_type.type_id()) {
    case TypeId::kLong:
      return Literal::Long(value);
    case TypeId::kTime:
      return Literal::Time(value);
    case TypeId::kTimestamp:
      return Literal::Timestamp(value);
    case TypeId::kTimestampTz:
      return Literal::TimestampTz(value);
    case TypeId::kTimestampNs:
      return Literal::TimestampNs(value);
    case TypeId::kTimestampTzNs:
      return Literal::TimestampTzNs(value);
    case TypeId::kDecimal:
      return DecimalLiteral(value, iceberg_type);
    default:
      return InvalidArgument("Cannot convert Parquet INT64 value to Iceberg type {}",
                             iceberg_type.ToString());
  }
}

Result<Literal> ParquetValueToLiteral(float value, const PrimitiveType& iceberg_type) {
  switch (iceberg_type.type_id()) {
    case TypeId::kFloat:
      return Literal::Float(value);
    case TypeId::kDouble:
      return Literal::Double(value);
    default:
      return InvalidArgument("Cannot convert Parquet FLOAT value to Iceberg type {}",
                             iceberg_type.ToString());
  }
}

Result<Literal> ParquetValueToLiteral(bool value, const PrimitiveType&) {
  return Literal::Boolean(value);
}

Result<Literal> ParquetValueToLiteral(double value, const PrimitiveType&) {
  return Literal::Double(value);
}

Result<Literal> ParquetValueToLiteral(const ::parquet::ByteArray& value,
                                      const PrimitiveType& iceberg_type) {
  return BinaryLiteral(BytesFromByteArray(value), iceberg_type);
}

Result<Literal> ParquetValueToLiteral(const ::parquet::FixedLenByteArray& value,
                                      const PrimitiveType& iceberg_type, int32_t length) {
  return BinaryLiteral(BytesFromFLBA(value, length), iceberg_type);
}

}  // namespace iceberg::parquet
