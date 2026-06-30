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

#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/extension_type.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/literal_util_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

namespace iceberg::arrow {

namespace {

Result<std::shared_ptr<::arrow::DataType>> ToArrowType(const PrimitiveType& type) {
  switch (type.type_id()) {
    case TypeId::kBoolean:
      return ::arrow::boolean();
    case TypeId::kInt:
      return ::arrow::int32();
    case TypeId::kLong:
      return ::arrow::int64();
    case TypeId::kFloat:
      return ::arrow::float32();
    case TypeId::kDouble:
      return ::arrow::float64();
    case TypeId::kDecimal: {
      const auto& decimal_type = internal::checked_cast<const DecimalType&>(type);
      return ::arrow::decimal128(decimal_type.precision(), decimal_type.scale());
    }
    case TypeId::kDate:
      return ::arrow::date32();
    case TypeId::kTime:
      return ::arrow::time64(::arrow::TimeUnit::MICRO);
    case TypeId::kTimestamp:
      return ::arrow::timestamp(::arrow::TimeUnit::MICRO);
    case TypeId::kTimestampTz:
      return ::arrow::timestamp(::arrow::TimeUnit::MICRO, "UTC");
    case TypeId::kTimestampNs:
      return ::arrow::timestamp(::arrow::TimeUnit::NANO);
    case TypeId::kTimestampTzNs:
      return ::arrow::timestamp(::arrow::TimeUnit::NANO, "UTC");
    case TypeId::kString:
      return ::arrow::utf8();
    case TypeId::kBinary:
      return ::arrow::binary();
    case TypeId::kFixed: {
      const auto& fixed_type = internal::checked_cast<const FixedType&>(type);
      return ::arrow::fixed_size_binary(static_cast<int32_t>(fixed_type.length()));
    }
    case TypeId::kUuid:
      return ::arrow::fixed_size_binary(16);
    default:
      return NotSupported("Cannot convert {} to an Arrow type", type);
  }
}

Result<std::shared_ptr<::arrow::Buffer>> ToArrowBuffer(const std::vector<uint8_t>& bytes,
                                                       ::arrow::MemoryPool* pool) {
  ICEBERG_ARROW_ASSIGN_OR_RETURN(std::unique_ptr<::arrow::Buffer> buffer,
                                 ::arrow::AllocateBuffer(bytes.size(), pool));
  std::memcpy(buffer->mutable_data(), bytes.data(), bytes.size());
  return std::shared_ptr<::arrow::Buffer>(std::move(buffer));
}

}  // namespace

Result<std::shared_ptr<::arrow::Scalar>> ToArrowScalar(const Literal& literal,
                                                       ::arrow::MemoryPool* pool) {
  if (literal.type() == nullptr) {
    return InvalidArgument("Cannot convert a literal without type to an Arrow scalar");
  }

  if (literal.IsAboveMax() || literal.IsBelowMin()) {
    return NotSupported("Cannot convert {} to an Arrow scalar", literal);
  }

  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<::arrow::DataType> arrow_type,
                          ToArrowType(*literal.type()));
  if (literal.IsNull()) {
    return ::arrow::MakeNullScalar(std::move(arrow_type));
  }

  const Literal::Value& value = literal.value();
  switch (literal.type()->type_id()) {
    case TypeId::kBoolean:
      return std::make_shared<::arrow::BooleanScalar>(std::get<bool>(value));
    case TypeId::kInt:
      return std::make_shared<::arrow::Int32Scalar>(std::get<int32_t>(value));
    case TypeId::kLong:
      return std::make_shared<::arrow::Int64Scalar>(std::get<int64_t>(value));
    case TypeId::kFloat:
      return std::make_shared<::arrow::FloatScalar>(std::get<float>(value));
    case TypeId::kDouble:
      return std::make_shared<::arrow::DoubleScalar>(std::get<double>(value));
    case TypeId::kDecimal: {
      const auto& decimal = std::get<Decimal>(value);
      ::arrow::Decimal128 arrow_decimal(
          static_cast<int64_t>(decimal.value() >> 64),
          static_cast<uint64_t>(decimal.value() & ~uint64_t{0}));
      return std::make_shared<::arrow::Decimal128Scalar>(arrow_decimal,
                                                         std::move(arrow_type));
    }
    case TypeId::kDate:
      return std::make_shared<::arrow::Date32Scalar>(std::get<int32_t>(value));
    case TypeId::kTime:
      return std::make_shared<::arrow::Time64Scalar>(std::get<int64_t>(value),
                                                     std::move(arrow_type));
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs:
      return std::make_shared<::arrow::TimestampScalar>(std::get<int64_t>(value),
                                                        std::move(arrow_type));
    case TypeId::kString:
      return std::make_shared<::arrow::StringScalar>(std::get<std::string>(value));
    case TypeId::kBinary: {
      ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<::arrow::Buffer> buffer,
                              ToArrowBuffer(std::get<std::vector<uint8_t>>(value), pool));
      return std::make_shared<::arrow::BinaryScalar>(std::move(buffer));
    }
    case TypeId::kFixed: {
      ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<::arrow::Buffer> buffer,
                              ToArrowBuffer(std::get<std::vector<uint8_t>>(value), pool));
      return std::make_shared<::arrow::FixedSizeBinaryScalar>(std::move(buffer),
                                                              std::move(arrow_type));
    }
    case TypeId::kUuid: {
      const Uuid& uuid = std::get<Uuid>(value);
      ICEBERG_ASSIGN_OR_RAISE(
          std::shared_ptr<::arrow::Buffer> buffer,
          ToArrowBuffer(std::vector<uint8_t>(uuid.bytes().begin(), uuid.bytes().end()),
                        pool));
      return std::make_shared<::arrow::FixedSizeBinaryScalar>(std::move(buffer),
                                                              std::move(arrow_type));
    }
    default:
      return NotSupported("Cannot convert {} literal to an Arrow scalar",
                          *literal.type());
  }
}

Result<std::shared_ptr<::arrow::Array>> MakeDefaultArray(
    const Literal& literal, const std::shared_ptr<::arrow::DataType>& type,
    int64_t num_rows, ::arrow::MemoryPool* pool) {
  // An extension type (e.g. `arrow.uuid` for an Iceberg UUID) is backed by a storage
  // type, and compute::Cast has no kernel that casts a storage array into an extension
  // type. Materialize the array as the storage type and wrap it in the extension type.
  if (type->id() == ::arrow::Type::EXTENSION) {
    const auto& extension_type =
        internal::checked_cast<const ::arrow::ExtensionType&>(*type);
    ICEBERG_ASSIGN_OR_RAISE(
        std::shared_ptr<::arrow::Array> storage,
        MakeDefaultArray(literal, extension_type.storage_type(), num_rows, pool));
    return ::arrow::ExtensionType::WrapArray(type, storage);
  }

  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<::arrow::Scalar> scalar,
                          ToArrowScalar(literal, pool));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(std::shared_ptr<::arrow::Array> array,
                                 ::arrow::MakeArrayFromScalar(*scalar, num_rows, pool));
  if (!array->type()->Equals(*type)) {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(::arrow::Datum cast_result,
                                   ::arrow::compute::Cast(array, type));
    return cast_result.make_array();
  }
  return array;
}

}  // namespace iceberg::arrow
