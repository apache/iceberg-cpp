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

#include "iceberg/util/bucket_util.h"

#include <limits>
#include <utility>

#include "iceberg/exception.h"
#include "iceberg/expression/literal.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/murmurhash3_internal.h"
#include "iceberg/util/temporal_util.h"

namespace iceberg {

namespace {
template <TypeId type_id>
int32_t HashLiteral(const Literal& literal) {
  std::unreachable();
}

template <>
int32_t HashLiteral<TypeId::kInt>(const Literal& literal) {
  return BucketUtils::HashInt(std::get<int32_t>(literal.value()));
}

template <>
int32_t HashLiteral<TypeId::kDate>(const Literal& literal) {
  return BucketUtils::HashInt(std::get<int32_t>(literal.value()));
}

template <>
int32_t HashLiteral<TypeId::kLong>(const Literal& literal) {
  return BucketUtils::HashLong(std::get<int64_t>(literal.value()));
}

template <>
int32_t HashLiteral<TypeId::kTime>(const Literal& literal) {
  return BucketUtils::HashLong(std::get<int64_t>(literal.value()));
}

template <>
int32_t HashLiteral<TypeId::kTimestamp>(const Literal& literal) {
  return BucketUtils::HashLong(std::get<int64_t>(literal.value()));
}

template <>
int32_t HashLiteral<TypeId::kTimestampTz>(const Literal& literal) {
  return BucketUtils::HashLong(std::get<int64_t>(literal.value()));
}

template <>
int32_t HashLiteral<TypeId::kTimestampNs>(const Literal& literal) {
  return BucketUtils::HashLong(
      TemporalUtils::NanosToMicros(std::get<int64_t>(literal.value())));
}

template <>
int32_t HashLiteral<TypeId::kTimestampTzNs>(const Literal& literal) {
  return BucketUtils::HashLong(
      TemporalUtils::NanosToMicros(std::get<int64_t>(literal.value())));
}

template <>
int32_t HashLiteral<TypeId::kDecimal>(const Literal& literal) {
  const auto& decimal = std::get<Decimal>(literal.value());
  return BucketUtils::HashBytes(decimal.ToBigEndian());
}

template <>
int32_t HashLiteral<TypeId::kString>(const Literal& literal) {
  const auto& str = std::get<std::string>(literal.value());
  return BucketUtils::HashBytes(
      std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(str.data()), str.size()));
}

template <>
int32_t HashLiteral<TypeId::kUuid>(const Literal& literal) {
  const auto& uuid = std::get<Uuid>(literal.value());
  return BucketUtils::HashBytes(uuid.bytes());
}

template <>
int32_t HashLiteral<TypeId::kBinary>(const Literal& literal) {
  const auto& binary = std::get<std::vector<uint8_t>>(literal.value());
  return BucketUtils::HashBytes(binary);
}

template <>
int32_t HashLiteral<TypeId::kFixed>(const Literal& literal) {
  const auto& fixed = std::get<std::vector<uint8_t>>(literal.value());
  return BucketUtils::HashBytes(fixed);
}

}  // namespace

int32_t BucketUtils::HashBytes(std::span<const uint8_t> bytes) {
  // MurmurHash3_x86_32 takes a signed 32-bit length. Passing a larger size_t
  // would wrap it negative and make the hash loop read far outside `bytes`
  // (and silently hash the wrong prefix for even larger inputs). Iceberg's
  // bucket transform is defined on byte sequences of at most 2^31 - 1 bytes
  // (Java arrays), so reject anything longer before narrowing.
  ICEBERG_CHECK_OR_DIE(
      bytes.size() <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
      "BucketUtils::HashBytes: input length {} exceeds the maximum supported "
      "length {}",
      bytes.size(), std::numeric_limits<int32_t>::max());
  int32_t hash_value = 0;
  MurmurHash3_x86_32(bytes.data(), static_cast<int>(bytes.size()), 0, &hash_value);
  return hash_value;
}

int32_t BucketUtils::HashLong(int64_t value) {
  int32_t hash_value = 0;
  value = ToLittleEndian(value);
  MurmurHash3_x86_32(&value, sizeof(int64_t), 0, &hash_value);
  return hash_value;
}

#define DISPATCH_HASH_LITERAL(TYPE_ID)          \
  case TYPE_ID:                                 \
    hash_value = HashLiteral<TYPE_ID>(literal); \
    break;

Result<int32_t> BucketUtils::BucketIndex(const Literal& literal, int32_t num_buckets) {
  if (num_buckets <= 0) [[unlikely]] {
    return InvalidArgument("Number of buckets must be positive, got {}", num_buckets);
  }

  if (literal.IsAboveMax() || literal.IsBelowMin()) [[unlikely]] {
    return NotSupported("Cannot compute bucket index for {}", literal.ToString());
  }

  int32_t hash_value = 0;
  switch (literal.type()->type_id()) {
    DISPATCH_HASH_LITERAL(TypeId::kInt)
    DISPATCH_HASH_LITERAL(TypeId::kDate)
    DISPATCH_HASH_LITERAL(TypeId::kLong)
    DISPATCH_HASH_LITERAL(TypeId::kTime)
    DISPATCH_HASH_LITERAL(TypeId::kTimestamp)
    DISPATCH_HASH_LITERAL(TypeId::kTimestampTz)
    DISPATCH_HASH_LITERAL(TypeId::kTimestampNs)
    DISPATCH_HASH_LITERAL(TypeId::kTimestampTzNs)
    DISPATCH_HASH_LITERAL(TypeId::kDecimal)
    DISPATCH_HASH_LITERAL(TypeId::kString)
    DISPATCH_HASH_LITERAL(TypeId::kUuid)
    DISPATCH_HASH_LITERAL(TypeId::kBinary)
    DISPATCH_HASH_LITERAL(TypeId::kFixed)
    default:
      return NotSupported("Hashing not supported for type {}",
                          literal.type()->ToString());
  }

  return (hash_value & std::numeric_limits<int32_t>::max()) % num_buckets;
}

}  // namespace iceberg
