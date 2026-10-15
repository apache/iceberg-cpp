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
#include <optional>
#include <utility>

#include "iceberg/exception.h"
#include "iceberg/expression/literal.h"
#include "iceberg/util/murmurhash3_internal.h"
#include "iceberg/util/temporal_util.h"

namespace iceberg {

namespace {

// Keep bucket inputs compatible with Java's signed 32-bit array lengths.
// The underlying MurmurHash3 implementation accepts size_t lengths.
constexpr size_t kMaxHashBytes = static_cast<size_t>(std::numeric_limits<int32_t>::max());

/// \brief Returns the number of bytes `HashLiteral` will hash for `literal`.
///
/// Returns `std::nullopt` for values hashed as fixed-width integers, which have no
/// length limit.
std::optional<size_t> HashedByteSize(const Literal& literal) {
  switch (literal.type()->type_id()) {
    case TypeId::kString:
      return std::get<std::string>(literal.value()).size();
    case TypeId::kBinary:
    case TypeId::kFixed:
      return std::get<std::vector<uint8_t>>(literal.value()).size();
    default:
      return std::nullopt;
  }
}

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
  ICEBERG_CHECK_OR_DIE(
      bytes.size() <= kMaxHashBytes,
      "BucketUtils::HashBytes: input length {} exceeds the maximum supported "
      "length {}",
      bytes.size(), kMaxHashBytes);
  return static_cast<int32_t>(MurmurHash3_x86_32(bytes.data(), bytes.size(), 0));
}

int32_t BucketUtils::HashLong(int64_t value) {
  return static_cast<int32_t>(MurmurHash3_x86_32_Long(static_cast<uint64_t>(value), 0));
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

  // HashBytes rejects oversized inputs by throwing, but this function reports failures
  // through its Result, so check the length up front.
  if (auto byte_size = HashedByteSize(literal);
      byte_size.has_value() && *byte_size > kMaxHashBytes) [[unlikely]] {
    return InvalidArgument(
        "Cannot compute bucket index for {}: value of {} bytes exceeds the maximum "
        "supported length {}",
        literal.type()->ToString(), *byte_size, kMaxHashBytes);
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
