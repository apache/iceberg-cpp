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

#pragma once

#include <cstdint>
#include <span>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class ICEBERG_EXPORT BucketUtils {
 public:
  /// \brief Hash a 32-bit integer using MurmurHash3 and return a 32-bit hash value.
  /// \param value The input integer to hash.
  /// \note Integer and long hash results must be identical for all integer values. This
  /// ensures that schema evolution does not change bucket partition values if integer
  /// types are promoted.
  /// \return A 32-bit hash value.
  static inline int32_t HashInt(int32_t value) {
    return HashLong(static_cast<int64_t>(value));
  }

  /// \brief Hash a 64-bit integer using MurmurHash3 and return a 32-bit hash value.
  /// \param value The input long to hash.
  /// \return A 32-bit hash value.
  static int32_t HashLong(int64_t value);

  /// \brief Hash a byte array using MurmurHash3 and return a 32-bit hash value.
  /// \param bytes The input byte array to hash.
  /// \return A 32-bit hash value.
  static int32_t HashBytes(std::span<const uint8_t> bytes);

  /// \brief Compute the bucket index for a given literal and number of buckets.
  /// \param literal The input literal to hash.
  /// \param num_buckets The number of buckets to hash into.
  /// \return (murmur3_x86_32_hash(literal) & Integer.MAX_VALUE) % num_buckets
  static Result<int32_t> BucketIndex(const Literal& literal, int32_t num_buckets);
};

}  // namespace iceberg
