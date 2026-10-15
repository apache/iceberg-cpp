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

#include <cstddef>
#include <cstdint>

#include "iceberg/iceberg_export.h"

/// \file iceberg/util/bucket_util_internal.h
/// \brief Internal entry points for the bucket transform.

namespace iceberg::internal {

/// \brief Hash `len` bytes starting at `data` using MurmurHash3.
///
/// The raw pointer and length are taken separately so that a length that does not
/// fit the pointee object can be passed without forming an invalid `std::span`.
/// `BucketUtils::HashBytes` forwards to this function.
///
/// \param data Pointer to the first byte to hash. It is only dereferenced once `len`
///     is accepted, so it may be null when the call is expected to be rejected.
/// \param len The number of bytes to hash.
/// \return A 32-bit hash value.
/// \throws IcebergError if `len` exceeds `INT32_MAX`; see `BucketUtils::HashBytes`.
ICEBERG_EXPORT int32_t HashBytes(const uint8_t* data, size_t len);

}  // namespace iceberg::internal
