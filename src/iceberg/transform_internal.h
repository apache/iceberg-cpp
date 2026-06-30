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

#include "iceberg/result.h"

namespace iceberg::internal {

inline Status ValidateBucketTransformParameter(int32_t num_buckets) {
  if (num_buckets <= 0) {
    return InvalidArgument("Number of buckets must be positive, got {}", num_buckets);
  }
  return {};
}

inline Status ValidateTruncateTransformParameter(int32_t width) {
  if (width <= 0) {
    return InvalidArgument("Width must be positive, got {}", width);
  }
  return {};
}

}  // namespace iceberg::internal
