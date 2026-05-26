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
#include <vector>

#include "iceberg/iceberg_data_export.h"

namespace iceberg {

class PositionDeleteIndex;

/// \brief Apply `positions` to `target` as deletes; semantically equivalent
/// to calling `target.Delete(pos)` for each entry. Out-of-range positions
/// are silently ignored. Sorted, mostly-contiguous input is fastest.
///
/// \param scratch Caller-owned reusable buffer for the bulk dispatch path.
///     Cleared and reused per key group; retain across calls to amortize
///     allocations. Pass a distinct `scratch` per thread when calling
///     concurrently with disjoint `target`.
void ICEBERG_DATA_EXPORT ForEachPositionDelete(std::span<const int64_t> positions,
                                               PositionDeleteIndex& target,
                                               std::vector<uint32_t>& scratch);

}  // namespace iceberg
