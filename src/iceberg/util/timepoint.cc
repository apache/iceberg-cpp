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

#include "iceberg/util/timepoint.h"

#include <chrono>

namespace iceberg {

Result<TimePointMs> TimePointMsFromUnixMs(int64_t unix_ms) {
  return TimePointMs{std::chrono::milliseconds(unix_ms)};
}

int64_t UnixMsFromTimePointMs(const TimePointMs& time_point_ms) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             time_point_ms.time_since_epoch())
      .count();
}

Result<TimePointNs> TimePointNsFromUnixNs(int64_t unix_ns) {
  return TimePointNs{std::chrono::nanoseconds(unix_ns)};
}

int64_t UnixNsFromTimePointNs(const TimePointNs& time_point_ns) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             time_point_ns.time_since_epoch())
      .count();
}

}  // namespace iceberg
