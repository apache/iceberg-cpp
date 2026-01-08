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

#include <chrono>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief A time point in milliseconds
using TimePointMs =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;

/// \brief A time point in nanoseconds
using TimePointNs =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

/// \brief Returns a TimePointMs from a Unix timestamp in milliseconds
ICEBERG_EXPORT Result<TimePointMs> TimePointMsFromUnixMs(int64_t unix_ms);

/// \brief Returns a Unix timestamp in milliseconds from a TimePointMs
ICEBERG_EXPORT int64_t UnixMsFromTimePointMs(TimePointMs time_point_ms);

/// \brief Returns a TimePointNs from a Unix timestamp in nanoseconds
ICEBERG_EXPORT Result<TimePointNs> TimePointNsFromUnixNs(int64_t unix_ns);

/// \brief Returns a Unix timestamp in nanoseconds from a TimePointNs
ICEBERG_EXPORT int64_t UnixNsFromTimePointNs(TimePointNs time_point_ns);

/// \brief Returns a human-readable string representation of a TimePointMs
ICEBERG_EXPORT std::string FormatTimePointMs(TimePointMs time_point_ms);

/// \brief Returns a time point in milliseconds that represents the current system time
ICEBERG_EXPORT TimePointMs CurrentTimePointMs();

}  // namespace iceberg
