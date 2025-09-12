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
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Convert microseconds since epoch to days since epoch
/// \param micros_since_epoch Microseconds since Unix epoch
/// \return Days since Unix epoch (1970-01-01)
ICEBERG_EXPORT int32_t MicrosToDays(int64_t micros_since_epoch);

/// \brief Cross-platform implementation of timegm function
/// \param tm Time structure to convert
/// \return Time as seconds since Unix epoch
ICEBERG_EXPORT time_t TimegmCustom(std::tm* tm);

/// \brief Parse a date string in YYYY-MM-DD format
/// \param date_str Date string to parse
/// \return Days since Unix epoch (1970-01-01) on success
ICEBERG_EXPORT Result<int32_t> ParseDateString(const std::string& date_str);

/// \brief Parse a time string in HH:MM:SS.ffffff format
/// \param time_str Time string to parse
/// \return Microseconds since midnight on success
ICEBERG_EXPORT Result<int64_t> ParseTimeString(const std::string& time_str);

/// \brief Parse a timestamp string in YYYY-MM-DDTHH:MM:SS.ffffff format
/// \param timestamp_str Timestamp string to parse
/// \return Microseconds since Unix epoch on success
ICEBERG_EXPORT Result<int64_t> ParseTimestampString(const std::string& timestamp_str);

/// \brief Parse a timestamp with timezone string in YYYY-MM-DDTHH:MM:SS.ffffffZ format
/// \param timestamptz_str Timestamp with timezone string to parse
/// \return Microseconds since Unix epoch on success
///
/// \note This implementation only supports UTC designator 'Z', not timezone offsets
ICEBERG_EXPORT Result<int64_t> ParseTimestampTzString(const std::string& timestamptz_str);

/// \brief Parse fractional seconds from a string
/// \param fractional_str Fractional seconds string (up to 6 digits)
/// \return Microseconds value of the fractional part
ICEBERG_EXPORT Result<int64_t> ParseFractionalSeconds(const std::string& fractional_str);

}  // namespace iceberg
