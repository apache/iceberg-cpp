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

#include <string>

#include "iceberg/iceberg_export.h"

namespace iceberg {

class ICEBERG_EXPORT TransformUtil {
 public:
  /// \brief Returns a human-readable string for a year.
  ///
  /// The string is formatted as "yyyy".
  ///
  /// \param year The year to format.
  /// \return A human-readable string for the year.
  static std::string HumanYear(int32_t year);

  /// \brief Returns a human-readable string for a month.
  ///
  /// The string is formatted as "yyyy-MM".
  ///
  /// \param month The month to format.
  /// \return A human-readable string for the month.
  static std::string HumanMonth(int32_t month);

  /// \brief Returns a human-readable string for the given day ordinal.
  ///
  /// The string is formatted as: `yyyy-MM-dd`.
  ///
  /// \param day_ordinal The day ordinal.
  /// \return A human-readable string for the given day ordinal.
  static std::string HumanDay(int32_t day_ordinal);

  /// \brief Returns a human-readable string for the given hour ordinal.
  ///
  /// The string is formatted as: `yyyy-MM-dd-HH`.
  ///
  /// \param hour_ordinal The hour ordinal.
  /// \return A human-readable string for the given hour ordinal.
  static std::string HumanHour(int32_t hour_ordinal);

  /// \brief Outputs this time as a String, such as 10:15.
  ///
  /// The output will be one of the following ISO-8601 formats:
  /// HH:mm
  /// HH:mm:ss
  /// HH:mm:ss.SSS
  /// HH:mm:ss.SSSSSS
  /// The format used will be the shortest that outputs the full value of the time where
  /// the omitted parts are implied to be zero.
  ///
  /// \param microseconds_from_midnight the time in microseconds from midnight
  /// \return a string representation of this time
  static std::string HumanTime(int64_t micros_from_midnight);

  /// \brief Returns a string representation of a timestamp in microseconds.
  ///
  /// The output will be one of the following forms, according to the precision of the
  /// timestamp:
  ///  - yyyy-MM-ddTHH:mm:ss
  ///  - yyyy-MM-ddTHH:mm:ss.SSS
  ///  - yyyy-MM-ddTHH:mm:ss.SSSSSS
  ///
  /// \param timestamp_micros the timestamp in microseconds.
  /// \return a string representation of this timestamp.
  static std::string HumanTimestamp(int64_t timestamp_micros);

  /// \brief Returns a human-readable string representation of a timestamp with a time
  /// zone.
  ///
  /// The output will be one of the following forms, according to the precision of the
  /// timestamp:
  ///  - yyyy-MM-ddTHH:mm:ss+00:00
  ///  - yyyy-MM-ddTHH:mm:ss.SSS+00:00
  ///  - yyyy-MM-ddTHH:mm:ss.SSSSSS+00:00
  ///
  /// \param timestamp_micros the timestamp in microseconds.
  /// \return a string representation of this timestamp.
  static std::string HumanTimestampWithZone(int64_t timestamp_micros);

  /// \brief Base64 encode a string
  static std::string Base64Encode(std::string_view str_to_encode);
};

}  // namespace iceberg
