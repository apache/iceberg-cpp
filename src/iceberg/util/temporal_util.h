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

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

struct ICEBERG_EXPORT TemporalParts {
  int32_t year{0};
  uint16_t month{0};
  uint16_t day{0};
  int32_t hour{0};
  int32_t minute{0};
  int32_t second{0};
  int32_t microsecond{0};
  int32_t nanosecond{0};
  // e.g. -480 for PST (UTC-8:00), +480 for Asia/Shanghai (UTC+8:00)
  int32_t tz_offset_minutes;
};

class ICEBERG_EXPORT TemporalUtils {
 public:
  /// \brief Extract a date or timestamp year, as years from 1970
  static Result<Literal> ExtractYear(const Literal& literal);

  /// \brief Extract a date or timestamp month, as months from 1970-01-01
  static Result<Literal> ExtractMonth(const Literal& literal);

  /// \brief Extract a date or timestamp day, as days from 1970-01-01
  static Result<Literal> ExtractDay(const Literal& literal);

  /// \brief Extract a timestamp hour, as hours from 1970-01-01 00:00:00
  static Result<Literal> ExtractHour(const Literal& literal);

  /// \brief Construct a Calendar date without timezone or time
  static int32_t CreateDate(const TemporalParts& parts);

  /// \brief Construct a time-of-day, microsecond precision, without date, timezone
  static int64_t CreateTime(const TemporalParts& parts);

  /// \brief Construct a timestamp, microsecond precision, without timezone
  static int64_t CreateTimestamp(const TemporalParts& parts);

  /// \brief Construct a timestamp, microsecond precision, with timezone
  static int64_t CreateTimestampTz(const TemporalParts& parts);

  /// \brief Construct a timestamp, nanosecond precision, without timezone
  static int64_t CreateTimestampNanos(const TemporalParts& parts);

  /// \brief Construct a timestamp, nanosecond precision, with timezone
  static int64_t CreateTimestampTzNanos(const TemporalParts& parts);
};

}  // namespace iceberg
