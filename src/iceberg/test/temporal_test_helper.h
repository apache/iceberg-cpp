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
#include <cstdint>

namespace iceberg {

using namespace std::chrono;  // NOLINT

struct DateParts {
  int32_t year{0};
  uint8_t month{0};
  uint8_t day{0};
};

struct TimeParts {
  int32_t hour{0};
  int32_t minute{0};
  int32_t second{0};
  int32_t microsecond{0};
};

struct TimestampParts {
  int32_t year{0};
  uint8_t month{0};
  uint8_t day{0};
  int32_t hour{0};
  int32_t minute{0};
  int32_t second{0};
  int32_t microsecond{0};
  // e.g. -480 for PST (UTC-8:00), +480 for Asia/Shanghai (UTC+8:00)
  int32_t tz_offset_minutes{0};
};

struct TimestampNanosParts {
  int32_t year{0};
  uint8_t month{0};
  uint8_t day{0};
  int32_t hour{0};
  int32_t minute{0};
  int32_t second{0};
  int32_t nanosecond{0};
  // e.g. -480 for PST (UTC-8:00), +480 for Asia/Shanghai (UTC+8:00)
  int32_t tz_offset_minutes{0};
};

class TemporalTestHelper {
  static constexpr auto kEpochDays = sys_days(year{1970} / January / 1);

 public:
  /// \brief Construct a Calendar date without timezone or time
  static int32_t CreateDate(const DateParts& parts) {
    return static_cast<int32_t>(
        (sys_days(year{parts.year} / month{parts.month} / day{parts.day}) - kEpochDays)
            .count());
  }

  /// \brief Construct a time-of-day, microsecond precision, without date, timezone
  static int64_t CreateTime(const TimeParts& parts) {
    return duration_cast<microseconds>(hours(parts.hour) + minutes(parts.minute) +
                                       seconds(parts.second) +
                                       microseconds(parts.microsecond))
        .count();
  }

  /// \brief Construct a timestamp, microsecond precision, without timezone
  static int64_t CreateTimestamp(const TimestampParts& parts) {
    year_month_day ymd{year{parts.year}, month{parts.month}, day{parts.day}};
    auto tp = sys_time<microseconds>{(sys_days(ymd) + hours{parts.hour} +
                                      minutes{parts.minute} + seconds{parts.second} +
                                      microseconds{parts.microsecond})
                                         .time_since_epoch()};
    return tp.time_since_epoch().count();
  }

  /// \brief Construct a timestamp, microsecond precision, with timezone
  static int64_t CreateTimestampTz(const TimestampParts& parts) {
    year_month_day ymd{year{parts.year}, month{parts.month}, day{parts.day}};
    auto tp = sys_time<microseconds>{(sys_days(ymd) + hours{parts.hour} +
                                      minutes{parts.minute} + seconds{parts.second} +
                                      microseconds{parts.microsecond} -
                                      minutes{parts.tz_offset_minutes})
                                         .time_since_epoch()};
    return tp.time_since_epoch().count();
  }

  /// \brief Construct a timestamp, nanosecond precision, without timezone
  static int64_t CreateTimestampNanos(const TimestampNanosParts& parts) {
    year_month_day ymd{year{parts.year}, month{parts.month}, day{parts.day}};
    auto tp =
        sys_time<nanoseconds>{(sys_days(ymd) + hours{parts.hour} + minutes{parts.minute} +
                               seconds{parts.second} + nanoseconds{parts.nanosecond})
                                  .time_since_epoch()};
    return tp.time_since_epoch().count();
  }

  /// \brief Construct a timestamp, nanosecond precision, with timezone
  static int64_t CreateTimestampTzNanos(const TimestampNanosParts& parts) {
    year_month_day ymd{year{parts.year}, month{parts.month}, day{parts.day}};
    auto tp =
        sys_time<nanoseconds>{(sys_days(ymd) + hours{parts.hour} + minutes{parts.minute} +
                               seconds{parts.second} + nanoseconds{parts.nanosecond} -
                               minutes{parts.tz_offset_minutes})
                                  .time_since_epoch()};
    return tp.time_since_epoch().count();
  }
};

}  // namespace iceberg
