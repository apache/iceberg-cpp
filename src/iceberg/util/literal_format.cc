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

#include "iceberg/util/literal_format.h"

#include <chrono>
#include <cstring>
#include <iomanip>

namespace iceberg {

std::string FormatDate(int32_t days_since_epoch) {
  // Convert days since Unix epoch to date
  auto time_point =
      std::chrono::system_clock::time_point{} + std::chrono::days{days_since_epoch};
  auto date = std::chrono::floor<std::chrono::days>(time_point);
  auto ymd = std::chrono::year_month_day{date};

  std::ostringstream oss;
  oss << static_cast<int>(ymd.year()) << "-" << std::setfill('0') << std::setw(2)
      << static_cast<unsigned>(ymd.month()) << "-" << std::setfill('0') << std::setw(2)
      << static_cast<unsigned>(ymd.day());
  return oss.str();
}

std::string FormatTime(int64_t microseconds_since_midnight) {
  auto hours = microseconds_since_midnight / (1000000LL * 3600);
  auto minutes = (microseconds_since_midnight % (1000000LL * 3600)) / (1000000LL * 60);
  auto seconds = (microseconds_since_midnight % (1000000LL * 60)) / 1000000LL;
  auto micros = microseconds_since_midnight % 1000000LL;

  std::ostringstream oss;
  oss << std::setfill('0') << std::setw(2) << hours << ":" << std::setfill('0')
      << std::setw(2) << minutes << ":" << std::setfill('0') << std::setw(2) << seconds
      << "." << std::setfill('0') << std::setw(6) << micros;
  return oss.str();
}

std::string FormatTimestamp(int64_t microseconds_since_epoch) {
  auto time_point = std::chrono::system_clock::time_point{} +
                    std::chrono::microseconds{microseconds_since_epoch};
  auto date = std::chrono::floor<std::chrono::days>(time_point);
  auto time_of_day = time_point - date;
  auto micros_of_day =
      std::chrono::duration_cast<std::chrono::microseconds>(time_of_day).count();

  auto ymd = std::chrono::year_month_day{date};

  std::ostringstream oss;
  oss << static_cast<int>(ymd.year()) << "-" << std::setfill('0') << std::setw(2)
      << static_cast<unsigned>(ymd.month()) << "-" << std::setfill('0') << std::setw(2)
      << static_cast<unsigned>(ymd.day()) << "T" << FormatTime(micros_of_day);
  return oss.str();
}

std::string FormatTimestampTz(int64_t microseconds_since_epoch) {
  return FormatTimestamp(microseconds_since_epoch) + "+00:00";
}

}  // namespace iceberg
