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

/// \file iceberg/util/literal_format.h
/// \brief Format literal values as strings

namespace iceberg {

/// \brief Format a date value as a string.
ICEBERG_EXPORT std::string FormatDate(int32_t days_since_epoch);

/// \brief Format a time value as a string.
ICEBERG_EXPORT std::string FormatTime(int64_t microseconds_since_midnight);

/// \brief Format a timestamp value as a string.
ICEBERG_EXPORT std::string FormatTimestamp(int64_t microseconds_since_epoch);

/// \brief Format a timestamp with timezone value as a string.
ICEBERG_EXPORT std::string FormatTimestampTz(int64_t microseconds_since_epoch);

}  // namespace iceberg
