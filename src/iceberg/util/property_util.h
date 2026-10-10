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

/// \file iceberg/util/property_util.h
/// \brief Provide property conversion helpers.

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

class ICEBERG_EXPORT PropertyUtil {
 public:
  static Status ValidateCommitProperties(
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Read a boolean property from a property map.
  ///
  /// Mirrors Java's PropertyUtil.propertyAsBoolean: the value is parsed with
  /// StringUtils::ParseBoolean, so anything that is not "true" ignoring case reads as
  /// false rather than being rejected.
  ///
  /// \param properties The property map to read from.
  /// \param key The property key.
  /// \param default_value Returned when the property is absent.
  /// \return The parsed value, or default_value if the property is absent.
  static bool PropertyAsBoolean(
      const std::unordered_map<std::string, std::string>& properties,
      std::string_view key, bool default_value);

  /// \brief Read a boolean property that may be unset.
  ///
  /// Like PropertyAsBoolean, but returns std::nullopt when the property is absent so
  /// callers can distinguish an unset property from an explicit "false". Mirrors Java's
  /// PropertyUtil.propertyAsNullableBoolean.
  ///
  /// \param properties The property map to read from.
  /// \param key The property key.
  /// \return The parsed value, or std::nullopt if the property is absent.
  static std::optional<bool> PropertyAsOptionalBoolean(
      const std::unordered_map<std::string, std::string>& properties,
      std::string_view key);
};

}  // namespace iceberg
