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

/// \file iceberg/logging/loggers.h
/// \brief Property-driven registry/factory for Logger backends.

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/logging/logger.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Property key selecting the logger implementation.
constexpr std::string_view kLoggerImpl = "logger-impl";
/// \brief Built-in logger type identifiers.
constexpr std::string_view kLoggerTypeNoop = "noop";
constexpr std::string_view kLoggerTypeCerr = "cerr";
constexpr std::string_view kLoggerTypeSpdlog = "spdlog";

/// \brief Factory constructing a Logger from catalog-style properties.
using LoggerFactory = std::function<Result<std::unique_ptr<Logger>>(
    const std::unordered_map<std::string, std::string>& properties)>;

/// \brief Registry of logger factories, mirroring MetricsReporters.
///
/// Built-in factories: "noop", "cerr", and (only when built with ICEBERG_SPDLOG)
/// "spdlog". When the "logger-impl" property is absent, the default is "spdlog"
/// if compiled in, otherwise "cerr" -- an intentional divergence from the metrics
/// registry's noop default (we want logs by default).
class ICEBERG_EXPORT Loggers {
 public:
  /// \brief Construct and initialize a logger from properties.
  static Result<std::unique_ptr<Logger>> Load(
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Register a factory for \p logger_type (overwrites any existing).
  static Status Register(std::string_view logger_type, LoggerFactory factory);

  /// \brief Load a logger from properties and install it as the default.
  static Status LoadAndSetDefault(
      const std::unordered_map<std::string, std::string>& properties);
};

}  // namespace iceberg
