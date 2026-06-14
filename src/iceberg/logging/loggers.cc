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

#include "iceberg/logging/loggers.h"

#include <exception>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

// Build-generated, .cc-only. Defines ICEBERG_HAS_SPDLOG; tested with #ifdef.
#include "iceberg/logging/cerr_logger.h"
#include "iceberg/logging/config.h"
#include "iceberg/util/macros.h"
#ifdef ICEBERG_HAS_SPDLOG
#  include "iceberg/logging/internal/spdlog_logger.h"
#endif

namespace iceberg {

namespace {

/// \brief Registry-constructible no-op logger (Load returns unique_ptr).
class NoopLogger final : public Logger {
 public:
  bool ShouldLog(LogLevel /*level*/) const override { return false; }
  void Log(LogMessage&& /*message*/) noexcept override {}
  void SetLevel(LogLevel /*level*/) override {}
  LogLevel level() const override { return LogLevel::kOff; }
  bool IsNoop() const override { return true; }
};

/// \brief Extract the logger type, defaulting to the compiled-in backend.
std::string InferLoggerType(
    const std::unordered_map<std::string, std::string>& properties) {
  auto it = properties.find(std::string(kLoggerImpl));
  if (it != properties.end() && !it->second.empty()) {
    return it->second;
  }
#ifdef ICEBERG_HAS_SPDLOG
  return std::string(kLoggerTypeSpdlog);
#else
  return std::string(kLoggerTypeCerr);
#endif
}

struct LoggerRegistryState {
  std::shared_mutex mtx;
  std::unordered_map<std::string, LoggerFactory> map;
};

LoggerRegistryState& GetRegistry() {
  static auto* state =
      new LoggerRegistryState{.map = {
                                  {std::string(kLoggerTypeNoop),
                                   [](const std::unordered_map<std::string, std::string>&)
                                       -> Result<std::unique_ptr<Logger>> {
                                     return std::make_unique<NoopLogger>();
                                   }},
                                  {std::string(kLoggerTypeCerr),
                                   [](const std::unordered_map<std::string, std::string>&)
                                       -> Result<std::unique_ptr<Logger>> {
                                     return std::make_unique<CerrLogger>();
                                   }},
#ifdef ICEBERG_HAS_SPDLOG
                                  {std::string(kLoggerTypeSpdlog),
                                   [](const std::unordered_map<std::string, std::string>&)
                                       -> Result<std::unique_ptr<Logger>> {
                                     return std::make_unique<internal::SpdLogger>();
                                   }},
#endif
                              }};
  return *state;
}

}  // namespace

Status Loggers::Register(std::string_view logger_type, LoggerFactory factory) {
  if (!factory) {
    return InvalidArgument("Logger factory for '{}' must not be empty", logger_type);
  }
  auto& registry = GetRegistry();
  std::unique_lock lock(registry.mtx);
  registry.map[std::string(logger_type)] = std::move(factory);
  return {};
}

Result<std::unique_ptr<Logger>> Loggers::Load(
    const std::unordered_map<std::string, std::string>& properties) {
  std::string logger_type = InferLoggerType(properties);

  LoggerFactory factory;
  {
    auto& registry = GetRegistry();
    std::shared_lock lock(registry.mtx);
    auto it = registry.map.find(logger_type);
    if (it == registry.map.end()) {
      return InvalidArgument(
          "Unknown logger type '{}'. Register a factory with Loggers::Register() "
          "before using this type.",
          logger_type);
    }
    factory = it->second;
  }

  try {
    ICEBERG_ASSIGN_OR_RAISE(auto logger, factory(properties));
    if (!logger) {
      return InvalidArgument("Logger factory for '{}' returned null", logger_type);
    }
    ICEBERG_RETURN_UNEXPECTED(logger->Initialize(properties));
    return logger;
  } catch (const std::exception& ex) {
    return InvalidArgument("Logger factory for '{}' failed: {}", logger_type, ex.what());
  } catch (...) {
    return InvalidArgument("Logger factory for '{}' failed with unknown exception",
                           logger_type);
  }
}

Status Loggers::LoadAndSetDefault(
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_ASSIGN_OR_RAISE(auto logger, Load(properties));
  SetDefaultLogger(std::shared_ptr<Logger>(std::move(logger)));
  return {};
}

}  // namespace iceberg
