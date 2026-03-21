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

#include "iceberg/metrics_reporters.h"

#include <unordered_set>

#include "iceberg/util/string_util.h"

namespace iceberg {

namespace {

/// \brief Registry type for MetricsReporter factories with heterogeneous lookup support.
using MetricsReporterRegistry = std::unordered_map<std::string, MetricsReporterFactory>;

/// \brief Get the set of known metrics reporter types.
const std::unordered_set<std::string>& DefaultReporterTypes() {
  static const std::unordered_set<std::string> kReporterTypes = {
      std::string(kMetricsReporterTypeNoop),
  };
  return kReporterTypes;
}

/// \brief Infer the reporter type from properties.
std::string InferReporterType(
    const std::unordered_map<std::string, std::string>& properties) {
  auto it = properties.find(std::string(kMetricsReporterImpl));
  if (it != properties.end() && !it->second.empty()) {
    return StringUtils::ToLower(it->second);
  }
  // Default to noop reporter
  return std::string(kMetricsReporterTypeNoop);
}

/// \brief Metrics reporter that does nothing.
///
/// This is the default reporter used when no reporter is configured.
/// It silently discards all reports.
class NoopMetricsReporter : public MetricsReporter {
 public:
  static Result<std::unique_ptr<MetricsReporter>> Make(
      [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
    return std::make_unique<NoopMetricsReporter>();
  }

  void Report([[maybe_unused]] const MetricsReport& report) override {
    // Intentionally empty - noop implementation discards all reports
  }
};

/// \brief Template helper to create factory functions for reporter types.
template <typename T>
MetricsReporterFactory MakeReporterFactory() {
  return [](const std::unordered_map<std::string, std::string>& props)
             -> Result<std::unique_ptr<MetricsReporter>> { return T::Make(props); };
}

/// \brief Create the default registry with built-in reporters.
MetricsReporterRegistry CreateDefaultRegistry() {
  return {
      {std::string(kMetricsReporterTypeNoop), MakeReporterFactory<NoopMetricsReporter>()},
  };
}

/// \brief Get the global registry of metrics reporter factories.
MetricsReporterRegistry& GetRegistry() {
  static MetricsReporterRegistry registry = CreateDefaultRegistry();
  return registry;
}

}  // namespace

void MetricsReporters::Register(std::string_view reporter_type,
                                MetricsReporterFactory factory) {
  GetRegistry()[StringUtils::ToLower(reporter_type)] = std::move(factory);
}

Result<std::unique_ptr<MetricsReporter>> MetricsReporters::Load(
    const std::unordered_map<std::string, std::string>& properties) {
  std::string reporter_type = InferReporterType(properties);

  auto& registry = GetRegistry();
  auto it = registry.find(reporter_type);
  if (it == registry.end()) {
    if (DefaultReporterTypes().contains(reporter_type)) {
      return NotImplemented("Metrics reporter type '{}' is not yet supported",
                            reporter_type);
    }
    return InvalidArgument("Unknown metrics reporter type: '{}'", reporter_type);
  }

  return it->second(properties);
}

}  // namespace iceberg
