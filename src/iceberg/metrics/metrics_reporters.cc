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

#include "iceberg/metrics/metrics_reporters.h"

namespace iceberg {

namespace {

/// \brief Registry type for MetricsReporter factories.
using MetricsReporterRegistry = std::unordered_map<std::string, MetricsReporterFactory>;

/// \brief Extract the reporter type identifier from properties.
///
/// Returns the value of "metrics-reporter-impl" verbatim (case-preserved), or
/// kMetricsReporterTypeNoop if the property is absent or empty.
std::string InferReporterType(
    const std::unordered_map<std::string, std::string>& properties) {
  auto it = properties.find(std::string(kMetricsReporterImpl));
  if (it != properties.end() && !it->second.empty()) {
    return it->second;
  }
  return std::string(kMetricsReporterTypeNoop);
}

/// \brief Metrics reporter that does nothing.
class NoopMetricsReporter : public MetricsReporter {
 public:
  static Result<std::unique_ptr<MetricsReporter>> Make(
      [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
    return std::make_unique<NoopMetricsReporter>();
  }

  void Report([[maybe_unused]] const MetricsReport& report) override {}
};

template <typename T>
MetricsReporterFactory MakeReporterFactory() {
  return [](const std::unordered_map<std::string, std::string>& props)
             -> Result<std::unique_ptr<MetricsReporter>> { return T::Make(props); };
}

MetricsReporterRegistry CreateDefaultRegistry() {
  return {
      {std::string(kMetricsReporterTypeNoop), MakeReporterFactory<NoopMetricsReporter>()},
  };
}

MetricsReporterRegistry& GetRegistry() {
  static MetricsReporterRegistry registry = CreateDefaultRegistry();
  return registry;
}

}  // namespace

// --- CompositeMetricsReporter ---

CompositeMetricsReporter::CompositeMetricsReporter(
    std::unordered_set<std::shared_ptr<MetricsReporter>> reporters)
    : reporters_(std::move(reporters)) {}

void CompositeMetricsReporter::Report(const MetricsReport& report) {
  for (const auto& reporter : reporters_) {
    try {
      reporter->Report(report);
    } catch (...) {
      // Catch all exceptions to ensure one failing reporter doesn't prevent others from
      // receiving the report.
    }
  }
}

const std::unordered_set<std::shared_ptr<MetricsReporter>>&
CompositeMetricsReporter::Reporters() const {
  return reporters_;
}

// --- MetricsReporters ---

void MetricsReporters::Register(std::string_view reporter_type,
                                MetricsReporterFactory factory) {
  GetRegistry()[std::string(reporter_type)] = std::move(factory);
}

Result<std::unique_ptr<MetricsReporter>> MetricsReporters::Load(
    const std::unordered_map<std::string, std::string>& properties) {
  std::string reporter_type = InferReporterType(properties);

  auto& registry = GetRegistry();
  auto it = registry.find(reporter_type);
  if (it == registry.end()) {
    return InvalidArgument(
        "Unknown metrics reporter type '{}'. Register a factory with "
        "MetricsReporters::Register() before using this type.",
        reporter_type);
  }

  return it->second(properties);
}

std::shared_ptr<MetricsReporter> MetricsReporters::Combine(
    std::shared_ptr<MetricsReporter> first, std::shared_ptr<MetricsReporter> second) {
  if (!first) return second;
  if (!second || first.get() == second.get()) return first;

  std::unordered_set<std::shared_ptr<MetricsReporter>> reporters;

  auto collect = [&reporters](const std::shared_ptr<MetricsReporter>& r) {
    if (auto* composite = dynamic_cast<CompositeMetricsReporter*>(r.get())) {
      for (const auto& inner : composite->Reporters()) {
        reporters.insert(inner);
      }
    } else {
      reporters.insert(r);
    }
  };

  collect(first);
  collect(second);

  return std::make_shared<CompositeMetricsReporter>(std::move(reporters));
}

}  // namespace iceberg
