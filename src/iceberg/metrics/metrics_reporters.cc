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

#include <exception>
#include <mutex>
#include <shared_mutex>

namespace iceberg {

namespace {

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

  Status Report([[maybe_unused]] const MetricsReport& report) override { return {}; }
};

template <typename T>
MetricsReporterFactory MakeReporterFactory() {
  return [](const std::unordered_map<std::string, std::string>& props)
             -> Result<std::unique_ptr<MetricsReporter>> { return T::Make(props); };
}

struct MetricsReporterRegistryState {
  std::shared_mutex mtx;
  std::unordered_map<std::string, MetricsReporterFactory> map;
};

MetricsReporterRegistryState& GetRegistry() {
  static MetricsReporterRegistryState state{
      .map = {{std::string(kMetricsReporterTypeNoop),
               MakeReporterFactory<NoopMetricsReporter>()}}};
  return state;
}

}  // namespace

// --- CompositeMetricsReporter ---

CompositeMetricsReporter::CompositeMetricsReporter(
    std::unordered_set<std::shared_ptr<MetricsReporter>> reporters)
    : reporters_(std::move(reporters)) {}

Status CompositeMetricsReporter::Report(const MetricsReport& report) {
  Status result;
  for (const auto& reporter : reporters_) {
    try {
      if (auto s = reporter->Report(report); !s && result.has_value()) {
        result = std::move(s);
      }
    } catch (const std::exception& ex) {
      if (result.has_value()) {
        result = InvalidArgument("Metrics reporter failed: {}", ex.what());
      }
    } catch (...) {
      if (result.has_value()) {
        result = InvalidArgument("Metrics reporter failed with unknown exception");
      }
    }
  }
  return result;
}

const std::unordered_set<std::shared_ptr<MetricsReporter>>&
CompositeMetricsReporter::Reporters() const {
  return reporters_;
}

// --- MetricsReporters ---

Status MetricsReporters::Register(std::string_view reporter_type,
                                  MetricsReporterFactory factory) {
  if (!factory) {
    return InvalidArgument("Metrics reporter factory for '{}' must not be empty",
                           reporter_type);
  }
  auto& registry = GetRegistry();
  std::unique_lock lock(registry.mtx);
  registry.map[std::string(reporter_type)] = std::move(factory);
  return {};
}

Result<std::unique_ptr<MetricsReporter>> MetricsReporters::Load(
    const std::unordered_map<std::string, std::string>& properties) {
  std::string reporter_type = InferReporterType(properties);

  MetricsReporterFactory factory;
  {
    auto& registry = GetRegistry();
    std::shared_lock lock(registry.mtx);
    auto it = registry.map.find(reporter_type);
    if (it == registry.map.end()) {
      return InvalidArgument(
          "Unknown metrics reporter type '{}'. Register a factory with "
          "MetricsReporters::Register() before using this type.",
          reporter_type);
    }
    factory = it->second;
  }

  try {
    ICEBERG_ASSIGN_OR_RAISE(auto reporter, factory(properties));
    if (!reporter) {
      return InvalidArgument("Metrics reporter factory for '{}' returned null",
                             reporter_type);
    }
    ICEBERG_RETURN_UNEXPECTED(reporter->Initialize(properties));
    return reporter;
  } catch (const std::exception& ex) {
    return InvalidArgument("Metrics reporter factory for '{}' failed: {}", reporter_type,
                           ex.what());
  } catch (...) {
    return InvalidArgument(
        "Metrics reporter factory for '{}' failed with unknown exception", reporter_type);
  }
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
