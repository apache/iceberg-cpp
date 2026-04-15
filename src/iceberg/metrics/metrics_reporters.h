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

/// \file iceberg/metrics/metrics_reporters.h
/// \brief Factory for creating MetricsReporter instances.

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Property key for configuring the metrics reporter implementation.
///
/// Set this property in catalog properties to specify which metrics reporter
/// implementation to use. The value should match a registered reporter type.
inline constexpr std::string_view kMetricsReporterImpl = "metrics-reporter-impl";

/// \brief Property value for the noop metrics reporter.
inline constexpr std::string_view kMetricsReporterTypeNoop = "noop";

/// \brief Function type for creating MetricsReporter instances.
///
/// \param properties Configuration properties for the reporter.
/// \return A new MetricsReporter instance or an error.
using MetricsReporterFactory = std::function<Result<std::unique_ptr<MetricsReporter>>(
    const std::unordered_map<std::string, std::string>& properties)>;

/// \brief A MetricsReporter that delegates to multiple reporters.
///
/// Combines several reporters so that every report is delivered to each of them.
/// Any exception thrown by an individual reporter is caught and written to stderr;
/// the remaining reporters still receive the report.
///
/// Use MetricsReporters::Combine() to create instances — that helper flattens
/// nested composites and deduplicates reporters by identity.
class ICEBERG_EXPORT CompositeMetricsReporter : public MetricsReporter {
 public:
  explicit CompositeMetricsReporter(
      std::unordered_set<std::shared_ptr<MetricsReporter>> reporters);

  void Report(const MetricsReport& report) override;

  /// \brief The reporters contained in this composite.
  ///
  /// Used by MetricsReporters::Combine() for flattening.
  const std::unordered_set<std::shared_ptr<MetricsReporter>>& Reporters() const;

 private:
  std::unordered_set<std::shared_ptr<MetricsReporter>> reporters_;
};

/// \brief Factory class for creating and managing MetricsReporter instances.
///
/// This class provides a registry-based factory for creating MetricsReporter
/// implementations. Custom reporter implementations can be registered using
/// the Register() method.
class ICEBERG_EXPORT MetricsReporters {
 public:
  /// \brief Load a metrics reporter based on properties.
  ///
  /// This method looks up the "metrics-reporter-impl" property to determine
  /// which reporter implementation to create. If not specified, returns a
  /// NoopMetricsReporter.
  ///
  /// \param properties Configuration properties containing reporter type.
  /// \return A new MetricsReporter instance or an error.
  static Result<std::unique_ptr<MetricsReporter>> Load(
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Register a factory for a metrics reporter type.
  ///
  /// This method is not thread-safe. All registrations should be done during
  /// application startup before any concurrent access to Load().
  ///
  /// \param reporter_type Case-insensitive type identifier (e.g., "noop").
  /// \param factory Factory function that produces the reporter.
  static void Register(std::string_view reporter_type, MetricsReporterFactory factory);

  /// \brief Combine two reporters into one.
  ///
  /// \param first  First reporter, may be nullptr.
  /// \param second Second reporter, may be nullptr.
  /// \return Combined reporter, or nullptr if both inputs are nullptr.
  static std::shared_ptr<MetricsReporter> Combine(
      std::shared_ptr<MetricsReporter> first, std::shared_ptr<MetricsReporter> second);
};

}  // namespace iceberg
