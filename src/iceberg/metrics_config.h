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

/// \file iceberg/metrics_config.h
/// \brief Metrics configuration for Iceberg tables

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

struct ICEBERG_EXPORT MetricsMode {
 public:
  enum class Kind : uint8_t {
    kNone,
    kCounts,
    kTruncate,
    kFull,
  };

  static Result<MetricsMode> FromString(std::string_view mode);
  static MetricsMode None();
  static MetricsMode Counts();
  static MetricsMode Full();

  Kind kind;
  std::variant<std::monostate, int32_t> length;
};

/// \brief Configuration for collecting column metrics for an Iceberg table.
class ICEBERG_EXPORT MetricsConfig {
 public:
  /// \brief Get the default metrics config.
  static const std::shared_ptr<MetricsConfig>& Default();

  /// \brief Creates a metrics config from a table.
  static Result<std::shared_ptr<MetricsConfig>> Make(const Table& table);

  /// \brief Get `limit` num of primitive field ids from schema
  static Result<std::unordered_set<int32_t>> LimitFieldIds(const Schema& schema,
                                                           int32_t limit);

  /// \brief Verify that all referenced columns are valid
  /// \param updates The updates to verify
  /// \param schema The schema to verify against
  /// \return OK if all referenced columns are valid
  static Status VerifyReferencedColumns(
      const std::unordered_map<std::string, std::string>& updates, const Schema& schema);

  /// \brief Get the metrics mode for a specific column
  /// \param column_name The full name of the column
  /// \return The metrics mode for the column
  MetricsMode ColumnMode(std::string_view column_name) const;

 private:
  using ColumnModeMap =
      std::unordered_map<std::string, MetricsMode, StringHash, StringEqual>;

  MetricsConfig(ColumnModeMap column_modes, MetricsMode default_mode);

  /// \brief Generate a MetricsConfig for all columns based on overrides, schema, and sort
  /// order.
  ///
  /// \param props will be read for metrics overrides (write.metadata.metrics.column.*)
  /// and default(write.metadata.metrics.default)
  /// \param schema table schema
  /// \param order sort order columns, will be promoted to truncate(16)
  /// \return metrics configuration
  static Result<std::shared_ptr<MetricsConfig>> MakeInternal(const TableProperties& props,
                                                             const Schema& schema,
                                                             const SortOrder& order);

  ColumnModeMap column_modes_;
  MetricsMode default_mode_;
};

}  // namespace iceberg
