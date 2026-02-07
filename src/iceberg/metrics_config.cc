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

#include "iceberg/metrics_config.h"

#include <string>
#include <unordered_map>

#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_properties.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/type_util.h"

namespace iceberg {

namespace {

constexpr std::string_view kNoneName = "none";
constexpr std::string_view kCountsName = "counts";
constexpr std::string_view kFullName = "full";
constexpr std::string_view kTruncatePrefix = "truncate(";
constexpr int32_t kDefaultTruncateLength = 16;
constexpr MetricsMode kDefaultMetricsMode = {.kind = MetricsMode::Kind::kTruncate,
                                             .length = kDefaultTruncateLength};

MetricsMode SortedColumnDefaultMode(MetricsMode default_mode) {
  if (default_mode.kind == MetricsMode::Kind::kNone ||
      default_mode.kind == MetricsMode::Kind::kCounts) {
    return kDefaultMetricsMode;
  } else {
    return default_mode;
  }
}

int32_t MaxInferredColumns(const TableProperties& properties) {
  int32_t max_inferred_columns =
      properties.Get(TableProperties::kMetricsMaxInferredColumnDefaults);
  if (max_inferred_columns < 0) {
    // fallback to default
    return TableProperties::kMetricsMaxInferredColumnDefaults.value();
  }
  return max_inferred_columns;
}

Result<MetricsMode> ParseMode(std::string_view mode, MetricsMode fallback) {
  return MetricsMode::FromString(mode).value_or(fallback);
}

}  // namespace

MetricsMode MetricsMode::None() { return {.kind = Kind::kNone}; }

MetricsMode MetricsMode::Counts() { return {.kind = Kind::kCounts}; }

MetricsMode MetricsMode::Full() { return {.kind = Kind::kFull}; }

Result<MetricsMode> MetricsMode::FromString(std::string_view mode) {
  if (StringUtils::EqualsIgnoreCase(mode, kNoneName)) {
    return MetricsMode::None();
  } else if (StringUtils::EqualsIgnoreCase(mode, kCountsName)) {
    return MetricsMode::Counts();
  } else if (StringUtils::EqualsIgnoreCase(mode, kFullName)) {
    return MetricsMode::Full();
  }

  if (StringUtils::StartsWithIgnoreCase(mode, kTruncatePrefix) && mode.ends_with(")")) {
    auto res = StringUtils::ParseNumber<int32_t>(
        mode.substr(9 /* "truncate(" length */, mode.size() - 10));
    if (!res.has_value()) {
      return InvalidArgument("Invalid truncate mode: {}", mode);
    }
    int32_t length = res.value();
    if (length == kDefaultTruncateLength) {
      return kDefaultMetricsMode;
    }
    ICEBERG_PRECHECK(length > 0, "Truncate length should be positive.");
    return MetricsMode{.kind = Kind::kTruncate, .length = length};
  }
  return InvalidArgument("Invalid metrics mode: {}", mode);
}

MetricsConfig::MetricsConfig(ColumnModeMap column_modes, MetricsMode default_mode)
    : column_modes_(std::move(column_modes)), default_mode_(default_mode) {}

const std::shared_ptr<MetricsConfig>& MetricsConfig::Default() {
  static const std::shared_ptr<MetricsConfig> kDefaultConfig(
      new MetricsConfig(/*column_modes=*/{}, kDefaultMetricsMode));
  return kDefaultConfig;
}

Result<std::shared_ptr<MetricsConfig>> MetricsConfig::Make(const Table& table) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, table.schema());
  auto sort_order = table.sort_order();
  return MakeInternal(table.properties(), *schema,
                      *sort_order.value_or(SortOrder::Unsorted()));
}

Result<std::shared_ptr<MetricsConfig>> MetricsConfig::MakeInternal(
    const TableProperties& props, const Schema& schema, const SortOrder& order) {
  ColumnModeMap column_modes;

  MetricsMode default_mode = kDefaultMetricsMode;
  if (props.configs().contains(TableProperties::kDefaultWriteMetricsMode.key())) {
    std::string configured_metrics_mode =
        props.Get(TableProperties::kDefaultWriteMetricsMode);
    ICEBERG_ASSIGN_OR_RAISE(default_mode,
                            ParseMode(configured_metrics_mode, kDefaultMetricsMode));
  } else {
    int32_t max_inferred_columns = MaxInferredColumns(props);
    GetProjectedIdsVisitor visitor(/*include_struct_ids=*/true);
    ICEBERG_RETURN_UNEXPECTED(visitor.Visit(schema));
    auto projected_columns = static_cast<int32_t>(visitor.Finish().size());
    if (max_inferred_columns < projected_columns) {
      ICEBERG_ASSIGN_OR_RAISE(auto limit_field_ids,
                              LimitFieldIds(schema, max_inferred_columns));
      for (auto id : limit_field_ids) {
        ICEBERG_ASSIGN_OR_RAISE(auto column_name, schema.FindColumnNameById(id));
        ICEBERG_CHECK(column_name.has_value(), "Field id {} not found in schema", id);
        column_modes[std::string(column_name.value())] = kDefaultMetricsMode;
      }
      // All other columns don't use metrics
      default_mode = MetricsMode::None();
    }
  }

  // First set sorted column with sorted column default (can be overridden by user)
  auto sorted_col_default_mode = SortedColumnDefaultMode(default_mode);
  auto sorted_columns = SortOrder::OrderPreservingSortedColumns(schema, order);
  for (const auto& sorted_column : sorted_columns) {
    column_modes[std::string(sorted_column)] = sorted_col_default_mode;
  }

  // Handle user overrides of defaults
  for (const auto& prop : props.configs()) {
    if (prop.first.starts_with(TableProperties::kMetricModeColumnConfPrefix)) {
      std::string column_alias =
          prop.first.substr(TableProperties::kMetricModeColumnConfPrefix.size());
      ICEBERG_ASSIGN_OR_RAISE(auto mode, ParseMode(prop.second, default_mode));
      column_modes[std::move(column_alias)] = mode;
    }
  }

  return std::shared_ptr<MetricsConfig>(
      new MetricsConfig(std::move(column_modes), default_mode));
}

Result<std::unordered_set<int32_t>> MetricsConfig::LimitFieldIds(const Schema& schema,
                                                                 int32_t limit) {
  class Visitor {
   public:
    explicit Visitor(int32_t limit) : limit_(limit) {}

    Status Visit(const Type& type) {
      if (type.is_nested()) {
        return VisitNested(internal::checked_cast<const NestedType&>(type));
      } else {
        return VisitPrimitive(internal::checked_cast<const PrimitiveType&>(type));
      }
    }

    Status VisitNested(const NestedType& type) {
      for (const auto& field : type.fields()) {
        if (!ShouldContinue()) {
          break;
        }
        // TODO(zhuo.wang): variant type should also be handled here
        if (field.type()->is_primitive()) {
          ids_.insert(field.field_id());
        }
      }

      for (const auto& field : type.fields()) {
        if (ShouldContinue()) {
          ICEBERG_RETURN_UNEXPECTED(Visit(*field.type()));
        }
      }
      return {};
    }

    Status VisitPrimitive(const PrimitiveType& type) { return {}; }

    std::unordered_set<int32_t> Finish() const { return ids_; }

   private:
    bool ShouldContinue() { return ids_.size() < limit_; }

   private:
    std::unordered_set<int32_t> ids_;
    int32_t limit_;
  };

  Visitor visitor(limit);
  ICEBERG_RETURN_UNEXPECTED(visitor.Visit(schema));
  return visitor.Finish();
}

Status MetricsConfig::VerifyReferencedColumns(
    const std::unordered_map<std::string, std::string>& updates, const Schema& schema) {
  for (const auto& [key, value] : updates) {
    if (!key.starts_with(TableProperties::kMetricModeColumnConfPrefix)) {
      continue;
    }
    auto field_name =
        std::string_view(key).substr(TableProperties::kMetricModeColumnConfPrefix.size());
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema.FindFieldByName(field_name));
    ICEBERG_CHECK(field.has_value(),
                  "Invalid metrics config, could not find column {} from table prop {} "
                  "in schema {}",
                  field_name, key, schema.ToString());
  }
  return {};
}

MetricsMode MetricsConfig::ColumnMode(std::string_view column_name) const {
  if (auto it = column_modes_.find(column_name); it != column_modes_.end()) {
    return it->second;
  }
  return default_mode_;
}

}  // namespace iceberg
