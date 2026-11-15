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

#include "iceberg/stats/content_stats_util.h"

#include <algorithm>
#include <functional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/metadata_columns.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg {

namespace {

using Util = ContentStatsUtil;

/// \brief Chooses the metrics mode (which stats to include, see MetricsMode in
/// metrics_config.h) for a table field, given its ID and canonical dotted name.
/// std::nullopt leaves the field out of `content_stats`.
///
/// This is the only difference between the write and read types: StatsWriteType
/// looks the mode up in the table's MetricsConfig, and StatsReadType uses `full` for
/// the requested field IDs and skips the others.
using ModeFn =
    std::function<std::optional<MetricsMode>(int32_t field_id, std::string_view name)>;

bool IsValidStatsId(int32_t stats_id) {
  return stats_id >= Util::kMetadataStatsRangeStart &&
         stats_id < Util::kContentStatsRangeEnd;
}

bool InBaseIdRange(int32_t base_id, int32_t stats_id) {
  return stats_id >= base_id && stats_id < base_id + Util::kNumReservedFieldStatsIds;
}

bool IsFloatingPoint(const Type& type) {
  return type.type_id() == TypeId::kFloat || type.type_id() == TypeId::kDouble;
}

bool IsGeo(const Type& type) {
  return type.type_id() == TypeId::kGeometry || type.type_id() == TypeId::kGeography;
}

bool IsVariableLength(const Type& type) {
  return type.is_variant() || type.type_id() == TypeId::kString ||
         type.type_id() == TypeId::kBinary || IsGeo(type);
}

bool HasBounds(MetricsMode mode) {
  return mode.kind == MetricsMode::Kind::kTruncate ||
         mode.kind == MetricsMode::Kind::kFull;
}

std::shared_ptr<StructType> GeoLowerType(int32_t base_id) {
  return struct_({
      SchemaField::MakeRequired(base_id + Util::kGeoLowerXOffset, "x", float64(),
                                "Bounding box westernmost/xmin; [-180..180]"),
      SchemaField::MakeRequired(base_id + Util::kGeoLowerYOffset, "y", float64(),
                                "Bounding box southernmost/ymin; [-90..90]"),
      SchemaField::MakeOptional(base_id + Util::kGeoLowerZOffset, "z", float64(),
                                "Bounding box zmin"),
      SchemaField::MakeOptional(base_id + Util::kGeoLowerMOffset, "m", float64(),
                                "Bounding box mmin"),
  });
}

std::shared_ptr<StructType> GeoUpperType(int32_t base_id) {
  return struct_({
      SchemaField::MakeRequired(base_id + Util::kGeoUpperXOffset, "x", float64(),
                                "Bounding box easternmost/xmax; [-180..180]"),
      SchemaField::MakeRequired(base_id + Util::kGeoUpperYOffset, "y", float64(),
                                "Bounding box northernmost/ymax; [-90..90]"),
      SchemaField::MakeOptional(base_id + Util::kGeoUpperZOffset, "z", float64(),
                                "Bounding box zmax"),
      SchemaField::MakeOptional(base_id + Util::kGeoUpperMOffset, "m", float64(),
                                "Bounding box mmax"),
  });
}

/// \brief Build a field stats struct; see ContentStatsUtil::FieldStatsType.
std::shared_ptr<StructType> MakeFieldStatsType(const std::shared_ptr<Type>& type,
                                               bool with_null_count, int32_t base_id,
                                               MetricsMode mode) {
  if (mode.kind == MetricsMode::Kind::kNone || type == nullptr || type->is_nested()) {
    return nullptr;
  }

  std::vector<SchemaField> fields;

  if (HasBounds(mode)) {
    std::shared_ptr<Type> lower_type = type;
    std::shared_ptr<Type> upper_type = type;
    if (IsGeo(*type)) {
      lower_type = GeoLowerType(base_id);
      upper_type = GeoUpperType(base_id);
    }
    fields.push_back(SchemaField::MakeOptional(
        base_id + Util::kLowerBoundOffset, Util::kLowerBoundName, std::move(lower_type)));
    fields.push_back(SchemaField::MakeOptional(
        base_id + Util::kUpperBoundOffset, Util::kUpperBoundName, std::move(upper_type)));
  }

  fields.push_back(
      SchemaField::MakeOptional(base_id + Util::kValueCountOffset, Util::kValueCountName,
                                int64(), "Number of values (including null and NaN)"));

  if (with_null_count) {
    fields.push_back(SchemaField::MakeOptional(base_id + Util::kNullValueCountOffset,
                                               Util::kNullValueCountName, int64(),
                                               "Number of null values"));
  }

  if (IsFloatingPoint(*type)) {
    fields.push_back(SchemaField::MakeOptional(base_id + Util::kNanValueCountOffset,
                                               Util::kNanValueCountName, int64(),
                                               "Number of NaN values"));
  }

  if (IsVariableLength(*type)) {
    fields.push_back(SchemaField::MakeOptional(base_id + Util::kTotalBytesOffset,
                                               Util::kTotalBytesName, int64()));
  }

  return struct_(std::move(fields));
}

/// \brief Add stats structs for the fields of a struct that are not in a list or map.
///
/// \param dotted_prefix prefix of the canonical name, used to look up metrics modes
/// \param stats_prefix prefix of the stats struct name; names are joined with '_'
void CollectFieldStats(const StructType& struct_type, const std::string& dotted_prefix,
                       const std::string& stats_prefix, const ModeFn& mode_fn,
                       bool for_read, std::vector<SchemaField>& stats_fields) {
  for (const auto& field : struct_type.fields()) {
    std::string dotted_name = dotted_prefix + std::string(field.name());
    std::string stats_name = stats_prefix + std::string(field.name());
    const auto& type = *field.type();

    if (type.type_id() == TypeId::kStruct) {
      CollectFieldStats(internal::checked_cast<const StructType&>(type),
                        dotted_name + ".", stats_name + "_", mode_fn, for_read,
                        stats_fields);
      continue;
    }

    // lists and maps (and anything in them) may hold many values per row: no stats
    if (type.is_nested()) {
      continue;
    }

    auto base_id = Util::BaseId(field.field_id());
    if (!base_id.has_value()) {
      continue;
    }

    auto mode = mode_fn(field.field_id(), dotted_name);
    if (!mode.has_value()) {
      continue;
    }

    // Reads always project null_value_count: other writers (Java, since
    // apache/iceberg#17413) also write it for required fields, and a missing optional
    // stat reads as unknown.
    bool with_null_count = for_read || field.optional();
    if (auto field_stats = MakeFieldStatsType(field.type(), with_null_count,
                                              base_id.value(), mode.value())) {
      stats_fields.push_back(
          SchemaField::MakeOptional(base_id.value(), stats_name, std::move(field_stats)));
    }
  }
}

std::shared_ptr<StructType> BuildStatsType(const Schema& schema, const ModeFn& mode_fn,
                                           bool for_read) {
  std::vector<SchemaField> stats_fields;
  CollectFieldStats(schema, /*dotted_prefix=*/"", /*stats_prefix=*/"", mode_fn, for_read,
                    stats_fields);
  std::ranges::sort(stats_fields, std::less<>{}, &SchemaField::field_id);
  return struct_(std::move(stats_fields));
}

}  // namespace

std::optional<int32_t> ContentStatsUtil::BaseId(int32_t field_id) {
  if (field_id >= 0 && field_id < kDataFieldIdEnd) {
    return kContentStatsRangeStart + field_id * kNumReservedFieldStatsIds;
  }
  if (field_id == MetadataColumns::kLastUpdatedSequenceNumberColumnId) {
    return kLastUpdatedSequenceNumberBaseId;
  }
  if (field_id == MetadataColumns::kRowIdColumnId) {
    return kRowIdBaseId;
  }
  return std::nullopt;
}

Result<int32_t> ContentStatsUtil::FieldId(int32_t stats_id) {
  if (!IsValidStatsId(stats_id)) {
    return InvalidArgument("Invalid stats field ID: {}", stats_id);
  }

  if (stats_id < kContentStatsRangeStart) {
    if (InBaseIdRange(kLastUpdatedSequenceNumberBaseId, stats_id)) {
      return MetadataColumns::kLastUpdatedSequenceNumberColumnId;
    }
    if (InBaseIdRange(kRowIdBaseId, stats_id)) {
      return MetadataColumns::kRowIdColumnId;
    }
    return NotSupported("Unsupported metadata stats field ID: {}", stats_id);
  }

  return (stats_id - kContentStatsRangeStart) / kNumReservedFieldStatsIds;
}

Result<int32_t> ContentStatsUtil::StatOffset(int32_t stats_id) {
  if (!IsValidStatsId(stats_id)) {
    return InvalidArgument("Invalid stats field ID: {}", stats_id);
  }
  return stats_id % kNumReservedFieldStatsIds;
}

std::shared_ptr<StructType> ContentStatsUtil::FieldStatsType(const SchemaField& field,
                                                             int32_t base_id,
                                                             MetricsMode mode) {
  // the spec includes null_value_count for optional fields only
  return MakeFieldStatsType(field.type(), /*with_null_count=*/field.optional(), base_id,
                            mode);
}

std::shared_ptr<StructType> ContentStatsUtil::StatsWriteType(
    const Schema& schema, const MetricsConfig& metrics_config) {
  return BuildStatsType(
      schema,
      [&metrics_config](int32_t /*field_id*/, std::string_view name) {
        return std::optional<MetricsMode>(metrics_config.ColumnMode(name));
      },
      /*for_read=*/false);
}

std::shared_ptr<StructType> ContentStatsUtil::StatsReadType(
    const Schema& schema, std::span<const int32_t> field_ids) {
  std::unordered_set<int32_t> ids(field_ids.begin(), field_ids.end());
  return BuildStatsType(
      schema,
      [&ids](int32_t field_id, std::string_view) -> std::optional<MetricsMode> {
        if (ids.contains(field_id)) {
          return MetricsMode::Full();
        }
        return std::nullopt;
      },
      /*for_read=*/true);
}

}  // namespace iceberg
