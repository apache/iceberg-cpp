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

/// \file iceberg/stats/content_stats_util.h
/// \brief Field IDs and types of the v4 `content_stats` struct.
///
/// See the "Content Stats" section of the Iceberg table spec.

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/metrics_config.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Utilities to derive the v4 `content_stats` struct from a table schema.
///
/// In v4, a manifest entry's column stats are one struct, `content_stats`, with an
/// optional *field stats struct* per table field (e.g. `lower_bound`, `value_count`).
/// The field IDs inside `content_stats` are computed from the table field IDs. The
/// terms below are the spec's ("Content Stats" section):
///
/// - **stats ID**: the field ID of any field inside `content_stats`, either a field
///   stats struct or one of its stats. Stats IDs are a separate ID space from table
///   field IDs, reserved in [9_000, 200_000_000).
/// - **base-id**: the first stats ID of a table field's range of 200 stats IDs,
///   `10_000 + 200 * field-id`. The field stats struct itself uses the base-id.
///   Reserved metadata fields use fixed base-ids (9000, 9200).
/// - **offset**: identifies a stat within the range, stats ID = base-id + offset
///   (1 `lower_bound`, 2 `upper_bound`, 3 `value_count`, ...).
///
/// For example, stats for table field 2 use stats IDs [10_400, 10_599]: its stats
/// struct has ID 10_400, and its `lower_bound` (offset 1) has ID 10_401.
struct ICEBERG_EXPORT ContentStatsUtil {
  /// \brief Number of stats IDs reserved for each table field.
  static constexpr int32_t kNumReservedFieldStatsIds = 200;
  /// \brief First stats ID used by metadata fields (inclusive).
  static constexpr int32_t kMetadataStatsRangeStart = 9'000;
  /// \brief First stats ID used by data fields (inclusive).
  static constexpr int32_t kContentStatsRangeStart = 10'000;
  /// \brief End of the stats ID range (exclusive).
  static constexpr int32_t kContentStatsRangeEnd = 200'000'000;
  /// \brief End of the data field IDs that can have stats (exclusive).
  static constexpr int32_t kDataFieldIdEnd = 999'950;

  /// \brief Base ID of `_last_updated_sequence_number` stats.
  static constexpr int32_t kLastUpdatedSequenceNumberBaseId = 9'000;
  /// \brief Base ID of `_row_id` stats.
  static constexpr int32_t kRowIdBaseId = 9'200;

  /// \brief Offsets of the stats within a field stats struct.
  static constexpr int32_t kLowerBoundOffset = 1;
  static constexpr int32_t kUpperBoundOffset = 2;
  static constexpr int32_t kValueCountOffset = 3;
  static constexpr int32_t kNullValueCountOffset = 4;
  static constexpr int32_t kNanValueCountOffset = 5;
  static constexpr int32_t kTotalBytesOffset = 6;

  /// \brief Offsets of the `geo_lower` fields (x, y, z, m).
  static constexpr int32_t kGeoLowerXOffset = 10;
  static constexpr int32_t kGeoLowerYOffset = 11;
  static constexpr int32_t kGeoLowerZOffset = 12;
  static constexpr int32_t kGeoLowerMOffset = 13;

  /// \brief Offsets of the `geo_upper` fields (x, y, z, m).
  static constexpr int32_t kGeoUpperXOffset = 14;
  static constexpr int32_t kGeoUpperYOffset = 15;
  static constexpr int32_t kGeoUpperZOffset = 16;
  static constexpr int32_t kGeoUpperMOffset = 17;

  static constexpr std::string_view kLowerBoundName = "lower_bound";
  static constexpr std::string_view kUpperBoundName = "upper_bound";
  static constexpr std::string_view kValueCountName = "value_count";
  static constexpr std::string_view kNullValueCountName = "null_value_count";
  static constexpr std::string_view kNanValueCountName = "nan_value_count";
  static constexpr std::string_view kTotalBytesName = "total_bytes";

  /// \brief Return the base-id of a table field: the stats ID of its field stats
  /// struct, and the start of its stats ID range.
  ///
  /// \param field_id a table field ID
  /// \return the base-id, or std::nullopt if stats for the field cannot be stored (the
  /// range would fall outside the stats ID range, or the field is a metadata field
  /// other than `_last_updated_sequence_number` and `_row_id`)
  static std::optional<int32_t> BaseId(int32_t field_id);

  /// \brief Return the table field ID that a stats ID belongs to (the inverse of
  /// BaseId plus an offset).
  ///
  /// \param stats_id a stats ID: the field ID of a field stats struct, or of a stat in
  /// one
  /// \return the table field ID; InvalidArgument if the ID is outside the stats range,
  /// or NotSupported if it is in the metadata range but not used by a reserved field
  static Result<int32_t> FieldId(int32_t stats_id);

  /// \brief Return the offset of a stats ID within its field stats struct.
  ///
  /// \param stats_id a stats ID
  /// \return the offset, 0 for the ID of the stats struct itself; InvalidArgument if
  /// the ID is outside the stats range
  static Result<int32_t> StatOffset(int32_t stats_id);

  /// \brief Build the stats struct of one field for a metrics mode, for writing.
  ///
  /// The metrics mode is the table's per-column setting for which stats writers
  /// collect (`write.metadata.metrics.default` and
  /// `write.metadata.metrics.column.<name>`, see MetricsConfig):
  /// - `none`: no stats, so no stats struct;
  /// - `counts`: `value_count`, `null_value_count`, `nan_value_count`, `total_bytes`;
  /// - `truncate(N)` and `full`: counts plus `lower_bound` and `upper_bound`. Truncation
  ///   shortens bound values but does not change their type.
  ///
  /// `null_value_count` is included only for optional fields, as the spec says.
  ///
  /// \param field a primitive or variant table field
  /// \param base_id the base ID of the field's stats struct
  /// \param mode the metrics mode of the field
  /// \return the stats struct, or nullptr if the field has no stats (mode `none`, or a
  /// nested type)
  static std::shared_ptr<StructType> FieldStatsType(const SchemaField& field,
                                                    int32_t base_id, MetricsMode mode);

  /// \brief Build the `content_stats` struct a writer puts in manifests for a table.
  ///
  /// This is the exact type that is written: which stats are present follows the
  /// table's metrics config, so it differs between tables and can change over time.
  /// It is the type of `tracked_file.content_stats` when manifests are written.
  ///
  /// It has a stats struct for every field that is not nested, is not in a list or a
  /// map, can be stored in the stats ID range, and whose metrics mode is not `none`.
  /// Fields are ordered by stats ID.
  static std::shared_ptr<StructType> StatsWriteType(const Schema& schema,
                                                    const MetricsConfig& metrics_config);

  /// \brief Build the `content_stats` projection a reader requests for some fields.
  ///
  /// A reader doesn't know the metrics config a file was written with, so it asks for
  /// every stat that any writer may have written for the requested fields, and only for
  /// those fields (e.g. the columns of a scan filter). Readers match by stats ID, and a
  /// stat missing from a file reads as null, meaning unknown.
  ///
  /// So it uses the same rules as StatsWriteType with mode `full` for every requested
  /// field, and it also includes `null_value_count` for required fields: the spec
  /// writes it for optional fields only, but Java writes it for all fields. IDs of
  /// fields that are missing from the schema or have no stats are ignored.
  static std::shared_ptr<StructType> StatsReadType(const Schema& schema,
                                                   std::span<const int32_t> field_ids);
};

}  // namespace iceberg
