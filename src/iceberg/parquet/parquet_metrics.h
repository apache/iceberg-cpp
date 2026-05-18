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

/// \file iceberg/parquet/parquet_metrics.h
/// \brief Utilities for extracting metrics from Parquet files.

#include <unordered_map>

#include <parquet/metadata.h>

#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/metrics.h"
#include "iceberg/metrics_config.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"

namespace iceberg::parquet {

/// \brief Utility class for computing metrics from Parquet files.
class ICEBERG_BUNDLE_EXPORT ParquetMetrics {
 public:
  ParquetMetrics() = delete;

  /// \brief Compute file-level metrics from Parquet file metadata.
  ///
  /// This function extracts metrics including row count, column sizes, value counts,
  /// null value counts, and lower/upper bounds from Parquet file metadata.
  /// The metrics are computed according to the provided MetricsConfig, which determines
  /// which columns to collect metrics for and at what granularity (counts only, truncated
  /// bounds, or full bounds).
  ///
  /// \param schema The Iceberg schema for the table.
  /// \param parquet_schema The Parquet schema descriptor.
  /// \param metrics_config The configuration specifying how to collect metrics.
  /// \param metadata The Parquet file metadata containing row group statistics.
  /// \param field_metrics Optional per-field metrics computed during write.
  ///        If provided, these take precedence over footer statistics.
  /// \return Result containing the computed Metrics or an error.
  static Result<Metrics> GetMetrics(
      const Schema& schema, const ::parquet::SchemaDescriptor& parquet_schema,
      const MetricsConfig& metrics_config, const ::parquet::FileMetaData& metadata,
      const std::unordered_map<int32_t, FieldMetrics>& field_metrics = {});
};

}  // namespace iceberg::parquet
