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

#include <cstdint>
#include <memory>
#include <unordered_map>

#include <parquet/arrow/schema.h>

#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg::parquet {

// Inclusive footer-statistics filtering, following Java's
// ParquetMetricsRowGroupFilter. False proves that the row group cannot match;
// true still requires evaluating the residual predicate on the returned rows.
class ICEBERG_BUNDLE_EXPORT ParquetMetricsRowGroupFilter {
 public:
  static Result<std::unique_ptr<ParquetMetricsRowGroupFilter>> Make(
      const std::shared_ptr<Expression>& filter,
      const ::parquet::SchemaDescriptor& file_schema);

  Result<bool> ShouldRead(const ::parquet::arrow::SchemaManifest& manifest,
                          const ::parquet::RowGroupMetaData& row_group) const;

 private:
  ParquetMetricsRowGroupFilter() = default;
  std::shared_ptr<Expression> bound_;
  std::unordered_map<int32_t, int> column_indices_;
};

}  // namespace iceberg::parquet
