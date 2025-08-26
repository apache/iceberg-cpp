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

/// \file iceberg/v3_metadata.h

#include <memory>

#include "iceberg/manifest_adapter.h"

namespace iceberg {

/// \brief Adapter to convert V3ManifestEntry to `ArrowArray`.
class ManifestEntryAdapterV3 : public ManifestEntryAdapter {
 public:
  ManifestEntryAdapterV3(std::optional<int64_t> snapshot_id,
                         std::optional<int64_t> first_row_id,
                         std::shared_ptr<Schema> schema) {
    // TODO(xiao.dong): init v3 schema
  }
  Status StartAppending() override { return {}; }
  Status Append(const ManifestEntry& entry) override { return {}; }
  Result<ArrowArray> FinishAppending() override { return {}; }

 private:
  std::shared_ptr<Schema> manifest_schema_;
  ArrowSchema schema_;  // converted from manifest_schema_
};

/// \brief Adapter to convert V3 ManifestFile to `ArrowArray`.
class ManifestFileAdapterV3 : public ManifestFileAdapter {
 public:
  ManifestFileAdapterV3(int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
                        int64_t sequence_number, std::optional<int64_t> first_row_id,
                        std::shared_ptr<Schema> schema) {
    // TODO(xiao.dong): init v3 schema
  }
  Status StartAppending() override { return {}; }
  Status Append(const ManifestFile& file) override { return {}; }
  Result<ArrowArray> FinishAppending() override { return {}; }

 private:
  std::shared_ptr<Schema> manifest_list_schema_;
  ArrowSchema schema_;  // converted from manifest_list_schema_
};

}  // namespace iceberg
