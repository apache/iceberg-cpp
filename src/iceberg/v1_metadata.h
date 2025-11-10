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

#include "iceberg/manifest_adapter.h"

/// \file iceberg/v1_metadata.h

namespace iceberg {

/// \brief Adapter to convert V1 ManifestEntry to `ArrowArray`.
class ManifestEntryAdapterV1 : public ManifestEntryAdapter {
 public:
  ManifestEntryAdapterV1(std::optional<int64_t> snapshot_id,
                         std::shared_ptr<PartitionSpec> partition_spec,
                         std::shared_ptr<Schema> current_schema);

  Status Init() override;
  Status Append(const ManifestEntry& entry) override;

  static std::shared_ptr<Schema> EntrySchema(std::shared_ptr<StructType> partition_type);
  static std::shared_ptr<Schema> WrapFileSchema(std::shared_ptr<StructType> file_schema);
  static std::shared_ptr<StructType> DataFileSchema(
      std::shared_ptr<StructType> partition_type);

 private:
  std::optional<int64_t> snapshot_id_;
};

/// \brief Adapter to convert V1 ManifestFile to `ArrowArray`.
class ManifestFileAdapterV1 : public ManifestFileAdapter {
 public:
  ManifestFileAdapterV1(int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id)
      : snapshot_id_(snapshot_id), parent_snapshot_id_(parent_snapshot_id) {}
  Status Init() override;
  Status Append(const ManifestFile& file) override;

  static const std::shared_ptr<Schema> kManifestListSchema;

 private:
  int64_t snapshot_id_;
  std::optional<int64_t> parent_snapshot_id_;
};

}  // namespace iceberg
