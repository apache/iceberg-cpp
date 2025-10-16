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

/// \file iceberg/v2_metadata.h

#include "iceberg/manifest_adapter.h"

namespace iceberg {

/// \brief Adapter to convert V2 ManifestEntry to `ArrowArray`.
class ManifestEntryAdapterV2 : public ManifestEntryAdapter {
 public:
  ManifestEntryAdapterV2(std::optional<int64_t> snapshot_id,
                         std::shared_ptr<PartitionSpec> partition_spec)
      : ManifestEntryAdapter(std::move(partition_spec)), snapshot_id_(snapshot_id) {}
  Status Init() override;
  Status Append(const ManifestEntry& entry) override;

 protected:
  Result<std::optional<int64_t>> GetSequenceNumber(
      const ManifestEntry& entry) const override;
  Result<std::optional<std::string>> GetReferenceDataFile(
      const DataFile& file) const override;

 private:
  std::optional<int64_t> snapshot_id_;
};

/// \brief Adapter to convert V2 ManifestFile to `ArrowArray`.
class ManifestFileAdapterV2 : public ManifestFileAdapter {
 public:
  ManifestFileAdapterV2(int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
                        int64_t sequence_number)
      : snapshot_id_(snapshot_id),
        parent_snapshot_id_(parent_snapshot_id),
        sequence_number_(sequence_number) {}
  Status Init() override;
  Status Append(const ManifestFile& file) override;

 protected:
  Result<int64_t> GetSequenceNumber(const ManifestFile& file) const override;
  Result<int64_t> GetMinSequenceNumber(const ManifestFile& file) const override;

 private:
  int64_t snapshot_id_;
  std::optional<int64_t> parent_snapshot_id_;
  int64_t sequence_number_;
};

}  // namespace iceberg
