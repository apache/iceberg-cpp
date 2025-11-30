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

#include "iceberg/inheritable_metadata.h"

#include <utility>

#include <iceberg/result.h>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/snapshot.h"

namespace iceberg {

BaseInheritableMetadata::BaseInheritableMetadata(int32_t spec_id, int64_t snapshot_id,
                                                 int64_t sequence_number,
                                                 std::string manifest_location)
    : spec_id_(spec_id),
      snapshot_id_(snapshot_id),
      sequence_number_(sequence_number),
      manifest_location_(std::move(manifest_location)) {}

Status BaseInheritableMetadata::Apply(ManifestEntry& entry) {
  if (!entry.snapshot_id.has_value()) {
    entry.snapshot_id = snapshot_id_;
  }

  // In v1 metadata, the data sequence number is not persisted and can be safely defaulted
  // to 0.
  // In v2 metadata, the data sequence number should be inherited iff the entry status
  // is ADDED.
  if (!entry.sequence_number.has_value() &&
      (sequence_number_ == 0 || entry.status == ManifestStatus::kAdded)) {
    entry.sequence_number = sequence_number_;
  }

  // In v1 metadata, the file sequence number is not persisted and can be safely defaulted
  // to 0.
  // In v2 metadata, the file sequence number should be inherited iff the entry status
  // is ADDED.
  if (!entry.file_sequence_number.has_value() &&
      (sequence_number_ == 0 || entry.status == ManifestStatus::kAdded)) {
    entry.file_sequence_number = sequence_number_;
  }

  if (entry.data_file) {
    entry.data_file->partition_spec_id = spec_id_;
  } else {
    return InvalidManifest("Manifest entry has no data file");
  }

  return {};
}

Status EmptyInheritableMetadata::Apply(ManifestEntry& entry) {
  if (!entry.snapshot_id.has_value()) {
    return InvalidManifest(
        "Entries must have explicit snapshot ids if inherited metadata is empty");
  }
  return {};
}

CopyInheritableMetadata::CopyInheritableMetadata(int64_t snapshot_id)
    : snapshot_id_(snapshot_id) {}

Status CopyInheritableMetadata::Apply(ManifestEntry& entry) {
  entry.snapshot_id = snapshot_id_;
  return {};
}

Result<std::unique_ptr<InheritableMetadata>> InheritableMetadataFactory::Empty() {
  return std::make_unique<EmptyInheritableMetadata>();
}

Result<std::unique_ptr<InheritableMetadata>> InheritableMetadataFactory::FromManifest(
    const ManifestFile& manifest) {
  // Validate that the manifest has a snapshot ID assigned
  if (manifest.added_snapshot_id == Snapshot::kInvalidSnapshotId) {
    return InvalidManifest("Manifest file {} has no snapshot ID", manifest.manifest_path);
  }

  return std::make_unique<BaseInheritableMetadata>(
      manifest.partition_spec_id, manifest.added_snapshot_id, manifest.sequence_number,
      manifest.manifest_path);
}

Result<std::unique_ptr<InheritableMetadata>> InheritableMetadataFactory::ForCopy(
    int64_t snapshot_id) {
  return std::make_unique<CopyInheritableMetadata>(snapshot_id);
}

}  // namespace iceberg
