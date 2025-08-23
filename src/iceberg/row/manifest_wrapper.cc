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

#include "iceberg/row/manifest_wrapper.h"

#include "iceberg/manifest_reader_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
template <typename T>
  requires std::is_same_v<T, std::vector<uint8_t>> || std::is_same_v<T, std::string>
std::string_view ToView(const T& value) {
  return {reinterpret_cast<const char*>(value.data()), value.size()};  // NOLINT
}

}  // namespace

Result<Scalar> PartitionFieldSummaryStructLike::GetField(size_t pos) const {
  if (pos >= num_fields()) {
    return InvalidArgument("Invalid partition field summary index: {}", pos);
  }
  switch (pos) {
    case 0:
      return summary_.get().contains_null;
    case 1: {
      if (summary_.get().contains_nan.has_value()) {
        return summary_.get().contains_nan.value();
      }
      return {};
    }
    case 2: {
      if (summary_.get().lower_bound.has_value()) {
        return ToView(summary_.get().lower_bound.value());
      }
      return {};
    }
    case 3: {
      if (summary_.get().upper_bound.has_value()) {
        return ToView(summary_.get().upper_bound.value());
      }
      return {};
    }
    default:
      return InvalidArgument("Invalid partition field summary index: {}", pos);
  }
}

Result<Scalar> PartitionFieldSummaryArrayLike::GetElement(size_t pos) const {
  if (pos >= size()) {
    return InvalidArgument("Invalid partition field summary index: {}", pos);
  }
  if (summary_ == nullptr) {
    summary_ = std::make_shared<PartitionFieldSummaryStructLike>(summaries_.get()[pos]);
  } else {
    summary_->Reset(summaries_.get()[pos]);
  }
  return summary_;
}

Result<Scalar> ManifestFileStructLike::GetField(size_t pos) const {
  if (pos >= num_fields()) {
    return InvalidArgument("Invalid manifest file field index: {}", pos);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto field,
                          ManifestFileFieldFromIndex(static_cast<int32_t>(pos)));
  const auto& manifest_file = manifest_file_.get();
  switch (field) {
    case ManifestFileField::kManifestPath:
      return ToView(manifest_file.manifest_path);
    case ManifestFileField::kManifestLength:
      return manifest_file.manifest_length;
    case ManifestFileField::kPartitionSpecId:
      return manifest_file.partition_spec_id;
    case ManifestFileField::kContent:
      return static_cast<int32_t>(manifest_file.content);
    case ManifestFileField::kSequenceNumber:
      return manifest_file.sequence_number;
    case ManifestFileField::kMinSequenceNumber:
      return manifest_file.min_sequence_number;
    case ManifestFileField::kAddedSnapshotId:
      return manifest_file.added_snapshot_id;
    case ManifestFileField::kAddedFilesCount: {
      if (manifest_file.added_files_count.has_value()) {
        return manifest_file.added_files_count.value();
      }
      return {};
    }
    case ManifestFileField::kExistingFilesCount: {
      if (manifest_file.existing_files_count.has_value()) {
        return manifest_file.existing_files_count.value();
      }
      return {};
    }
    case ManifestFileField::kDeletedFilesCount: {
      if (manifest_file.deleted_files_count.has_value()) {
        return manifest_file.deleted_files_count.value();
      }
      return {};
    }
    case ManifestFileField::kAddedRowsCount: {
      if (manifest_file.added_rows_count.has_value()) {
        return manifest_file.added_rows_count.value();
      }
      return {};
    }
    case ManifestFileField::kExistingRowsCount: {
      if (manifest_file.existing_rows_count.has_value()) {
        return manifest_file.existing_rows_count.value();
      }
      return {};
    }
    case ManifestFileField::kDeletedRowsCount: {
      if (manifest_file.deleted_rows_count.has_value()) {
        return manifest_file.deleted_rows_count.value();
      }
      return {};
    }
    case ManifestFileField::kPartitionFieldSummary: {
      if (summaries_ == nullptr) {
        summaries_ =
            std::make_shared<PartitionFieldSummaryArrayLike>(manifest_file.partitions);
      } else {
        summaries_->Reset(manifest_file.partitions);
      }
      return summaries_;
    }
    case ManifestFileField::kKeyMetadata:
      return ToView(manifest_file.key_metadata);
    case ManifestFileField::kFirstRowId:
      if (manifest_file.first_row_id.has_value()) {
        return manifest_file.first_row_id.value();
      }
      return {};
    case ManifestFileField::kNextUnusedId:
      return InvalidArgument("Invalid manifest file field index: {}", pos);
  }
  return InvalidArgument("Invalid manifest file field index: {}", pos);
}

size_t ManifestFileStructLike::num_fields() const {
  return static_cast<size_t>(ManifestFileField::kNextUnusedId);
}

std::unique_ptr<StructLike> FromManifestFile(const ManifestFile& file) {
  return std::make_unique<ManifestFileStructLike>(file);
}

}  // namespace iceberg
