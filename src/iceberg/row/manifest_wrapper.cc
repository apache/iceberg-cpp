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

#include <iterator>
#include <map>
#include <memory>
#include <span>
#include <type_traits>
#include <vector>

#include "iceberg/manifest/manifest_reader_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

enum class DataFileFieldPosition : size_t {
  kContent = 0,
  kFilePath = 1,
  kFileFormat = 2,
  kSpecId = 3,
  kPartition = 4,
  kRecordCount = 5,
  kFileSize = 6,
  kColumnSizes = 7,
  kValueCounts = 8,
  kNullValueCounts = 9,
  kNanValueCounts = 10,
  kLowerBounds = 11,
  kUpperBounds = 12,
  kKeyMetadata = 13,
  kSplitOffsets = 14,
  kEqualityIds = 15,
  kSortOrderId = 16,
  kFirstRowId = 17,
  kReferencedDataFile = 18,
  kContentOffset = 19,
  kContentSize = 20,
  kNextUnusedId = 21,
};

template <typename T>
  requires std::is_same_v<T, std::vector<uint8_t>> || std::is_same_v<T, std::string>
std::string_view ToView(const T& value) {
  return {reinterpret_cast<const char*>(value.data()), value.size()};  // NOLINT
}

Scalar ToScalar(const int32_t value) { return value; }

Scalar ToScalar(const int64_t value) { return value; }

Scalar ToScalar(const std::vector<uint8_t>& value) { return ToView(value); }

template <typename T>
Result<Scalar> FromOptional(const std::optional<T>& value) {
  if (value.has_value()) {
    return value.value();
  }
  return std::monostate{};
}

Result<Scalar> FromOptionalString(const std::optional<std::string>& value) {
  if (value.has_value()) {
    return ToView(value.value());
  }
  return std::monostate{};
}

template <typename T>
class VectorArrayLike : public ArrayLike {
 public:
  explicit VectorArrayLike(std::span<const T> values) : values_(values) {}

  Result<Scalar> GetElement(size_t pos) const override {
    if (pos >= size()) {
      return InvalidArgument("Invalid array index: {}", pos);
    }
    return ToScalar(values_[pos]);
  }

  size_t size() const override { return values_.size(); }

 private:
  std::span<const T> values_;
};

template <typename V>
class IntMapLike : public MapLike {
 public:
  explicit IntMapLike(const std::map<int32_t, V>& values) : values_(values) {}

  Result<Scalar> GetKey(size_t pos) const override {
    if (pos >= size()) {
      return InvalidArgument("Invalid map index: {}", pos);
    }
    return std::next(values_.get().cbegin(), pos)->first;
  }

  Result<Scalar> GetValue(size_t pos) const override {
    if (pos >= size()) {
      return InvalidArgument("Invalid map index: {}", pos);
    }
    return ToScalar(std::next(values_.get().cbegin(), pos)->second);
  }

  size_t size() const override { return values_.get().size(); }

 private:
  std::reference_wrapper<const std::map<int32_t, V>> values_;
};

template <typename V>
Result<Scalar> FromOptionalMap(const std::map<int32_t, V>& values) {
  if (values.empty()) {
    return std::monostate{};
  }
  return std::make_shared<IntMapLike<V>>(values);
}

template <typename T>
Result<Scalar> FromOptionalVector(const std::vector<T>& values) {
  if (values.empty()) {
    return std::monostate{};
  }
  return std::make_shared<VectorArrayLike<T>>(values);
}

Result<Scalar> FromOptionalBytes(const std::vector<uint8_t>& value) {
  if (value.empty()) {
    return std::monostate{};
  }
  return ToView(value);
}

}  // namespace

Result<Scalar> PartitionFieldSummaryStructLike::GetField(size_t pos) const {
  if (pos >= num_fields()) {
    return InvalidArgument("Invalid partition field summary index: {}", pos);
  }
  switch (pos) {
    case 0:
      return summary_.get().contains_null;
    case 1:
      return FromOptional(summary_.get().contains_nan);
    case 2:
      return FromOptional(
          summary_.get().lower_bound.transform(ToView<std::vector<uint8_t>>));
    case 3:
      return FromOptional(
          summary_.get().upper_bound.transform(ToView<std::vector<uint8_t>>));
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
    case ManifestFileField::kAddedFilesCount:
      return FromOptional(manifest_file.added_files_count);
    case ManifestFileField::kExistingFilesCount:
      return FromOptional(manifest_file.existing_files_count);
    case ManifestFileField::kDeletedFilesCount:
      return FromOptional(manifest_file.deleted_files_count);
    case ManifestFileField::kAddedRowsCount:
      return FromOptional(manifest_file.added_rows_count);
    case ManifestFileField::kExistingRowsCount:
      return FromOptional(manifest_file.existing_rows_count);
    case ManifestFileField::kDeletedRowsCount:
      return FromOptional(manifest_file.deleted_rows_count);
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
      return FromOptional(manifest_file.first_row_id);
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

Result<Scalar> DataFileStructLike::GetField(size_t pos) const {
  if (pos >= num_fields()) {
    return InvalidArgument("Invalid data file field index: {}", pos);
  }

  const auto& data_file = data_file_.get();
  switch (static_cast<DataFileFieldPosition>(pos)) {
    case DataFileFieldPosition::kContent:
      return static_cast<int32_t>(data_file.content);
    case DataFileFieldPosition::kFilePath:
      return ToView(data_file.file_path);
    case DataFileFieldPosition::kFileFormat:
      return ToString(data_file.file_format);
    case DataFileFieldPosition::kSpecId:
      return FromOptional(data_file.partition_spec_id);
    case DataFileFieldPosition::kPartition: {
      partition_ = std::make_shared<PartitionValues>(data_file.partition);
      return partition_;
    }
    case DataFileFieldPosition::kRecordCount:
      return data_file.record_count;
    case DataFileFieldPosition::kFileSize:
      return data_file.file_size_in_bytes;
    case DataFileFieldPosition::kColumnSizes:
      return FromOptionalMap(data_file.column_sizes);
    case DataFileFieldPosition::kValueCounts:
      return FromOptionalMap(data_file.value_counts);
    case DataFileFieldPosition::kNullValueCounts:
      return FromOptionalMap(data_file.null_value_counts);
    case DataFileFieldPosition::kNanValueCounts:
      return FromOptionalMap(data_file.nan_value_counts);
    case DataFileFieldPosition::kLowerBounds:
      return FromOptionalMap(data_file.lower_bounds);
    case DataFileFieldPosition::kUpperBounds:
      return FromOptionalMap(data_file.upper_bounds);
    case DataFileFieldPosition::kKeyMetadata:
      return FromOptionalBytes(data_file.key_metadata);
    case DataFileFieldPosition::kSplitOffsets:
      return FromOptionalVector(data_file.split_offsets);
    case DataFileFieldPosition::kEqualityIds:
      return FromOptionalVector(data_file.equality_ids);
    case DataFileFieldPosition::kSortOrderId:
      return FromOptional(data_file.sort_order_id);
    case DataFileFieldPosition::kFirstRowId:
      return FromOptional(data_file.first_row_id);
    case DataFileFieldPosition::kReferencedDataFile:
      return FromOptionalString(data_file.referenced_data_file);
    case DataFileFieldPosition::kContentOffset:
      return FromOptional(data_file.content_offset);
    case DataFileFieldPosition::kContentSize:
      return FromOptional(data_file.content_size_in_bytes);
    case DataFileFieldPosition::kNextUnusedId:
      return InvalidArgument("Invalid data file field index: {}", pos);
  }
  return InvalidArgument("Invalid data file field index: {}", pos);
}

size_t DataFileStructLike::num_fields() const {
  return static_cast<size_t>(DataFileFieldPosition::kNextUnusedId);
}

}  // namespace iceberg
