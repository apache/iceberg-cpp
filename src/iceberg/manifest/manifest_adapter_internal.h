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

/// \file iceberg/manifest/manifest_adapter_internal.h
/// Base class for adapters handling v1/v2/v3/v4 manifest metadata.

#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base class for appending manifest metadata to Arrow arrays.
class ICEBERG_EXPORT ManifestAdapter {
 public:
  ManifestAdapter() = default;
  virtual ~ManifestAdapter() = default;
  virtual Status Init() = 0;

  Status StartAppending();
  Result<ArrowArray*> FinishAppending();
  int64_t size() const { return size_; }
  const std::unordered_map<std::string, std::string>& metadata() const {
    return metadata_;
  }

 protected:
  ArrowArray array_;
  // Arrow schema of manifest or manifest list depending on the subclass
  ArrowSchema schema_;
  // Number of appended elements in the array
  int64_t size_ = 0;
  std::unordered_map<std::string, std::string> metadata_;
};

/// \brief Adapter for appending a list of `ManifestEntry`s to an `ArrowArray`.
/// Implemented by different versions with version-specific schemas.
class ICEBERG_EXPORT ManifestEntryAdapter : public ManifestAdapter {
 public:
  ManifestEntryAdapter(std::optional<int64_t> snapshot_id_,
                       std::shared_ptr<PartitionSpec> partition_spec,
                       std::shared_ptr<Schema> current_schema, ManifestContent content);

  ~ManifestEntryAdapter() override;

  virtual Status Append(const ManifestEntry& entry) = 0;

  const std::shared_ptr<Schema>& schema() const { return manifest_schema_; }

  ManifestContent content() const { return content_; }

  std::optional<int64_t> snapshot_id() const { return snapshot_id_; }

  const std::shared_ptr<PartitionSpec>& partition_spec() const { return partition_spec_; }

  const std::shared_ptr<StructType>& partition_type() const { return partition_type_; }

 protected:
  Status AppendInternal(const ManifestEntry& entry);
  Status AppendDataFile(ArrowArray* array,
                        const std::shared_ptr<StructType>& data_file_type,
                        const DataFile& file);
  static Status AppendPartitionValues(ArrowArray* array,
                                      const std::shared_ptr<StructType>& partition_type,
                                      const PartitionValues& partition_values);

  virtual Result<std::optional<int64_t>> GetSequenceNumber(
      const ManifestEntry& entry) const;
  virtual Result<std::optional<std::string>> GetReferenceDataFile(
      const DataFile& file) const;
  virtual Result<std::optional<int64_t>> GetFirstRowId(const DataFile& file) const;
  virtual Result<std::optional<int64_t>> GetContentOffset(const DataFile& file) const;
  virtual Result<std::optional<int64_t>> GetContentSizeInBytes(
      const DataFile& file) const;

 protected:
  std::optional<int64_t> snapshot_id_;
  std::shared_ptr<PartitionSpec> partition_spec_;
  std::shared_ptr<Schema> current_schema_;
  std::shared_ptr<StructType> partition_type_;
  std::shared_ptr<Schema> manifest_schema_;
  const ManifestContent content_;
};

/// \brief Adapter for appending a list of `ManifestFile`s to an `ArrowArray`.
/// Implemented by different versions with version-specific schemas.
class ICEBERG_EXPORT ManifestFileAdapter : public ManifestAdapter {
 public:
  ManifestFileAdapter() = default;
  ~ManifestFileAdapter() override;

  virtual Status Append(const ManifestFile& file) = 0;

  const std::shared_ptr<Schema>& schema() const { return manifest_list_schema_; }

  virtual std::optional<int64_t> next_row_id() const { return std::nullopt; }

 protected:
  Status AppendInternal(const ManifestFile& file);
  static Status AppendPartitionSummary(
      ArrowArray* array, const std::shared_ptr<ListType>& summary_type,
      const std::vector<PartitionFieldSummary>& summaries);

  virtual Result<int64_t> GetSequenceNumber(const ManifestFile& file) const;
  virtual Result<int64_t> GetMinSequenceNumber(const ManifestFile& file) const;
  virtual Result<std::optional<int64_t>> GetFirstRowId(const ManifestFile& file) const;

 protected:
  std::shared_ptr<Schema> manifest_list_schema_;
};

}  // namespace iceberg
