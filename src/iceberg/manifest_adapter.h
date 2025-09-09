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

/// \file iceberg/metadata_adapter.h
/// Base class of adapter for v1v2v3v4 metadata.

#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

// \brief Base class to append manifest metadata to Arrow array.
class ICEBERG_EXPORT ManifestAdapter {
 public:
  ManifestAdapter() = default;
  virtual ~ManifestAdapter() = default;
  virtual Status Init() = 0;

  Status StartAppending();
  Result<ArrowArray> FinishAppending();
  int64_t size() const { return size_; }
  virtual std::shared_ptr<Schema> schema() const = 0;

 protected:
  Status AppendField(ArrowArray* arrowArray, int64_t value);
  Status AppendField(ArrowArray* arrowArray, uint64_t value);
  Status AppendField(ArrowArray* arrowArray, double value);
  Status AppendField(ArrowArray* arrowArray, std::string_view value);
  Status AppendField(ArrowArray* arrowArray, const std::vector<uint8_t>& value);

 protected:
  bool is_initialized_ = false;
  ArrowArray array_;
  ArrowSchema schema_;  // converted from manifest_schema_ or manifest_list_schema_
  int64_t size_ = 0;
};

// \brief Implemented by different versions with different schemas to
// append a list of `ManifestEntry`s to an `ArrowArray`.
class ICEBERG_EXPORT ManifestEntryAdapter : public ManifestAdapter {
 public:
  explicit ManifestEntryAdapter(std::shared_ptr<Schema> partition_schema,
                                std::shared_ptr<PartitionSpec> partition_spec)
      : partition_spec_(std::move(partition_spec)),
        partition_schema_(std::move(partition_schema)) {}
  ~ManifestEntryAdapter() override;

  virtual Status Append(const ManifestEntry& entry) = 0;

  std::shared_ptr<Schema> schema() const override { return manifest_schema_; }

 protected:
  virtual std::shared_ptr<StructType> GetManifestEntryStructType();
  Status InitSchema(const std::unordered_set<int32_t>& fields_ids);
  Status AppendInternal(const ManifestEntry& entry);
  Status AppendDataFile(ArrowArray* arrow_array,
                        const std::shared_ptr<StructType>& data_file_type,
                        const std::shared_ptr<DataFile>& file);
  Status AppendPartitions(ArrowArray* arrow_array,
                          const std::shared_ptr<StructType>& partition_type,
                          const std::vector<Literal>& partitions);
  Status AppendList(ArrowArray* arrow_array, const std::vector<int32_t>& list_value);
  Status AppendList(ArrowArray* arrow_array, const std::vector<int64_t>& list_value);
  Status AppendMap(ArrowArray* arrow_array, const std::map<int32_t, int64_t>& map_value);
  Status AppendMap(ArrowArray* arrow_array,
                   const std::map<int32_t, std::vector<uint8_t>>& map_value);

  virtual Result<std::optional<int64_t>> GetWrappedSequenceNumber(
      const ManifestEntry& entry);
  virtual Result<std::optional<std::string>> GetWrappedReferenceDataFile(
      const std::shared_ptr<DataFile>& file);
  virtual Result<std::optional<int64_t>> GetWrappedFirstRowId(
      const std::shared_ptr<DataFile>& file);
  virtual Result<std::optional<int64_t>> GetWrappedContentOffset(
      const std::shared_ptr<DataFile>& file);
  virtual Result<std::optional<int64_t>> GetWrappedContentSizeInBytes(
      const std::shared_ptr<DataFile>& file);

 protected:
  std::shared_ptr<PartitionSpec> partition_spec_;
  std::shared_ptr<Schema> partition_schema_;
  std::shared_ptr<Schema> manifest_schema_;
  std::unordered_map<std::string, std::string> metadata_;
};

// \brief Implemented by different versions with different schemas to
// append a list of `ManifestFile`s to an `ArrowArray`.
class ICEBERG_EXPORT ManifestFileAdapter : public ManifestAdapter {
 public:
  ManifestFileAdapter() = default;
  ~ManifestFileAdapter() override;

  virtual Status Append(const ManifestFile& file) = 0;

  std::shared_ptr<Schema> schema() const override { return manifest_list_schema_; }

 protected:
  Status InitSchema(const std::unordered_set<int32_t>& fields_ids);
  Status AppendInternal(const ManifestFile& file);
  Status AppendPartitions(ArrowArray* arrow_array,
                          const std::shared_ptr<ListType>& partition_type,
                          const std::vector<PartitionFieldSummary>& partitions);

  virtual Result<int64_t> GetWrappedSequenceNumber(const ManifestFile& file);
  virtual Result<int64_t> GetWrappedMinSequenceNumber(const ManifestFile& file);
  virtual Result<std::optional<int64_t>> GetWrappedFirstRowId(const ManifestFile& file);

 protected:
  std::shared_ptr<Schema> manifest_list_schema_;
  std::unordered_map<std::string, std::string> metadata_;
};

}  // namespace iceberg
