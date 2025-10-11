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
#include <span>
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
  Result<ArrowArray*> FinishAppending();
  int64_t size() const { return size_; }

 protected:
  static Status AppendField(ArrowArray* arrowArray, int64_t value);
  static Status AppendField(ArrowArray* arrowArray, uint64_t value);
  static Status AppendField(ArrowArray* arrowArray, double value);
  static Status AppendField(ArrowArray* arrowArray, std::string_view value);
  static Status AppendField(ArrowArray* arrowArray, std::span<const uint8_t> value);

 protected:
  ArrowArray array_;
  ArrowSchema schema_;  // converted from manifest_schema_ or manifest_list_schema_
  int64_t size_ = 0;
  std::unordered_map<std::string, std::string> metadata_;
};

// \brief Implemented by different versions with different schemas to
// append a list of `ManifestEntry`s to an `ArrowArray`.
class ICEBERG_EXPORT ManifestEntryAdapter : public ManifestAdapter {
 public:
  explicit ManifestEntryAdapter(std::shared_ptr<PartitionSpec> partition_spec)
      : partition_spec_(std::move(partition_spec)) {}
  ~ManifestEntryAdapter() override;

  virtual Status Append(const ManifestEntry& entry) = 0;

  const std::shared_ptr<Schema>& schema() const { return manifest_schema_; }

 protected:
  virtual Result<std::shared_ptr<StructType>> GetManifestEntryStructType();

  /// \brief Init version-specific schema for each version.
  ///
  /// \param fields_ids each version of manifest schema has schema, we will init this
  /// schema based on the fields_ids.
  Status InitSchema(const std::unordered_set<int32_t>& fields_ids);
  Status AppendInternal(const ManifestEntry& entry);
  Status AppendDataFile(ArrowArray* arrow_array,
                        const std::shared_ptr<StructType>& data_file_type,
                        const DataFile& file);
  static Status AppendPartition(ArrowArray* arrow_array,
                                const std::shared_ptr<StructType>& partition_type,
                                const std::vector<Literal>& partitions);
  static Status AppendList(ArrowArray* arrow_array,
                           const std::vector<int32_t>& list_value);
  static Status AppendList(ArrowArray* arrow_array,
                           const std::vector<int64_t>& list_value);
  static Status AppendMap(ArrowArray* arrow_array,
                          const std::map<int32_t, int64_t>& map_value);
  static Status AppendMap(ArrowArray* arrow_array,
                          const std::map<int32_t, std::vector<uint8_t>>& map_value);

  virtual Result<std::optional<int64_t>> GetSequenceNumber(const ManifestEntry& entry);
  virtual Result<std::optional<std::string>> GetReferenceDataFile(const DataFile& file);
  virtual Result<std::optional<int64_t>> GetFirstRowId(const DataFile& file);
  virtual Result<std::optional<int64_t>> GetContentOffset(const DataFile& file);
  virtual Result<std::optional<int64_t>> GetContentSizeInBytes(const DataFile& file);

 protected:
  std::shared_ptr<PartitionSpec> partition_spec_;
  std::shared_ptr<Schema> manifest_schema_;
};

// \brief Implemented by different versions with different schemas to
// append a list of `ManifestFile`s to an `ArrowArray`.
class ICEBERG_EXPORT ManifestFileAdapter : public ManifestAdapter {
 public:
  ManifestFileAdapter() = default;
  ~ManifestFileAdapter() override;

  virtual Status Append(const ManifestFile& file) = 0;

  const std::shared_ptr<Schema>& schema() const { return manifest_list_schema_; }

 protected:
  /// \brief Init version-specific schema for each version.
  ///
  /// \param fields_ids each version of manifest schema has schema, we will init this
  /// schema based on the fields_ids.
  Status InitSchema(const std::unordered_set<int32_t>& fields_ids);
  Status AppendInternal(const ManifestFile& file);
  static Status AppendPartitionSummary(
      ArrowArray* arrow_array, const std::shared_ptr<ListType>& summary_type,
      const std::vector<PartitionFieldSummary>& summaries);

  virtual Result<int64_t> GetSequenceNumber(const ManifestFile& file);
  virtual Result<int64_t> GetMinSequenceNumber(const ManifestFile& file);
  virtual Result<std::optional<int64_t>> GetFirstRowId(const ManifestFile& file);

 protected:
  std::shared_ptr<Schema> manifest_list_schema_;
};

}  // namespace iceberg
