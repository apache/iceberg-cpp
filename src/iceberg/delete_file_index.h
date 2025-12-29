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

/// \file iceberg/delete_file_index.h
/// An index of delete files by sequence number.

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "iceberg/expression/literal.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

namespace internal {

/// \brief Wrapper for equality delete files that caches converted bounds.
struct ICEBERG_EXPORT EqualityDeleteFile {
  const Schema* schema;
  ManifestEntry wrapped;
  int64_t apply_sequence_number;  // = data_sequence_number - 1

  // Lazily converted bounds for pruning
  mutable std::unordered_map<int32_t, Literal> lower_bounds;
  mutable std::unordered_map<int32_t, Literal> upper_bounds;
  mutable bool bounds_converted = false;

  EqualityDeleteFile(const Schema* schema, ManifestEntry&& entry)
      : schema(schema),
        wrapped(std::move(entry)),
        apply_sequence_number(wrapped.sequence_number.value() - 1) {}

  /// \brief Check if this delete file has both lower and upper bounds.
  bool HasLowerAndUpperBounds() const {
    return !wrapped.data_file->lower_bounds.empty() &&
           !wrapped.data_file->upper_bounds.empty();
  }

  /// \brief Get the lower bound for a field ID.
  Result<std::optional<std::reference_wrapper<const Literal>>> LowerBound(
      int32_t id) const {
    ICEBERG_RETURN_UNEXPECTED(ConvertBoundsIfNeeded());
    auto it = lower_bounds.find(id);
    return it != lower_bounds.cend() ? std::make_optional(std::cref(it->second))
                                     : std::nullopt;
  }

  /// \brief Get the upper bound for a field ID.
  Result<std::optional<std::reference_wrapper<const Literal>>> UpperBound(
      int32_t id) const {
    ICEBERG_RETURN_UNEXPECTED(ConvertBoundsIfNeeded());
    auto it = upper_bounds.find(id);
    return it != upper_bounds.cend() ? std::make_optional(std::cref(it->second))
                                     : std::nullopt;
  }

 private:
  /// \brief Convert bounds from binary to Literal. Implemented in .cc file.
  Status ConvertBoundsIfNeeded() const;
};

/// \brief Check if two ranges overlap.
inline bool RangesOverlap(const Literal& data_lower, const Literal& data_upper,
                          const Literal& delete_lower, const Literal& delete_upper) {
  if (data_lower > delete_upper) {
    return false;
  }
  if (delete_lower > data_upper) {
    return false;
  }
  return true;
}

/// \brief Check if a value count map indicates all values are null.
inline bool AllNull(const std::map<int32_t, int64_t>& null_counts,
                    const std::map<int32_t, int64_t>& value_counts, int32_t field_id,
                    bool is_required) {
  if (is_required) {
    return false;
  }

  auto null_it = null_counts.find(field_id);
  auto value_it = value_counts.find(field_id);
  if (null_it == null_counts.cend() || value_it == value_counts.cend()) {
    return false;
  }

  return null_it->second == value_it->second;
}

/// \brief Check if all values are non-null.
inline bool AllNonNull(const std::map<int32_t, int64_t>& null_counts, int32_t field_id,
                       bool is_required) {
  if (is_required) {
    return true;
  }

  auto it = null_counts.find(field_id);
  if (it == null_counts.cend()) {
    return false;
  }

  return it->second <= 0;
}

/// \brief Check if the column contains any null values.
inline bool ContainsNull(const std::map<int32_t, int64_t>& null_counts, int32_t field_id,
                         bool is_required) {
  if (is_required) {
    return false;
  }

  auto it = null_counts.find(field_id);
  if (it == null_counts.cend()) {
    return true;  // Unknown, assume may contain null
  }

  return it->second > 0;
}

/// \brief Check if an equality delete file can contain deletes for a data file.
ICEBERG_EXPORT Result<bool> CanContainEqDeletesForFile(
    const DataFile& data_file, const EqualityDeleteFile& delete_file);

/// \brief A group of position delete files sorted by the sequence number they apply to.
///
/// Position delete files apply to data files with a sequence number <= the delete
/// file's data sequence number.
class ICEBERG_EXPORT PositionDeletes {
 public:
  PositionDeletes() = default;

  /// \brief Add a position delete file to this group.
  [[nodiscard]] Status Add(ManifestEntry&& entry);

  /// \brief Returns all delete files with data_sequence_number >= the given sequence
  /// number.
  std::vector<std::shared_ptr<DataFile>> Filter(int64_t seq);

  /// \brief Get all delete files in this group.
  std::vector<std::shared_ptr<DataFile>> ReferencedDeleteFiles();

  /// \brief Check if this group is empty.
  bool empty() const { return files_.empty(); }

 private:
  void IndexIfNeeded();

  std::vector<ManifestEntry> files_;
  std::vector<int64_t> seqs_;
  bool indexed_ = false;
};

/// \brief A group of equality delete files sorted by apply sequence number.
///
/// Equality deletes apply to data files with sequence number < the delete's
/// data sequence number (i.e., apply_sequence_number = data_sequence_number - 1).
class ICEBERG_EXPORT EqualityDeletes {
 public:
  explicit EqualityDeletes(const Schema& schema) : schema_(schema) {}

  /// \brief Add an equality delete file to this group.
  [[nodiscard]] Status Add(ManifestEntry&& entry);

  /// \brief Filter equality deletes that apply to the given data file.
  ///
  /// Returns delete files where:
  /// 1. apply_sequence_number >= the data file's sequence number
  /// 2. The delete file's bounds may overlap with the data file
  Result<std::vector<std::shared_ptr<DataFile>>> Filter(int64_t seq,
                                                        const DataFile& data_file);

  /// \brief Get all delete files in this group.
  std::vector<std::shared_ptr<DataFile>> ReferencedDeleteFiles();

  /// \brief Check if this group is empty.
  bool empty() const { return files_.empty(); }

 private:
  void IndexIfNeeded();

  const Schema& schema_;
  std::vector<EqualityDeleteFile> files_;
  std::vector<int64_t> seqs_;
  bool indexed_ = false;
};

}  // namespace internal

/// \brief An index of delete files by sequence number.
///
/// Use `DeleteFileIndex::Builder` to construct an index, and `ForDataFile()`
/// or `ForEntry()` to get the delete files to apply to a given data file.
///
/// The index organizes delete files by:
/// - Global equality deletes (apply to all partitions)
/// - Partitioned equality deletes (apply to specific partitions)
/// - Partitioned position deletes (apply to specific partitions)
/// - File-scoped position deletes (apply to specific data files)
/// - Deletion vectors (DVs) that reference specific data files
class ICEBERG_EXPORT DeleteFileIndex {
 public:
  class Builder;

  ~DeleteFileIndex();

  DeleteFileIndex(DeleteFileIndex&&) noexcept;
  DeleteFileIndex& operator=(DeleteFileIndex&&) noexcept;
  DeleteFileIndex(const DeleteFileIndex&) = delete;
  DeleteFileIndex& operator=(const DeleteFileIndex&) = delete;

  /// \brief Check if this index is empty (has no delete files).
  bool empty() const;

  /// \brief Check if this index has any equality delete files.
  bool has_equality_deletes() const;

  /// \brief Check if this index has any position delete files.
  bool has_position_deletes() const;

  /// \brief Get all delete files referenced by this index.
  /// TODO(gangwu): use lazy iterator to avoid large memory allocation.
  std::vector<std::shared_ptr<DataFile>> ReferencedDeleteFiles() const;

  /// \brief Get the delete files that apply to a manifest entry.
  ///
  /// \param entry The manifest entry to find delete files for
  /// \return Delete files that should be applied when reading the data file
  Result<std::vector<std::shared_ptr<DataFile>>> ForEntry(
      const ManifestEntry& entry) const;

  /// \brief Get the delete files that apply to a data file with a specific sequence
  /// number.
  ///
  /// \param sequence_number The data sequence number of the data file
  /// \param file The data file to find delete files for
  /// \return Delete files that should be applied when reading the data file
  Result<std::vector<std::shared_ptr<DataFile>>> ForDataFile(int64_t sequence_number,
                                                             const DataFile& file) const;

  /// \brief Create a builder for constructing a DeleteFileIndex from manifest files.
  ///
  /// \param io The FileIO to use for reading manifests
  /// \param delete_manifests The delete manifests to index
  /// \return A Builder instance
  static Result<Builder> BuilderFor(std::shared_ptr<FileIO> io,
                                    std::vector<ManifestFile> delete_manifests);

 private:
  friend class Builder;

  // Private constructor used by Builder
  DeleteFileIndex(
      std::unique_ptr<internal::EqualityDeletes> global_deletes,
      std::unique_ptr<PartitionMap<std::unique_ptr<internal::EqualityDeletes>>>
          eq_deletes_by_partition,
      std::unique_ptr<PartitionMap<std::unique_ptr<internal::PositionDeletes>>>
          pos_deletes_by_partition,
      std::unique_ptr<
          std::unordered_map<std::string, std::unique_ptr<internal::PositionDeletes>>>
          pos_deletes_by_path,
      std::unique_ptr<std::unordered_map<std::string, ManifestEntry>> dv_by_path);

  // Helper methods for finding delete files
  Result<std::vector<std::shared_ptr<DataFile>>> FindGlobalDeletes(
      int64_t seq, const DataFile& data_file) const;
  Result<std::vector<std::shared_ptr<DataFile>>> FindEqPartitionDeletes(
      int64_t seq, const DataFile& data_file) const;
  Result<std::vector<std::shared_ptr<DataFile>>> FindPosPartitionDeletes(
      int64_t seq, const DataFile& data_file) const;
  Result<std::vector<std::shared_ptr<DataFile>>> FindPathDeletes(
      int64_t seq, const DataFile& data_file) const;
  Result<std::shared_ptr<DataFile>> FindDV(int64_t seq, const DataFile& data_file) const;

  // Index data structures
  std::unique_ptr<internal::EqualityDeletes> global_deletes_;
  std::unique_ptr<PartitionMap<std::unique_ptr<internal::EqualityDeletes>>>
      eq_deletes_by_partition_;
  std::unique_ptr<PartitionMap<std::unique_ptr<internal::PositionDeletes>>>
      pos_deletes_by_partition_;
  std::unique_ptr<
      std::unordered_map<std::string, std::unique_ptr<internal::PositionDeletes>>>
      pos_deletes_by_path_;
  std::unique_ptr<std::unordered_map<std::string, ManifestEntry>> dv_by_path_;

  bool has_eq_deletes_ = false;
  bool has_pos_deletes_ = false;
  bool is_empty_ = true;
};

class ICEBERG_EXPORT DeleteFileIndex::Builder : public ErrorCollector {
 public:
  /// \brief Construct a builder from manifest files.
  Builder(std::shared_ptr<FileIO> io, std::vector<ManifestFile> delete_manifests);

  ~Builder() override;

  Builder(Builder&&) noexcept;
  Builder& operator=(Builder&&) noexcept;
  Builder(const Builder&) = delete;
  Builder& operator=(const Builder&) = delete;

  /// \brief Set the partition specs by ID.
  Builder& SpecsById(
      std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id);

  /// \brief Set the table schema.
  ///
  /// Required for filtering and expression evaluation.
  Builder& WithSchema(std::shared_ptr<Schema> schema);

  /// \brief Set the minimum sequence number for delete files.
  ///
  /// Only delete files with sequence number > min_sequence_number will be included.
  Builder& AfterSequenceNumber(int64_t seq);

  /// \brief Set a row-level data filter.
  ///
  /// This filter is projected to partition expressions for manifest pruning and
  /// then residuals are applied to data files.
  Builder& DataFilter(std::shared_ptr<Expression> filter);

  /// \brief Set a partition filter expression.
  Builder& PartitionFilter(std::shared_ptr<Expression> filter);

  /// \brief Set a partition set to filter manifests.
  Builder& FilterPartitions(std::shared_ptr<PartitionSet> partition_set);

  /// \brief Set case sensitivity for column name matching.
  Builder& CaseSensitive(bool case_sensitive);

  /// \brief Ignore residual expressions after partition filtering.
  Builder& IgnoreResiduals();

  /// \brief Build the DeleteFileIndex.
  Result<std::unique_ptr<DeleteFileIndex>> Build();

 private:
  // Load delete files from manifests
  Result<std::vector<ManifestEntry>> LoadDeleteFiles();

  // Add a DV to the index
  Status AddDV(std::unordered_map<std::string, ManifestEntry>& dv_by_path,
               ManifestEntry&& entry);

  // Add a position delete file to the index
  Status AddPositionDelete(
      std::unordered_map<std::string, std::unique_ptr<internal::PositionDeletes>>&
          deletes_by_path,
      PartitionMap<std::unique_ptr<internal::PositionDeletes>>& deletes_by_partition,
      ManifestEntry&& entry);

  // Add an equality delete file to the index
  Status AddEqualityDelete(
      internal::EqualityDeletes& global_deletes,
      PartitionMap<std::unique_ptr<internal::EqualityDeletes>>& deletes_by_partition,
      ManifestEntry&& entry);

  std::shared_ptr<FileIO> io_;
  std::vector<ManifestFile> delete_manifests_;
  int64_t min_sequence_number_ = 0;
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Expression> data_filter_;
  std::shared_ptr<Expression> partition_filter_;
  std::shared_ptr<PartitionSet> partition_set_;
  bool case_sensitive_ = true;
  bool ignore_residuals_ = false;
};

}  // namespace iceberg
