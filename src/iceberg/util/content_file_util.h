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

/// \file iceberg/util/content_file_util.h
/// Utility functions for content files (data files and delete files).

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Hash functor for std::shared_ptr<DataFile> based on file path.
struct ICEBERG_EXPORT DataFilePtrHash {
  size_t operator()(const std::shared_ptr<DataFile>& file) const {
    if (!file) {
      return 0;
    }
    return std::hash<std::string>{}(file->file_path);
  }
};

/// \brief Equality functor for std::shared_ptr<DataFile> based on file path.
struct ICEBERG_EXPORT DataFilePtrEqual {
  bool operator()(const std::shared_ptr<DataFile>& left,
                  const std::shared_ptr<DataFile>& right) const {
    if (left == right) {
      return true;
    }
    if (!left || !right) {
      return false;
    }
    return left->file_path == right->file_path;
  }
};

/// \brief A set of DataFile pointers, deduplicated by file path.
///
/// This preserves insertion order, which is important for row ID assignment in v3
/// manifests. Similar to Java's DataFileSet which uses LinkedHashSet to maintain
/// insertion order.
class ICEBERG_EXPORT DataFileSet {
 public:
  using value_type = std::shared_ptr<DataFile>;
  using iterator = typename std::vector<value_type>::iterator;
  using const_iterator = typename std::vector<value_type>::const_iterator;

  DataFileSet() = default;

  /// \brief Insert a data file into the set.
  /// \param file The data file to insert
  /// \return A pair with an iterator to the inserted element (or the existing one) and
  ///         a bool indicating whether insertion took place
  std::pair<iterator, bool> insert(const value_type& file) {
    if (!file) {
      return {elements_.end(), false};
    }
    // Check if file already exists using the hash set for O(1) lookup
    auto [hash_iter, hash_inserted] = hash_set_.insert(file);
    if (!hash_inserted) {
      // File already exists, find it in the vector using the element from hash_set_
      const auto& existing_file = *hash_iter;
      auto vec_iter = std::ranges::find_if(elements_, [&existing_file](const auto& elem) {
        return DataFilePtrEqual{}(elem, existing_file);
      });
      return {vec_iter, false};
    }
    elements_.push_back(*hash_iter);
    return {std::prev(elements_.end()), true};
  }

  /// \brief Insert a data file into the set (move version).
  std::pair<iterator, bool> insert(value_type&& file) {
    if (!file) {
      return {elements_.end(), false};
    }
    // Check if file already exists
    auto [hash_iter, hash_inserted] = hash_set_.insert(file);
    if (!hash_inserted) {
      // File already exists, find it in the vector using the element from hash_set_
      const auto& existing_file = *hash_iter;
      auto vec_iter = std::ranges::find_if(elements_, [&existing_file](const auto& elem) {
        return DataFilePtrEqual{}(elem, existing_file);
      });
      return {vec_iter, false};
    }
    elements_.push_back(*hash_iter);
    return {std::prev(elements_.end()), true};
  }

  /// \brief Get the number of elements in the set.
  size_t size() const { return elements_.size(); }

  /// \brief Check if the set is empty.
  bool empty() const { return elements_.empty(); }

  /// \brief Clear all elements from the set.
  void clear() {
    elements_.clear();
    hash_set_.clear();
  }

  /// \brief Get iterator to the beginning.
  iterator begin() { return elements_.begin(); }
  const_iterator begin() const { return elements_.begin(); }
  const_iterator cbegin() const { return elements_.cbegin(); }

  /// \brief Get iterator to the end.
  iterator end() { return elements_.end(); }
  const_iterator end() const { return elements_.end(); }
  const_iterator cend() const { return elements_.cend(); }

 private:
  // Vector to preserve insertion order
  std::vector<value_type> elements_;
  // Hash set for O(1) duplicate detection
  std::unordered_set<value_type, DataFilePtrHash, DataFilePtrEqual> hash_set_;
};

/// \brief Utility functions for content files.
struct ICEBERG_EXPORT ContentFileUtil {
  /// \brief Check if a delete file is a deletion vector (DV).
  static bool IsDV(const DataFile& file);

  /// \brief Get the referenced data file path from a position delete file.
  static Result<std::optional<std::string>> ReferencedDataFile(const DataFile& file);

  /// \brief Check if a delete file is file-scoped.
  static Result<bool> IsFileScoped(const DataFile& file);

  /// \brief Check if a collection of delete files contains exactly one DV.
  static bool ContainsSingleDV(std::span<const std::shared_ptr<DataFile>> files);

  /// \brief Generate a description string for a deletion vector.
  static std::string DVDesc(const DataFile& file);

  /// \brief In-place drop stats.
  static void DropAllStats(DataFile& data_file);

  /// \brief Preserve stats based on selected columns.
  static void DropUnselectedStats(DataFile& data_file,
                                  const std::unordered_set<int32_t>& selected_columns);
};

}  // namespace iceberg
