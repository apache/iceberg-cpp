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

/// \file iceberg/util/data_file_set.h
/// A set of DataFile pointers with insertion order preserved and deduplicated by file
/// path.

#include <iterator>
#include <memory>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

/// \brief A set of DataFile pointers with insertion order preserved and deduplicated by
/// file path.
class ICEBERG_EXPORT DataFileSet {
 public:
  using value_type = std::shared_ptr<DataFile>;
  using iterator = typename std::vector<value_type>::iterator;
  using const_iterator = typename std::vector<value_type>::const_iterator;
  using difference_type = typename std::vector<value_type>::difference_type;

  DataFileSet() = default;

  /// \brief Insert a data file into the set.
  /// \param file The data file to insert
  /// \return A pair with an iterator to the inserted element (or the existing one) and
  ///         a bool indicating whether insertion took place
  std::pair<iterator, bool> insert(const value_type& file) { return InsertImpl(file); }

  /// \brief Insert a data file into the set (move version).
  std::pair<iterator, bool> insert(value_type&& file) {
    return InsertImpl(std::move(file));
  }

  /// \brief Get the number of elements in the set.
  size_t size() const { return elements_.size(); }

  /// \brief Check if the set is empty.
  bool empty() const { return elements_.empty(); }

  /// \brief Clear all elements from the set.
  void clear() {
    elements_.clear();
    index_by_path_.clear();
  }

  /// \brief Get iterator to the beginning.
  iterator begin() { return elements_.begin(); }
  const_iterator begin() const { return elements_.begin(); }
  const_iterator cbegin() const { return elements_.cbegin(); }

  /// \brief Get iterator to the end.
  iterator end() { return elements_.end(); }
  const_iterator end() const { return elements_.end(); }
  const_iterator cend() const { return elements_.cend(); }

  /// \brief Get a non-owning view of the data files in insertion order.
  std::span<const value_type> as_span() const { return elements_; }

 private:
  std::pair<iterator, bool> InsertImpl(value_type file) {
    if (!file) {
      return {elements_.end(), false};
    }

    auto [index_iter, inserted] =
        index_by_path_.try_emplace(file->file_path, elements_.size());
    if (!inserted) {
      auto pos = static_cast<difference_type>(index_iter->second);
      return {elements_.begin() + pos, false};
    }

    elements_.push_back(std::move(file));
    return {std::prev(elements_.end()), true};
  }

  // Vector to preserve insertion order
  std::vector<value_type> elements_;
  std::unordered_map<std::string_view, size_t, StringHash, StringEqual> index_by_path_;
};

}  // namespace iceberg
