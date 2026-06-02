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
/// Sets of DataFile pointers with insertion order preserved and Iceberg-compatible
/// file identity deduplication.

#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <string>
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
  DataFileSet(const DataFileSet& other) : elements_(other.elements_) { RebuildIndex(); }
  DataFileSet& operator=(const DataFileSet& other) {
    if (this != &other) {
      elements_ = other.elements_;
      RebuildIndex();
    }
    return *this;
  }
  DataFileSet(DataFileSet&&) noexcept = default;
  DataFileSet& operator=(DataFileSet&&) noexcept = default;

  /// \brief Insert a data file into the set.
  /// \param file The data file to insert
  /// \return A pair with an iterator to the inserted element (or the existing one) and
  ///         a bool indicating whether insertion took place
  std::pair<iterator, bool> insert(const value_type& file) { return InsertImpl(file); }

  /// \brief Insert a data file into the set (move version).
  std::pair<iterator, bool> insert(value_type&& file) {
    return InsertImpl(std::move(file));
  }

  /// \brief Returns whether an equivalent data file exists in the set.
  bool contains(const DataFile& file) const {
    return index_by_path_.contains(file.file_path);
  }

  /// \brief Returns whether an equivalent data file exists in the set.
  bool contains(const value_type& file) const {
    return file != nullptr && contains(*file);
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

  void RebuildIndex() {
    index_by_path_.clear();
    for (size_t i = 0; i < elements_.size(); ++i) {
      if (elements_[i] != nullptr) {
        index_by_path_.try_emplace(elements_[i]->file_path, i);
      }
    }
  }

  // Vector to preserve insertion order
  std::vector<value_type> elements_;
  std::unordered_map<std::string_view, size_t, StringHash, StringEqual> index_by_path_;
};

/// \brief A set of delete-file pointers deduplicated by delete-file identity.
///
/// Delete files, especially deletion vectors, are identified by location plus
/// content offset and content size. This mirrors Java's DeleteFileSet behavior.
class ICEBERG_EXPORT DeleteFileSet {
 public:
  using value_type = std::shared_ptr<DataFile>;
  using iterator = typename std::vector<value_type>::iterator;
  using const_iterator = typename std::vector<value_type>::const_iterator;
  using difference_type = typename std::vector<value_type>::difference_type;

  DeleteFileSet() = default;
  DeleteFileSet(const DeleteFileSet& other) : elements_(other.elements_) {
    RebuildIndex();
  }
  DeleteFileSet& operator=(const DeleteFileSet& other) {
    if (this != &other) {
      elements_ = other.elements_;
      RebuildIndex();
    }
    return *this;
  }
  DeleteFileSet(DeleteFileSet&&) noexcept = default;
  DeleteFileSet& operator=(DeleteFileSet&&) noexcept = default;

  /// \brief Insert a delete file into the set.
  /// \param file The delete file to insert
  /// \return A pair with an iterator to the inserted element (or the existing one) and
  ///         a bool indicating whether insertion took place
  std::pair<iterator, bool> insert(const value_type& file) { return InsertImpl(file); }

  /// \brief Insert a delete file into the set (move version).
  std::pair<iterator, bool> insert(value_type&& file) {
    return InsertImpl(std::move(file));
  }

  /// \brief Returns whether an equivalent delete file exists in the set.
  bool contains(const DataFile& file) const {
    return index_by_file_.contains(DeleteFileKey(file));
  }

  /// \brief Returns whether an equivalent delete file exists in the set.
  bool contains(const value_type& file) const {
    return file != nullptr && contains(*file);
  }

  /// \brief Get the number of elements in the set.
  size_t size() const { return elements_.size(); }

  /// \brief Check if the set is empty.
  bool empty() const { return elements_.empty(); }

  /// \brief Clear all elements from the set.
  void clear() {
    elements_.clear();
    index_by_file_.clear();
  }

  /// \brief Get iterator to the beginning.
  iterator begin() { return elements_.begin(); }
  const_iterator begin() const { return elements_.begin(); }
  const_iterator cbegin() const { return elements_.cbegin(); }

  /// \brief Get iterator to the end.
  iterator end() { return elements_.end(); }
  const_iterator end() const { return elements_.end(); }
  const_iterator cend() const { return elements_.cend(); }

  /// \brief Get a non-owning view of the delete files in insertion order.
  std::span<const value_type> as_span() const { return elements_; }

 private:
  struct DeleteFileKey {
    explicit DeleteFileKey(const DataFile& file)
        : path(file.file_path),
          content_offset(file.content_offset),
          content_size_in_bytes(file.content_size_in_bytes) {}

    std::string path;
    std::optional<int64_t> content_offset;
    std::optional<int64_t> content_size_in_bytes;

    bool operator==(const DeleteFileKey& other) const = default;
  };

  struct DeleteFileKeyHash {
    size_t operator()(const DeleteFileKey& key) const {
      size_t hash = std::hash<std::string>{}(key.path);
      auto combine = [&hash](const auto& value) {
        size_t value_hash = value.has_value() ? std::hash<int64_t>{}(*value) : 0;
        hash ^= value_hash + 0x9e3779b9 + (hash << 6) + (hash >> 2);
      };
      combine(key.content_offset);
      combine(key.content_size_in_bytes);
      return hash;
    }
  };

  std::pair<iterator, bool> InsertImpl(value_type file) {
    if (!file) {
      return {elements_.end(), false};
    }

    auto [index_iter, inserted] =
        index_by_file_.try_emplace(DeleteFileKey(*file), elements_.size());
    if (!inserted) {
      auto pos = static_cast<difference_type>(index_iter->second);
      return {elements_.begin() + pos, false};
    }

    elements_.push_back(std::move(file));
    return {std::prev(elements_.end()), true};
  }

  void RebuildIndex() {
    index_by_file_.clear();
    for (size_t i = 0; i < elements_.size(); ++i) {
      if (elements_[i] != nullptr) {
        index_by_file_.try_emplace(DeleteFileKey(*elements_[i]), i);
      }
    }
  }

  // Vector to preserve insertion order.
  std::vector<value_type> elements_;
  std::unordered_map<DeleteFileKey, size_t, DeleteFileKeyHash> index_by_file_;
};

}  // namespace iceberg
