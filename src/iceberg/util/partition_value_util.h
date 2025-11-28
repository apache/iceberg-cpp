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

/// \file iceberg/row/partition_values.h
/// Wrapper classes for partition value related data structures.

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/row/partition_values.h"

namespace iceberg {

constexpr size_t kHashPrime = 0x9e3779b9;

using PartitionKey = std::pair<int32_t, PartitionValues>;

/// \brief Lightweight lookup key for heterogeneous lookup without copying
/// PartitionValues.
struct PartitionKeyRef {
  int32_t spec_id;
  const PartitionValues& values;

  PartitionKeyRef(int32_t id, const PartitionValues& vals) : spec_id(id), values(vals) {}

  explicit PartitionKeyRef(const PartitionKey& key)
      : spec_id(key.first), values(key.second) {}
};

/// \brief Hash functor for PartitionValues.
struct PartitionValuesHash {
  std::size_t operator()(const PartitionValues& partition) const noexcept {
    std::size_t hash = 0;
    LiteralHash literal_hash;
    for (const auto& literal : partition.values()) {
      hash ^= literal_hash(literal) + kHashPrime + (hash << 6) + (hash >> 2);
    }
    return hash;
  }
};

/// \brief Equality functor for PartitionValues.
struct PartitionValuesEqual {
  bool operator()(const PartitionValues& lhs, const PartitionValues& rhs) const {
    return lhs == rhs;
  }
};

/// \brief Transparent hash functor for PartitionKey with heterogeneous lookup support.
struct PartitionKeyHash {
  using is_transparent = void;

  std::size_t operator()(const PartitionKey& key) const noexcept {
    std::size_t hash = std::hash<int32_t>{}(key.first);
    hash ^= PartitionValuesHash{}(key.second) + kHashPrime + (hash << 6) + (hash >> 2);
    return hash;
  }

  std::size_t operator()(const PartitionKeyRef& key) const noexcept {
    std::size_t hash = std::hash<int32_t>{}(key.spec_id);
    hash ^= PartitionValuesHash{}(key.values) + kHashPrime + (hash << 6) + (hash >> 2);
    return hash;
  }
};

/// \brief Transparent equality functor for PartitionKey with heterogeneous lookup
/// support.
struct PartitionKeyEqual {
  using is_transparent = void;

  // Equality for PartitionKey vs PartitionKey
  bool operator()(const PartitionKey& lhs, const PartitionKey& rhs) const {
    return lhs.first == rhs.first && lhs.second == rhs.second;
  }

  // Equality for PartitionKey vs PartitionKeyRef (heterogeneous lookup)
  bool operator()(const PartitionKey& lhs, const PartitionKeyRef& rhs) const {
    return lhs.first == rhs.spec_id && lhs.second == rhs.values;
  }

  // Equality for PartitionKeyRef vs PartitionKey (heterogeneous lookup)
  bool operator()(const PartitionKeyRef& lhs, const PartitionKey& rhs) const {
    return lhs.spec_id == rhs.first && lhs.values == rhs.second;
  }

  // Equality for PartitionKeyRef vs PartitionKeyRef (heterogeneous lookup)
  bool operator()(const PartitionKeyRef& lhs, const PartitionKeyRef& rhs) const {
    return lhs.spec_id == rhs.spec_id && lhs.values == rhs.values;
  }
};

/// \brief A map that uses a pair of spec ID and partition tuple as keys.
///
/// \tparam V the type of values
template <typename V>
class PartitionMap {
 public:
  using map_type =
      std::unordered_map<PartitionKey, V, PartitionKeyHash, PartitionKeyEqual>;
  using iterator = typename map_type::iterator;
  using const_iterator = typename map_type::const_iterator;

  PartitionMap() = default;

  /// \brief Get the number of entries in the map.
  size_t size() const { return map_.size(); }

  /// \brief Check if the map is empty.
  bool empty() const { return map_.empty(); }

  /// \brief Clear all entries from the map.
  void clear() { map_.clear(); }

  /// \brief Check if the map contains a key.
  /// \param spec_id The partition spec ID.
  /// \param values The partition values.
  /// \return true if the key exists, false otherwise.
  bool contains(int32_t spec_id, const PartitionValues& values) const {
    return map_.contains(PartitionKeyRef{spec_id, values});
  }

  /// \brief Get the value associated with a key.
  /// \param spec_id The partition spec ID.
  /// \param values The partition values.
  /// \return Reference to the value if found, std::nullopt otherwise.
  std::optional<std::reference_wrapper<V>> get(int32_t spec_id,
                                               const PartitionValues& values) {
    auto it = map_.find(PartitionKeyRef{spec_id, values});
    return it != map_.end() ? std::make_optional(std::ref(it->second)) : std::nullopt;
  }

  /// \brief Get the value associated with a key (const version).
  /// \param spec_id The partition spec ID.
  /// \param values The partition values.
  /// \return Reference to the value if found, std::nullopt otherwise.
  std::optional<std::reference_wrapper<const V>> get(
      int32_t spec_id, const PartitionValues& values) const {
    auto it = map_.find(PartitionKeyRef{spec_id, values});
    return it != map_.end() ? std::make_optional(std::cref(it->second)) : std::nullopt;
  }

  /// \brief Insert or update a value in the map.
  /// \param spec_id The partition spec ID.
  /// \param values The partition values.
  /// \param value The value to insert.
  /// \return true if the entry was updated, false if it was inserted.
  bool put(int32_t spec_id, PartitionValues values, V value) {
    auto it = map_.find(PartitionKeyRef{spec_id, values});
    if (it != map_.end()) {
      it->second = std::move(value);
      return true;
    }
    map_.emplace(PartitionKey{spec_id, std::move(values)}, std::move(value));
    return false;
  }

  /// \brief Remove an entry from the map.
  /// \param spec_id The partition spec ID.
  /// \param values The partition values.
  /// \return true if the entry was removed, false if it didn't exist.
  bool remove(int32_t spec_id, const PartitionValues& values) {
    auto it = map_.find(PartitionKeyRef{spec_id, values});
    if (it != map_.end()) {
      map_.erase(it);
      return true;
    }
    return false;
  }

  /// \brief Get iterator to the beginning.
  iterator begin() { return map_.begin(); }
  const_iterator begin() const { return map_.begin(); }
  const_iterator cbegin() const { return map_.cbegin(); }

  /// \brief Get iterator to the end.
  iterator end() { return map_.end(); }
  const_iterator end() const { return map_.end(); }
  const_iterator cend() const { return map_.cend(); }

 private:
  map_type map_;
};

/// \brief A set that uses a pair of spec ID and partition tuple as elements.
class PartitionSet {
 public:
  using set_type = std::unordered_set<PartitionKey, PartitionKeyHash, PartitionKeyEqual>;
  using iterator = typename set_type::iterator;
  using const_iterator = typename set_type::const_iterator;

  PartitionSet() = default;

  /// \brief Get the number of elements in the set.
  size_t size() const { return set_.size(); }

  /// \brief Check if the set is empty.
  bool empty() const { return set_.empty(); }

  /// \brief Clear all elements from the set.
  void clear() { set_.clear(); }

  /// \brief Check if the set contains an element.
  /// \param spec_id The partition spec ID.
  /// \param values The partition values.
  /// \return true if the element exists, false otherwise.
  bool contains(int32_t spec_id, const PartitionValues& values) const {
    return set_.contains(PartitionKeyRef{spec_id, values});
  }

  /// \brief Add an element to the set.
  /// \param spec_id The partition spec ID.
  /// \param values The partition values.
  /// \return true if the element was added, false if it already existed.
  bool add(int32_t spec_id, PartitionValues values) {
    auto [_, inserted] = set_.emplace(spec_id, std::move(values));
    return inserted;
  }

  /// \brief Remove an element from the set.
  /// \param spec_id The partition spec ID.
  /// \param values The partition values.
  /// \return true if the element was removed, false if it didn't exist.
  bool remove(int32_t spec_id, const PartitionValues& values) {
    auto it = set_.find(PartitionKeyRef{spec_id, values});
    if (it != set_.end()) {
      set_.erase(it);
      return true;
    }
    return false;
  }

  /// \brief Get iterator to the beginning.
  iterator begin() { return set_.begin(); }
  const_iterator begin() const { return set_.begin(); }
  const_iterator cbegin() const { return set_.cbegin(); }

  /// \brief Get iterator to the end.
  iterator end() { return set_.end(); }
  const_iterator end() const { return set_.end(); }
  const_iterator cend() const { return set_.cend(); }

 private:
  set_type set_;
};

}  // namespace iceberg
