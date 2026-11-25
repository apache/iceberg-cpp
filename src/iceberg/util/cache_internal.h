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

// Borrowed the file from Apache Arrow:
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/cache_internal.h

#pragma once

/// \file iceberg/util/cache_internal.h
/// \brief Provide internal LRU cache and memoization helpers.

#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "iceberg/util/macros.h"

namespace iceberg::internal {

// Gives every entry a weight of one, so that the maximum weight of a cache is the
// maximum number of entries.
struct UnitWeigher {
  template <typename Key, typename Value>
  constexpr int64_t operator()(const Key& /*key*/,
                               const Value& /*value*/) const noexcept {
    return 1;
  }
};

// A LRU (Least recently used) replacement cache
//
// The combined weight of the cached entries never exceeds `max_weight`; least recently
// used entries are evicted first to make room. `Weigher` computes the weight of an entry
// from its key and value when the entry is inserted or replaced, e.g. its size in bytes.
// Weights must be non-negative. With the default UnitWeigher, `max_weight` is the
// maximum number of entries. Storage grows as entries are inserted.
template <typename Key, typename Value, typename Weigher = UnitWeigher,
          typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class LruCache {
 public:
  explicit LruCache(int64_t max_weight, Weigher weigher = Weigher{})
      : max_weight_(max_weight), weigher_(std::move(weigher)) {
    ICEBERG_DCHECK(max_weight_ >= 0, "Cache maximum weight must be non-negative");
  }

  LruCache(const LruCache&) = delete;
  LruCache& operator=(const LruCache&) = delete;
  LruCache(LruCache&&) noexcept = default;
  LruCache& operator=(LruCache&&) noexcept = default;

  void Clear() {
    items_.clear();
    map_.clear();
    total_weight_ = 0;
  }

  // Number of retained entries.
  int32_t size() const {
    ICEBERG_DCHECK(items_.size() == map_.size(), "Cache item and map sizes differ");
    return static_cast<int32_t>(items_.size());
  }

  // Combined weight of retained entries.
  int64_t total_weight() const { return total_weight_; }

  // Maximum combined weight allowed in this cache.
  int64_t max_weight() const { return max_weight_; }

  // Apply a new limit immediately, evicting least recently used entries as needed.
  void Resize(int64_t max_weight) {
    ICEBERG_DCHECK(max_weight >= 0, "Cache maximum weight must be non-negative");
    max_weight_ = max_weight;
    while (total_weight_ > max_weight_) {
      EvictLeastRecentlyUsed();
    }
  }

  template <typename K>
  const Value* Find(K&& key) {
    const auto it = map_.find(key);
    if (it == map_.end()) {
      return nullptr;
    }

    // Found => move item at front of the list
    auto list_it = it->second;
    items_.splice(items_.begin(), items_, list_it);
    return &list_it->value;
  }

  template <typename K>
  void Erase(const K& key) {
    const auto it = map_.find(key);
    if (it == map_.end()) {
      return;
    }
    total_weight_ -= it->second->weight;
    items_.erase(it->second);
    map_.erase(it);
  }

  // Insert or update the value for `key`, mark it as most recently used, and evict least
  // recently used entries until the cache is within its maximum weight again.
  //
  // Returns whether a new entry was added, and a pointer to the cached value.  A value
  // heavier than the maximum weight is not cached: any existing entry for `key` is
  // removed, other entries are kept, and {false, nullptr} is returned.
  // Use Replace to change a cached value. A successful replacement invalidates the
  // previous value pointer. If weighing throws, the cache retains its previous state.
  template <typename K, typename V>
  std::pair<bool, const Value*> Replace(K&& key, V&& value) {
    List candidate;
    auto& item = candidate.emplace_front(nullptr, std::forward<V>(value));
    auto [it, inserted] = map_.try_emplace(std::forward<K>(key));
    item.key = &it->first;
    try {
      item.weight = weigher_(*item.key, std::as_const(item.value));
    } catch (...) {
      if (inserted) {
        map_.erase(it);
      }
      throw;
    }
    ICEBERG_DCHECK(item.weight >= 0, "Cache entry weight must be non-negative");

    if (!inserted) {
      total_weight_ -= it->second->weight;
      items_.erase(it->second);
    }
    if (item.weight > max_weight_) {
      map_.erase(it);
      return {false, nullptr};
    }

    items_.splice(items_.begin(), candidate);
    it->second = items_.begin();
    // Subtract from the limit before adding the new weight to avoid overflow.
    while (total_weight_ > max_weight_ - item.weight) {
      EvictLeastRecentlyUsed();
    }
    total_weight_ += item.weight;
    return {inserted, &item.value};
  }

 private:
  struct Item {
    // Pointer to the key inside the unordered_map
    const Key* key;
    Value value;
    int64_t weight = 0;
  };
  using List = std::list<Item>;
  using ListIt = typename List::iterator;

  void EvictLeastRecentlyUsed() {
    ICEBERG_DCHECK(!items_.empty(), "Cannot evict from an empty cache");
    const Item& item = items_.back();
    total_weight_ -= item.weight;
    const bool erased = map_.erase(*item.key);
    ICEBERG_DCHECK(erased, "Failed to erase least recently used cache item");
    static_cast<void>(erased);
    items_.pop_back();
  }

  int64_t max_weight_;
  Weigher weigher_;
  int64_t total_weight_ = 0;
  // In most to least recently used order
  std::list<Item> items_;
  std::unordered_map<Key, ListIt, Hash, KeyEqual> map_;
};

template <typename Key, typename Value, typename Cache, typename Func>
struct ThreadSafeMemoizer {
  using RetType = Value;

  template <typename F>
  ThreadSafeMemoizer(F&& func, int32_t cache_capacity)
      : func_(std::forward<F>(func)), cache_(cache_capacity) {}

  // The memoizer can't return a pointer to the cached value, because
  // the cache entry may be evicted by another thread.
  template <typename K>
  RetType operator()(K&& key) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (const Value* value_ptr = cache_.Find(key); value_ptr != nullptr) {
      return *value_ptr;
    }
    lock.unlock();
    Value value = func_(key);
    lock.lock();
    const auto* cached = cache_.Replace(std::forward<K>(key), std::move(value)).second;
    if (cached == nullptr) {
      throw std::invalid_argument("Memoization cache capacity must be positive");
    }
    return *cached;
  }

 private:
  std::mutex mutex_;
  Func func_;
  Cache cache_;
};

template <typename Key, typename Value, typename Cache, typename Func>
struct ThreadUnsafeMemoizer {
  using RetType = const Value&;

  template <typename F>
  ThreadUnsafeMemoizer(F&& func, int32_t cache_capacity)
      : func_(std::forward<F>(func)), cache_(cache_capacity) {}

  template <typename K>
  RetType operator()(K&& key) {
    if (const Value* value_ptr = cache_.Find(key); value_ptr != nullptr) {
      return *value_ptr;
    }
    Value value = func_(key);
    const auto* cached = cache_.Replace(std::forward<K>(key), std::move(value)).second;
    if (cached == nullptr) {
      throw std::invalid_argument("Memoization cache capacity must be positive");
    }
    return *cached;
  }

 private:
  Func func_;
  Cache cache_;
};

template <typename T>
struct unary_traits;

template <typename R, typename Arg>
struct unary_traits<std::function<R(Arg)>> {
  using arg = Arg;
  using return_type = R;
};

template <template <typename...> class MemoizerType, typename Func>
auto Memoize(Func&& func, int32_t cache_capacity) {
  using Function = decltype(std::function{std::forward<Func>(func)});
  using Key = std::decay_t<typename unary_traits<Function>::arg>;
  using Value = std::decay_t<std::invoke_result_t<Func, const Key&>>;
  using Memoizer = MemoizerType<Key, Value, LruCache<Key, Value>, Func>;
  return Memoizer(std::forward<Func>(func), cache_capacity);
}

// Apply a LRU memoization cache to a callable. Calls require positive capacity.
template <typename Func>
auto MemoizeLru(Func&& func, int32_t cache_capacity) {
  return Memoize<ThreadSafeMemoizer>(std::forward<Func>(func), cache_capacity);
}

// Like MemoizeLru, but not thread-safe.  This version allows for much faster
// lookups (more than 2x faster), but you'll have to manage thread safety yourself.
// A recommended usage is to declare per-thread caches using `thread_local`.
template <typename Func>
auto MemoizeLruThreadUnsafe(Func&& func, int32_t cache_capacity) {
  return Memoize<ThreadUnsafeMemoizer>(std::forward<Func>(func), cache_capacity);
}

}  // namespace iceberg::internal
