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
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "iceberg/util/macros.h"

namespace iceberg::internal {

// A LRU (Least recently used) replacement cache
template <typename Key, typename Value>
class LruCache {
 public:
  explicit LruCache(int32_t capacity) : capacity_(capacity) {
    // The map size can temporarily exceed the cache capacity, see Replace()
    map_.reserve(capacity_ + 1);
  }

  LruCache(const LruCache&) = delete;
  LruCache& operator=(const LruCache&) = delete;
  LruCache(LruCache&&) noexcept = default;
  LruCache& operator=(LruCache&&) noexcept = default;

  void Clear() {
    items_.clear();
    map_.clear();
    // The C++ spec doesn't tell whether map_.clear() will shrink the map capacity
    map_.reserve(capacity_ + 1);
  }

  int32_t size() const {
    ICEBERG_DCHECK(items_.size() == map_.size(), "Cache item and map sizes differ");
    return static_cast<int32_t>(items_.size());
  }

  template <typename K>
  Value* Find(K&& key) {
    const auto it = map_.find(key);
    if (it == map_.end()) {
      return nullptr;
    }

    // Found => move item at front of the list
    auto list_it = it->second;
    items_.splice(items_.begin(), items_, list_it);
    return &list_it->value;
  }

  template <typename K, typename V>
  std::pair<bool, Value*> Replace(K&& key, V&& value) {
    // Try to insert temporary iterator
    auto [it, inserted] = map_.emplace(std::forward<K>(key), ListIt{});
    if (inserted) {
      // Inserted => push item at front of the list, and update iterator
      items_.emplace_front(&it->first, std::forward<V>(value));
      it->second = items_.begin();
      // Did we exceed the cache capacity?  If so, remove least recently used item
      if (static_cast<int32_t>(items_.size()) > capacity_) {
        const bool erased = map_.erase(*items_.back().key);
        ICEBERG_DCHECK(erased, "Failed to erase least recently used cache item");
        static_cast<void>(erased);
        items_.pop_back();
      }
      return {true, &it->second->value};
    }

    // Already exists => move item at front of the list, and update value
    auto list_it = it->second;
    items_.splice(items_.begin(), items_, list_it);
    list_it->value = std::forward<V>(value);
    return {false, &list_it->value};
  }

 private:
  struct Item {
    // Pointer to the key inside the unordered_map
    const Key* key;
    Value value;
  };
  using List = std::list<Item>;
  using ListIt = typename List::iterator;

  const int32_t capacity_;
  // In most to least recently used order
  std::list<Item> items_;
  std::unordered_map<Key, ListIt> map_;
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
    return *cache_.Replace(std::forward<K>(key), std::move(value)).second;
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
    return *cache_.Replace(std::forward<K>(key), std::move(value)).second;
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

// Apply a LRU memoization cache to a callable.
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
