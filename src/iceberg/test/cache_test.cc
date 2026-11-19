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

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <tuple>

#include <gtest/gtest.h>

#include "iceberg/test/executor.h"
#include "iceberg/util/cache_internal.h"
#include "iceberg/util/task_group.h"

namespace iceberg::internal {

TEST(LruCacheTest, Eviction) {
  LruCache<int32_t, std::string> cache(2);

  EXPECT_TRUE(cache.Replace(1, "one").first);
  EXPECT_TRUE(cache.Replace(2, "two").first);
  ASSERT_NE(cache.Find(1), nullptr);

  EXPECT_TRUE(cache.Replace(3, "three").first);
  EXPECT_EQ(cache.Find(2), nullptr);
  ASSERT_NE(cache.Find(1), nullptr);
  EXPECT_EQ(*cache.Find(1), "one");
  ASSERT_NE(cache.Find(3), nullptr);
  EXPECT_EQ(*cache.Find(3), "three");
  EXPECT_EQ(cache.size(), 2);
  EXPECT_EQ(cache.total_weight(), 2);
}

struct StringLengthWeigher {
  int64_t operator()(int32_t /*key*/, const std::string& value) const {
    return static_cast<int64_t>(value.size());
  }
};

using WeightedCache = LruCache<int32_t, std::string, StringLengthWeigher>;

TEST(LruCacheTest, WeightedEviction) {
  WeightedCache cache(10);

  EXPECT_TRUE(cache.Replace(1, "aaaa").first);
  EXPECT_TRUE(cache.Replace(2, "bbb").first);
  ASSERT_NE(cache.Find(1), nullptr);
  EXPECT_EQ(cache.total_weight(), 7);

  // 7 + 6 exceeds 10, so the least recently used entry 2 is evicted.
  auto [inserted, value] = cache.Replace(3, "cccccc");
  EXPECT_TRUE(inserted);
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(*value, "cccccc");
  EXPECT_EQ(cache.Find(2), nullptr);
  ASSERT_NE(cache.Find(1), nullptr);
  EXPECT_EQ(*cache.Find(1), "aaaa");
  EXPECT_EQ(cache.size(), 2);
  EXPECT_EQ(cache.total_weight(), 10);
}

TEST(LruCacheTest, WeightedEvictionRemovesMultipleEntries) {
  WeightedCache cache(10);
  cache.Replace(1, "aaa");
  cache.Replace(2, "bbb");
  cache.Replace(3, "ccc");

  // Making room for 7 requires evicting the two least recently used entries.
  ASSERT_NE(cache.Replace(4, "ddddddd").second, nullptr);
  EXPECT_EQ(cache.Find(1), nullptr);
  EXPECT_EQ(cache.Find(2), nullptr);
  EXPECT_NE(cache.Find(3), nullptr);
  EXPECT_NE(cache.Find(4), nullptr);
  EXPECT_EQ(cache.size(), 2);
  EXPECT_EQ(cache.total_weight(), 10);
}

TEST(LruCacheTest, OversizedValueIsNotCached) {
  WeightedCache cache(5);
  cache.Replace(1, "aa");

  auto [inserted, value] = cache.Replace(2, "bbbbbb");
  EXPECT_FALSE(inserted);
  EXPECT_EQ(value, nullptr);
  EXPECT_EQ(cache.Find(2), nullptr);
  // Rejecting the oversized value must not evict anything.
  ASSERT_NE(cache.Find(1), nullptr);
  EXPECT_EQ(cache.size(), 1);
  EXPECT_EQ(cache.total_weight(), 2);

  // A value exactly at the maximum weight fits.
  ASSERT_NE(cache.Replace(3, "ccccc").second, nullptr);
  EXPECT_EQ(cache.Find(1), nullptr);
  EXPECT_EQ(cache.total_weight(), 5);
}

TEST(LruCacheTest, ReplaceReweighsEntry) {
  WeightedCache cache(10);
  cache.Replace(1, "aaa");
  cache.Replace(2, "bbb");

  // Growing entry 2 from 3 to 7 still fits next to entry 1.
  EXPECT_FALSE(cache.Replace(2, "bbbbbbb").first);
  EXPECT_EQ(cache.size(), 2);
  EXPECT_EQ(cache.total_weight(), 10);

  // Growing entry 1 to 8 makes it most recently used and evicts entry 2.
  auto [inserted, value] = cache.Replace(1, "aaaaaaaa");
  EXPECT_FALSE(inserted);
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(*value, "aaaaaaaa");
  EXPECT_EQ(cache.Find(2), nullptr);
  EXPECT_EQ(cache.size(), 1);
  EXPECT_EQ(cache.total_weight(), 8);

  // Shrinking releases weight.
  cache.Replace(1, "a");
  EXPECT_EQ(cache.total_weight(), 1);

  // Replacing with an oversized value removes the existing entry.
  std::tie(inserted, value) = cache.Replace(1, std::string(11, 'a'));
  EXPECT_FALSE(inserted);
  EXPECT_EQ(value, nullptr);
  EXPECT_EQ(cache.Find(1), nullptr);
  EXPECT_EQ(cache.size(), 0);
  EXPECT_EQ(cache.total_weight(), 0);
}

TEST(LruCacheTest, ClearResetsWeight) {
  WeightedCache cache(10);
  cache.Replace(1, "aaaa");
  cache.Replace(2, "bbbb");

  cache.Clear();
  EXPECT_EQ(cache.size(), 0);
  EXPECT_EQ(cache.total_weight(), 0);

  ASSERT_NE(cache.Replace(3, std::string(10, 'c')).second, nullptr);
  EXPECT_EQ(cache.total_weight(), 10);
}

TEST(MemoizeLruTest, Caches) {
  std::atomic<int> calls{0};
  auto memoized = MemoizeLru(
      [&calls](int32_t key) {
        ++calls;
        return std::to_string(key);
      },
      2);

  EXPECT_EQ(memoized(1), "1");
  EXPECT_EQ(memoized(2), "2");
  EXPECT_EQ(memoized(1), "1");
  EXPECT_EQ(calls, 2);

  EXPECT_EQ(memoized(3), "3");
  EXPECT_EQ(memoized(2), "2");
  EXPECT_EQ(calls, 4);
}

TEST(MemoizeLruTest, MovesKey) {
  auto memoized = MemoizeLru([](const std::unique_ptr<int32_t>& key) { return *key; }, 2);
  EXPECT_EQ(memoized(std::make_unique<int32_t>(1)), 1);

  auto memoized_thread_unsafe =
      MemoizeLruThreadUnsafe([](const std::unique_ptr<int32_t>& key) { return *key; }, 2);
  EXPECT_EQ(memoized_thread_unsafe(std::make_unique<int32_t>(2)), 2);
}

TEST(MemoizeLruTest, ThreadUnsafe) {
  int32_t calls = 0;
  auto memoized = MemoizeLruThreadUnsafe(
      [&calls](int32_t key) {
        ++calls;
        return std::to_string(key);
      },
      2);

  EXPECT_EQ(memoized(1), "1");
  EXPECT_EQ(memoized(1), "1");
  EXPECT_EQ(calls, 1);
}

TEST(MemoizeLruTest, Threads) {
  auto memoized = MemoizeLru([](int32_t key) { return key * 2; }, 4);
  std::atomic<bool> mismatch{false};
  test::ThreadExecutor executor;
  TaskGroup group;
  group.SetExecutor(std::ref(executor));

  for (int32_t thread_id = 0; thread_id < 4; ++thread_id) {
    group.Submit([&] -> Status {
      for (int32_t i = 0; i < 100; ++i) {
        const int32_t key = i % 8;
        if (memoized(key) != key * 2) {
          mismatch.store(true, std::memory_order_relaxed);
        }
      }
      return {};
    });
  }

  ASSERT_TRUE(std::move(group).Run().has_value());
  EXPECT_FALSE(mismatch.load(std::memory_order_relaxed));
}

}  // namespace iceberg::internal
