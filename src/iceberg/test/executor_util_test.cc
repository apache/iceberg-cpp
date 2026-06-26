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
#include <concepts>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/util/executor_util_internal.h"

namespace iceberg {

using ::testing::ElementsAre;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

namespace {

struct IntTask {
  Result<std::vector<int>> operator()(int) {
    return Result<std::vector<int>>{std::vector<int>{}};
  }
};

static_assert(internal::ParallelCollectible<std::vector<int>&, IntTask>);
static_assert(
    std::same_as<decltype(ParallelCollect(std::nullopt, std::declval<std::vector<int>&>(),
                                          IntTask{})),
                 Result<std::vector<int>>>);
static_assert(internal::ParallelCollectible<decltype(std::views::iota(0, 3)), IntTask>);
static_assert(std::same_as<decltype(ParallelCollect(std::nullopt, std::views::iota(0, 3),
                                                    IntTask{})),
                           Result<std::vector<int>>>);

}  // namespace

TEST(ParallelReduceTest, MergesSets) {
  std::vector<std::unordered_set<int>> values = {{1, 2}, {2, 3}, {}};

  auto result = ParallelReduce<std::unordered_set<int>>::Reduce(values);

  EXPECT_THAT(result, UnorderedElementsAre(1, 2, 3));
}

TEST(ParallelReduceTest, JoinsVectors) {
  std::vector<std::vector<int>> values = {{1, 2}, {}, {3}};

  auto result = ParallelReduce<std::vector<int>>::Reduce(values);

  EXPECT_THAT(result, ElementsAre(1, 2, 3));
}

TEST(ParallelReduceTest, MergesMapsAndAppendsDuplicateVectors) {
  std::vector<std::unordered_map<int, std::vector<std::string>>> values = {
      {{1, {"a"}}, {2, {"b"}}}, {{1, {"c"}}, {3, {"d"}}}};

  auto result =
      ParallelReduce<std::unordered_map<int, std::vector<std::string>>>::Reduce(values);

  EXPECT_THAT(result,
              UnorderedElementsAre(Pair(1, ElementsAre("a", "c")),
                                   Pair(2, ElementsAre("b")), Pair(3, ElementsAre("d"))));
}

TEST(ParallelReduceTest, ReducesPairElements) {
  using Value = std::pair<std::unordered_set<int>, std::vector<std::string>>;

  std::vector<Value> values = {{{1}, {"a"}}, {{2}, {"b"}}};

  auto result = ParallelReduce<Value>::Reduce(values);

  EXPECT_THAT(result.first, UnorderedElementsAre(1, 2));
  EXPECT_THAT(result.second, ElementsAre("a", "b"));
}

TEST(ParallelReduceTest, ReducesTupleElements) {
  using Value = std::tuple<std::unordered_set<int>, std::vector<std::string>>;

  std::vector<Value> values = {{{1}, {"a"}}, {{2}, {"b"}}};

  auto result = ParallelReduce<Value>::Reduce(values);

  EXPECT_THAT(std::get<0>(result), UnorderedElementsAre(1, 2));
  EXPECT_THAT(std::get<1>(result), ElementsAre("a", "b"));
}

TEST(ParallelReduceTest, ReducesViewElements) {
  using Value = std::tuple<std::unordered_set<int>, std::vector<std::string>>;

  std::vector<Value> values = {{{0}, {"skip"}}, {{1}, {"a"}}, {{2}, {"b"}}};

  auto result = ParallelReduce<Value>::Reduce(values | std::views::drop(1));

  EXPECT_THAT(std::get<0>(result), UnorderedElementsAre(1, 2));
  EXPECT_THAT(std::get<1>(result), ElementsAre("a", "b"));
}

TEST(ParallelCollectTest, CollectsSingleRange) {
  std::vector<int> input = {1, 2, 3};

  auto result = ParallelCollect(std::nullopt, input, [](int value) {
    return Result<std::unordered_set<int>>{{value * 2}};
  });

  EXPECT_THAT(result, IsOk());
  EXPECT_THAT(*result, UnorderedElementsAre(2, 4, 6));
}

TEST(ParallelCollectTest, CollectsIotaView) {
  auto input = std::views::iota(1, 4);

  auto result = ParallelCollect(std::nullopt, input, [](int value) {
    return Result<std::unordered_set<int>>{{value * 2}};
  });

  EXPECT_THAT(result, IsOk());
  EXPECT_THAT(*result, UnorderedElementsAre(2, 4, 6));
}

TEST(ParallelCollectTest, CollectsTransformView) {
  std::vector<int> values = {1, 2, 3, 4};
  std::span<const int> files(values);
  auto input = std::views::iota(0, 2) | std::views::transform([files](int index) {
                 return files.subspan(index * 2, 2);
               });

  auto result = ParallelCollect(std::nullopt, input, [](std::span<const int> group) {
    return Result<std::vector<int>>{{group.front(), group.back()}};
  });

  EXPECT_THAT(result, IsOk());
  EXPECT_THAT(*result, ElementsAre(1, 2, 3, 4));
}

TEST(ParallelCollectTest, CollectsMoveOnlyPrvalues) {
  auto input = std::views::iota(1, 4) | std::views::transform([](int value) {
                 return std::make_unique<int>(value);
               });

  auto result = ParallelCollect(std::nullopt, input, [](std::unique_ptr<int> value) {
    return Result<std::vector<int>>{{*value}};
  });

  EXPECT_THAT(result, IsOk());
  EXPECT_THAT(*result, ElementsAre(1, 2, 3));
}

TEST(ParallelCollectTest, KeepsTupleResultFromSingleRange) {
  std::vector<int> input = {1, 2};

  auto result = ParallelCollect(
      std::nullopt, input,
      [](int value)
          -> Result<std::tuple<std::unordered_set<int>, std::vector<std::string>>> {
        return {{{value}, {std::to_string(value)}}};
      });

  EXPECT_THAT(result, IsOk());
  EXPECT_THAT(std::get<0>(*result), UnorderedElementsAre(1, 2));
  EXPECT_THAT(std::get<1>(*result), ElementsAre("1", "2"));
}

TEST(ParallelCollectTest, CollectsMultipleRanges) {
  test::ThreadExecutor executor;
  std::vector<int> left = {1, 2};
  std::vector<std::string> right = {"a", "b"};

  auto result = ParallelCollect(
      std::ref(executor), left,
      [](int value) { return Result<std::unordered_set<int>>{{value}}; }, right,
      [](const std::string& value) { return Result<std::vector<std::string>>{{value}}; });

  EXPECT_THAT(result, IsOk());
  EXPECT_THAT(std::get<0>(*result), UnorderedElementsAre(1, 2));
  EXPECT_THAT(std::get<1>(*result), ElementsAre("a", "b"));
  EXPECT_EQ(executor.submit_count(), 4);
}

TEST(ParallelCollectTest, PropagatesTaskErrors) {
  std::vector<int> input = {1, 2, 3};
  std::atomic<int> calls = 0;

  auto result = ParallelCollect(std::nullopt, input, [&calls](int value) {
    calls.fetch_add(1, std::memory_order_relaxed);
    if (value == 2) {
      return Result<std::vector<int>>{ValidationFailed("bad value")};
    }
    return Result<std::vector<int>>{{value}};
  });

  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(calls.load(std::memory_order_relaxed), 3);
}

}  // namespace iceberg
