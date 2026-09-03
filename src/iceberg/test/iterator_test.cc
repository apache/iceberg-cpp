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

#include "iceberg/util/iterator.h"

#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg {
namespace {

class CopyOnly {
 public:
  explicit CopyOnly(int value) : value_(value) {}

  CopyOnly(const CopyOnly&) = default;
  CopyOnly& operator=(const CopyOnly&) = default;
  CopyOnly(CopyOnly&&) = delete;
  CopyOnly& operator=(CopyOnly&&) = delete;

  int value() const { return value_; }

 private:
  int value_;
};

static_assert(std::is_copy_constructible_v<CopyOnly>);
static_assert(!std::is_move_constructible_v<CopyOnly>);

class CopyOnlyIterator final : public Iterator<CopyOnly> {
 public:
  Result<std::optional<CopyOnly>> Next() override {
    if (next_ == 3) {
      return Result<std::optional<CopyOnly>>(std::in_place, std::nullopt);
    }
    return Result<std::optional<CopyOnly>>(std::in_place, std::in_place, next_++);
  }

 private:
  int next_ = 0;
};

class MoveOnlyIterator final : public Iterator<std::unique_ptr<int>> {
 public:
  Result<std::optional<std::unique_ptr<int>>> Next() override {
    if (next_ == 3) {
      return Result<std::optional<std::unique_ptr<int>>>(std::in_place,
                                                         std::nullopt);
    }
    return Result<std::optional<std::unique_ptr<int>>>(
        std::in_place, std::in_place, std::make_unique<int>(next_++));
  }

 private:
  int next_ = 0;
};

class FailingIterator final : public Iterator<int> {
 public:
  Result<std::optional<int>> Next() override { return Invalid("iteration failed"); }
};

TEST(IteratorTest, ToVectorSupportsCopyOnlyValues) {
  CopyOnlyIterator iterator;

  ICEBERG_UNWRAP_OR_FAIL(auto values, iterator.ToVector());

  ASSERT_EQ(values.size(), 3);
  EXPECT_EQ(values[0].value(), 0);
  EXPECT_EQ(values[1].value(), 1);
  EXPECT_EQ(values[2].value(), 2);
}

TEST(IteratorTest, ToVectorSupportsMoveOnlyValues) {
  MoveOnlyIterator iterator;

  ICEBERG_UNWRAP_OR_FAIL(auto values, iterator.ToVector());

  ASSERT_EQ(values.size(), 3);
  EXPECT_EQ(*values[0], 0);
  EXPECT_EQ(*values[1], 1);
  EXPECT_EQ(*values[2], 2);
}

TEST(IteratorTest, ToVectorPropagatesErrors) {
  FailingIterator iterator;

  auto result = iterator.ToVector();

  EXPECT_THAT(result, IsError(ErrorKind::kInvalid));
  EXPECT_THAT(result, HasErrorMessage("iteration failed"));
}

}  // namespace
}  // namespace iceberg
