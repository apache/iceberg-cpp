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

#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/file_scan_task_stream.h"
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

// Exercises ToVector() with values that can be copied but not moved.
class CopyOnlyStream final : public Stream<CopyOnly> {
 private:
  Result<std::optional<CopyOnly>> NextImpl() override {
    if (next_ == 3) {
      return Result<std::optional<CopyOnly>>(std::in_place, std::nullopt);
    }
    return Result<std::optional<CopyOnly>>(std::in_place, std::in_place, next_++);
  }

  int next_ = 0;
};

// Exercises ToVector() with values that can be moved but not copied.
class MoveOnlyStream final : public Stream<std::unique_ptr<int>> {
 public:
  int calls() const { return calls_; }

 private:
  Result<std::optional<std::unique_ptr<int>>> NextImpl() override {
    ++calls_;
    if (next_ == 3) {
      return Result<std::optional<std::unique_ptr<int>>>(std::in_place, std::nullopt);
    }
    return Result<std::optional<std::unique_ptr<int>>>(std::in_place, std::in_place,
                                                       std::make_unique<int>(next_++));
  }

  int next_ = 0;
  int calls_ = 0;
};

// Exercises ToVector() error propagation after some values have been consumed.
class FailingStream final : public Stream<int> {
 public:
  int calls() const { return calls_; }

 private:
  Result<std::optional<int>> NextImpl() override {
    ++calls_;
    if (next_ < 2) {
      return Result<std::optional<int>>(std::in_place, std::in_place, next_++);
    }
    return Invalid("stream failed");
  }

  int next_ = 0;
  int calls_ = 0;
};

static_assert(std::is_move_constructible_v<CopyOnlyStream>);
static_assert(std::is_move_assignable_v<CopyOnlyStream>);
static_assert(std::is_move_constructible_v<MoveOnlyStream>);
static_assert(std::is_move_assignable_v<MoveOnlyStream>);

TEST(StreamTest, FileScanTaskStreamSupportsIncompleteFileScanTask) {
  static_assert(
      std::is_same_v<FileScanTaskStream, Stream<std::shared_ptr<FileScanTask>>>);
  static_assert(
      std::is_same_v<FileScanTaskStreamPtr, std::unique_ptr<FileScanTaskStream>>);
  FileScanTaskStreamPtr stream;
  EXPECT_EQ(stream, nullptr);
}

TEST(StreamTest, ToVectorSupportsCopyOnlyValues) {
  CopyOnlyStream stream;

  ICEBERG_UNWRAP_OR_FAIL(auto values, stream.ToVector());

  ASSERT_EQ(values.size(), 3);
  EXPECT_EQ(values[0].value(), 0);
  EXPECT_EQ(values[1].value(), 1);
  EXPECT_EQ(values[2].value(), 2);
}

TEST(StreamTest, ToVectorSupportsMoveOnlyValues) {
  MoveOnlyStream stream;

  ICEBERG_UNWRAP_OR_FAIL(auto values, stream.ToVector());

  ASSERT_EQ(values.size(), 3);
  EXPECT_EQ(*values[0], 0);
  EXPECT_EQ(*values[1], 1);
  EXPECT_EQ(*values[2], 2);
}

TEST(StreamTest, NextRemainsAtEndAfterExhaustion) {
  MoveOnlyStream stream;
  ICEBERG_UNWRAP_OR_FAIL(auto values, stream.ToVector());
  ASSERT_EQ(values.size(), 3);
  EXPECT_EQ(stream.calls(), 4);

  for (int i = 0; i < 2; ++i) {
    auto result = stream.Next();
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
  }
  EXPECT_EQ(stream.calls(), 4);
}

TEST(StreamTest, ToVectorPropagatesErrorsAfterPartialConsumption) {
  FailingStream stream;

  ICEBERG_UNWRAP_OR_FAIL(auto first, stream.Next());
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first.value(), 0);

  auto result = stream.ToVector();

  EXPECT_THAT(result, IsError(ErrorKind::kInvalid));
  EXPECT_THAT(result, HasErrorMessage("stream failed"));
}

TEST(StreamTest, NextRepeatsErrorWithoutAdvancing) {
  FailingStream stream;
  auto first_error = stream.ToVector();
  EXPECT_THAT(first_error, IsError(ErrorKind::kInvalid));
  EXPECT_THAT(first_error, HasErrorMessage("stream failed"));
  EXPECT_EQ(stream.calls(), 3);

  for (int i = 0; i < 2; ++i) {
    auto result = stream.Next();
    EXPECT_THAT(result, IsError(ErrorKind::kInvalid));
    EXPECT_THAT(result, HasErrorMessage("stream failed"));
  }
  EXPECT_EQ(stream.calls(), 3);
}

}  // namespace
}  // namespace iceberg
