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

#include "iceberg/transform.h"

#include <format>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

TEST(TransformTest, Transform) {
  auto transform = Transform::Identity();
  EXPECT_EQ(TransformType::kIdentity, transform->transform_type());
  EXPECT_EQ("identity", transform->ToString());
  EXPECT_EQ("identity", std::format("{}", *transform));

  auto source_type = std::make_shared<StringType>();
  auto identity_transform = transform->Bind(source_type);
  ASSERT_TRUE(identity_transform);

  ArrowArray arrow_array;
  auto result = identity_transform.value()->Transform(arrow_array);
  ASSERT_FALSE(result);
  EXPECT_EQ(ErrorKind::kNotImplemented, result.error().kind);
  EXPECT_EQ("IdentityTransform::Transform", result.error().message);
}

TEST(TransformFunctionTest, CreateBucketTransform) {
  constexpr int32_t bucket_count = 8;
  auto transform = Transform::Bucket(bucket_count);
  EXPECT_EQ("bucket[8]", transform->ToString());
  EXPECT_EQ("bucket[8]", std::format("{}", *transform));

  const auto transformPtr = transform->Bind(std::make_shared<StringType>());
  ASSERT_TRUE(transformPtr);
  EXPECT_EQ(transformPtr.value()->transform_type(), TransformType::kBucket);
}

TEST(TransformFunctionTest, CreateTruncateTransform) {
  constexpr int32_t width = 16;
  auto transform = Transform::Truncate(width);
  EXPECT_EQ("truncate[16]", transform->ToString());
  EXPECT_EQ("truncate[16]", std::format("{}", *transform));

  auto transformPtr = transform->Bind(std::make_shared<StringType>());
  EXPECT_EQ(transformPtr.value()->transform_type(), TransformType::kTruncate);
}
TEST(TransformFromStringTest, PositiveCases) {
  struct Case {
    std::string str;
    TransformType type;
    std::optional<int32_t> param;
  };

  const std::vector<Case> cases = {
      {.str = "identity", .type = TransformType::kIdentity, .param = std::nullopt},
      {.str = "year", .type = TransformType::kYear, .param = std::nullopt},
      {.str = "month", .type = TransformType::kMonth, .param = std::nullopt},
      {.str = "day", .type = TransformType::kDay, .param = std::nullopt},
      {.str = "hour", .type = TransformType::kHour, .param = std::nullopt},
      {.str = "void", .type = TransformType::kVoid, .param = std::nullopt},
      {.str = "bucket[16]", .type = TransformType::kBucket, .param = 16},
      {.str = "truncate[32]", .type = TransformType::kTruncate, .param = 32},
  };
  for (const auto& c : cases) {
    auto result = TransformFromString(c.str);
    ASSERT_TRUE(result.has_value()) << "Failed to parse: " << c.str;

    const auto& transform = result.value();
    EXPECT_EQ(transform->transform_type(), c.type);
    if (c.param.has_value()) {
      EXPECT_EQ(transform->ToString(),
                std::format("{}[{}]", TransformTypeToString(c.type), *c.param));
    } else {
      EXPECT_EQ(transform->ToString(), TransformTypeToString(c.type));
    }
  }
}

TEST(TransformFromStringTest, NegativeCases) {
  constexpr std::array<std::string_view, 6> invalid_cases = {
      "bucket",           // missing param
      "bucket[]",         // empty param
      "bucket[abc]",      // invalid number
      "unknown",          // unsupported transform
      "bucket[16",        // missing closing bracket
      "truncate[1]extra"  // extra characters
  };

  for (const auto& str : invalid_cases) {
    auto result = TransformFromString(str);
    EXPECT_FALSE(result.has_value()) << "Unexpected success for: " << str;
    EXPECT_EQ(result.error().kind, ErrorKind::kInvalidArgument);
  }
}

}  // namespace iceberg
