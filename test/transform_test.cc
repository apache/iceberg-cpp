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

#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"

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
  Transform transform{TransformType::kBucket, bucket_count};
  EXPECT_EQ("bucket[8]", transform.ToString());
  EXPECT_EQ("bucket[8]", std::format("{}", transform));

  const auto transformPtr = transform.Bind(std::make_shared<StringType>());
  ASSERT_TRUE(transformPtr);
  EXPECT_EQ(transformPtr.value()->transform_type(), TransformType::kBucket);
}

TEST(TransformFunctionTest, CreateTruncateTransform) {
  constexpr int32_t width = 16;
  Transform transform{TransformType::kTruncate, width};
  EXPECT_EQ("truncate[16]", transform.ToString());
  EXPECT_EQ("truncate[16]", std::format("{}", transform));

  auto transformPtr = transform.Bind(std::make_shared<StringType>());
  EXPECT_EQ(transformPtr.value()->transform_type(), TransformType::kTruncate);
}

}  // namespace iceberg
