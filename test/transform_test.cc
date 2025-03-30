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

#include "iceberg/util/formatter.h"

namespace iceberg {

TEST(TransformTest, TransformFunction) {
  class TestTransformFunction : public TransformFunction {
   public:
    TestTransformFunction() : TransformFunction(TransformType::kUnknown) {}
    expected<ArrowArray, Error> Transform(const ArrowArray& input) override {
      return unexpected(
          Error{.kind = ErrorKind::kNotSupported, .message = "test transform function"});
    }
  };

  TestTransformFunction transform;
  EXPECT_EQ(TransformType::kUnknown, transform.transform_type());
  EXPECT_EQ("unknown", transform.ToString());
  EXPECT_EQ("unknown", std::format("{}", transform));

  ArrowArray arrow_array;
  auto result = transform.Transform(arrow_array);
  ASSERT_FALSE(result);
  EXPECT_EQ(ErrorKind::kNotSupported, result.error().kind);
  EXPECT_EQ("test transform function", result.error().message);
}

}  // namespace iceberg
