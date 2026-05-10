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

#include "iceberg/util/lazy.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/test/matchers.h"

namespace {

struct NonDefaultConstructible {
  explicit NonDefaultConstructible(int value) : value(value) {}
  NonDefaultConstructible() = delete;

  int value;
};

iceberg::Result<NonDefaultConstructible> InitNonDefaultConstructible(int value) {
  return NonDefaultConstructible(value);
}

iceberg::Result<int> InitAlwaysFails() { return iceberg::Invalid("init failed"); }

}  // namespace

TEST(LazyTest, SupportsNonDefaultConstructibleValues) {
  const iceberg::Lazy<InitNonDefaultConstructible> lazy;

  auto first = lazy.Get(42);
  ASSERT_THAT(first, iceberg::IsOk());
  EXPECT_EQ(first->get().value, 42);

  auto second = lazy.Get(13);
  ASSERT_THAT(second, iceberg::IsOk());
  EXPECT_EQ(second->get().value, 42);
  EXPECT_EQ(&first->get(), &second->get());
}

TEST(LazyTest, ReusesInitializationError) {
  const iceberg::Lazy<InitAlwaysFails> lazy;

  auto first = lazy.Get();
  EXPECT_THAT(first, iceberg::IsError(iceberg::ErrorKind::kInvalid));
  EXPECT_THAT(first, iceberg::HasErrorMessage("init failed"));

  auto second = lazy.Get();
  EXPECT_THAT(second, iceberg::IsError(iceberg::ErrorKind::kInvalid));
  EXPECT_THAT(second, iceberg::HasErrorMessage("init failed"));
}
