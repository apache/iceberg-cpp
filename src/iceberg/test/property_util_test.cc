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

#include "iceberg/util/property_util.h"

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <gtest/gtest.h>

namespace iceberg {

namespace {

constexpr std::string_view kKey = "test.enabled";

std::unordered_map<std::string, std::string> Properties(std::string_view value) {
  return {{std::string(kKey), std::string(value)}};
}

}  // namespace

TEST(PropertyUtilTest, BooleanDefaultsWhenPropertyIsAbsent) {
  const std::unordered_map<std::string, std::string> empty;
  EXPECT_TRUE(PropertyUtil::PropertyAsBoolean(empty, kKey, true));
  EXPECT_FALSE(PropertyUtil::PropertyAsBoolean(empty, kKey, false));
  EXPECT_EQ(PropertyUtil::PropertyAsOptionalBoolean(empty, kKey), std::nullopt);
}

TEST(PropertyUtilTest, BooleanIgnoresCase) {
  for (const auto* value : {"true", "TRUE", "TrUe"}) {
    EXPECT_TRUE(PropertyUtil::PropertyAsBoolean(Properties(value), kKey, false)) << value;
    EXPECT_EQ(PropertyUtil::PropertyAsOptionalBoolean(Properties(value), kKey), true)
        << value;
  }
  for (const auto* value : {"false", "FALSE", "FaLsE"}) {
    EXPECT_FALSE(PropertyUtil::PropertyAsBoolean(Properties(value), kKey, true)) << value;
    EXPECT_EQ(PropertyUtil::PropertyAsOptionalBoolean(Properties(value), kKey), false)
        << value;
  }
}

TEST(PropertyUtilTest, NonBooleanValueReadsAsFalse) {
  for (const auto* value : {"", " true", "yes", "1", "ture"}) {
    EXPECT_FALSE(PropertyUtil::PropertyAsBoolean(Properties(value), kKey, true)) << value;
    EXPECT_EQ(PropertyUtil::PropertyAsOptionalBoolean(Properties(value), kKey), false)
        << value;
  }
}

}  // namespace iceberg
