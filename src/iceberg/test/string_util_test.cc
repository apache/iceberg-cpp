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

#include "iceberg/util/string_util.h"

#include <gtest/gtest.h>

namespace iceberg {

TEST(StringUtilsTest, ToLower) {
  ASSERT_EQ(StringUtils::ToLower("AbC"), "abc");
  ASSERT_EQ(StringUtils::ToLower("A-bC"), "a-bc");
  ASSERT_EQ(StringUtils::ToLower("A_bC"), "a_bc");
  ASSERT_EQ(StringUtils::ToLower(""), "");
  ASSERT_EQ(StringUtils::ToLower(" "), " ");
  ASSERT_EQ(StringUtils::ToLower("123"), "123");
}

TEST(StringUtilsTest, ToUpper) {
  ASSERT_EQ(StringUtils::ToUpper("abc"), "ABC");
  ASSERT_EQ(StringUtils::ToUpper("A-bC"), "A-BC");
  ASSERT_EQ(StringUtils::ToUpper("A_bC"), "A_BC");
  ASSERT_EQ(StringUtils::ToUpper(""), "");
  ASSERT_EQ(StringUtils::ToUpper(" "), " ");
  ASSERT_EQ(StringUtils::ToUpper("123"), "123");
}

// Non-ASCII (multibyte UTF-8) bytes have the high bit set, i.e. are negative
// when stored in a signed char. Passing them straight to std::tolower/toupper is
// undefined behavior. Conversion only touches ASCII; multibyte bytes pass through
// unchanged. See https://github.com/apache/iceberg-cpp/issues/613.
TEST(StringUtilsTest, NonAsciiPassThrough) {
  ASSERT_EQ(StringUtils::ToLower("Naïve"), "naïve");
  ASSERT_EQ(StringUtils::ToUpper("café"), "CAFé");
  // Pure non-ASCII input is returned verbatim.
  ASSERT_EQ(StringUtils::ToLower("日本語"), "日本語");
  ASSERT_EQ(StringUtils::ToUpper("日本語"), "日本語");
}

TEST(StringUtilsTest, EqualsIgnoreCase) {
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("AbC", "abc"));
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("", ""));
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("abc", "abcd"));
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("abc", "abd"));
  // ASCII case is folded; non-ASCII bytes are compared as-is (no UB).
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("Café", "café"));
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("café", "cafe"));
}

}  // namespace iceberg
