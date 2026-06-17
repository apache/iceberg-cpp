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

// Non-ASCII (multibyte UTF-8) bytes have the high bit set, i.e. are negative when stored
// in a signed char. Only ASCII letters are converted; multibyte bytes pass through
// unchanged. The non-ASCII strings are written as explicit UTF-8 byte escapes so the test
// does not depend on the source-file encoding. See
// https://github.com/apache/iceberg-cpp/issues/613.
TEST(StringUtilsTest, NonAsciiPassThrough) {
  // "Naïve" -> "naïve" (ï = U+00EF = 0xC3 0xAF; only the ASCII letters change).
  ASSERT_EQ(StringUtils::ToLower("Na\xC3\xAFve"), "na\xC3\xAFve");
  // "café" -> "CAFé" (é = U+00E9 = 0xC3 0xA9 stays unchanged).
  ASSERT_EQ(StringUtils::ToUpper("caf\xC3\xA9"), "CAF\xC3\xA9");
  // "日本語" (0xE6 0x97 0xA5 0xE6 0x9C 0xAC 0xE8 0xAA 0x9E) is returned verbatim.
  ASSERT_EQ(StringUtils::ToLower("\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E"),
            "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E");
  ASSERT_EQ(StringUtils::ToUpper("\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E"),
            "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E");
}

TEST(StringUtilsTest, EqualsIgnoreCase) {
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("AbC", "abc"));
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("", ""));
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("abc", "abcd"));
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("abc", "abd"));
  // ASCII case is folded; non-ASCII bytes are compared as-is. ("Café" vs "café")
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("Caf\xC3\xA9", "caf\xC3\xA9"));
  // "café" vs "cafe": the multibyte é differs from ASCII 'e'.
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("caf\xC3\xA9", "cafe"));
}

}  // namespace iceberg
