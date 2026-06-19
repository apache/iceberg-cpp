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

// Non-ASCII strings are written as explicit UTF-8 byte escapes so the test does not
// depend on the source-file encoding. An escape is split before a following hex digit
// (e.g. "...\x9E" "E") so the \x does not absorb it.
// See https://github.com/apache/iceberg-cpp/issues/613.
TEST(StringUtilsTest, ToLowerUnicode) {
  // "CAFÉ" -> "café" (É U+00C9 = 0xC3 0x89 -> é U+00E9 = 0xC3 0xA9).
  ASSERT_EQ(StringUtils::ToLower("CAF\xC3\x89"), "caf\xC3\xA9");
  // "GROẞE" -> "große": capital sharp S (ẞ U+1E9E) lower-cases to ß (U+00DF), not "ss"
  // as casefolding would produce.
  ASSERT_EQ(StringUtils::ToLower("GRO\xE1\xBA\x9E"
                                 "E"),
            "gro\xC3\x9F"
            "e");
  // "日本語" has no case mapping and is returned verbatim.
  ASSERT_EQ(StringUtils::ToLower("\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E"),
            "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E");
  // Invalid UTF-8 (a lone 0xFF byte) is returned unchanged rather than erroring.
  ASSERT_EQ(StringUtils::ToLower("\xFF"), "\xFF");
}

// ToUpper is intentionally ASCII-only; non-ASCII (multibyte UTF-8) bytes pass through.
TEST(StringUtilsTest, ToUpperAsciiOnly) {
  // "café" -> "CAFé" (é stays unchanged).
  ASSERT_EQ(StringUtils::ToUpper("caf\xC3\xA9"), "CAF\xC3\xA9");
  ASSERT_EQ(StringUtils::ToUpper("\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E"),
            "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E");
}

TEST(StringUtilsTest, EqualsIgnoreCase) {
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("AbC", "abc"));
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("", ""));
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("abc", "abcd"));
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("abc", "abd"));
  // Unicode-aware: "CAFÉ" matches "café".
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("CAF\xC3\x89", "caf\xC3\xA9"));
  // "GROẞE" matches "große" under lowercasing (ẞ -> ß).
  ASSERT_TRUE(
      StringUtils::EqualsIgnoreCase("GRO\xE1\xBA\x9E"
                                    "E",
                                    "gro\xC3\x9F"
                                    "e"));
  // Different letters still differ ("café" vs "cafe").
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("caf\xC3\xA9", "cafe"));
}

}  // namespace iceberg
