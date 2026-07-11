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
  // ASCII prefix before the first non-ASCII byte takes the fast path; the rest goes
  // through utf8proc. "ABÉ" -> "abé".
  ASSERT_EQ(StringUtils::ToLower("AB\xC3\x89"), "ab\xC3\xA9");
  // An invalid UTF-8 byte (a lone 0xFF) passes through unchanged rather than erroring.
  ASSERT_EQ(StringUtils::ToLower("\xFF"), "\xFF");
  // An invalid byte only passes through itself; the valid code points around it are
  // still lower-cased ("AB" 0xFF "CÉ" -> "ab" 0xFF "cé").
  ASSERT_EQ(StringUtils::ToLower("AB\xFF"
                                 "C\xC3\x89"),
            "ab\xFF"
            "c\xC3\xA9");
  // The invalid byte can abut a multi-byte code point with no ASCII between them; 0xFF
  // passes through and the adjacent "É" still lower-cases to "é" (0xFF "É" -> 0xFF "é").
  ASSERT_EQ(StringUtils::ToLower("\xFF\xC3\x89"), "\xFF\xC3\xA9");
  // A truncated multi-byte sequence (0xC3 with no continuation byte) passes through
  // without consuming the bytes after it.
  ASSERT_EQ(StringUtils::ToLower("\xC3"
                                 "AB"),
            "\xC3"
            "ab");
  // A stray continuation byte (0x80) behaves the same way.
  ASSERT_EQ(StringUtils::ToLower("A\x80"
                                 "B"),
            "a\x80"
            "b");
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
  // Fallback correctness: an ASCII operand can equal a non-ASCII one once lower-cased,
  // even though their raw byte lengths differ. "İ" (U+0130 = 0xC4 0xB0, two bytes)
  // lower-cases to one-byte "i", so it must compare equal to "i" and "I".
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("i", "\xC4\xB0"));
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("\xC4\xB0", "I"));
  // The non-ASCII byte can appear after a matching ASCII prefix ("abi" vs "abİ").
  ASSERT_TRUE(StringUtils::EqualsIgnoreCase("abi", "ab\xC4\xB0"));
  // Pure-ASCII operands that share a prefix but differ in length are not equal.
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("abc", "ab"));
  // Operands containing invalid UTF-8 are still compared case-insensitively on their
  // valid parts; the invalid bytes themselves compare verbatim.
  ASSERT_TRUE(
      StringUtils::EqualsIgnoreCase("AB\xFF"
                                    "C",
                                    "ab\xFF"
                                    "c"));
  ASSERT_FALSE(StringUtils::EqualsIgnoreCase("\xFF", "\xFE"));
}

TEST(StringUtilsTest, StartsWithIgnoreCase) {
  ASSERT_TRUE(StringUtils::StartsWithIgnoreCase("AbCdef", "abc"));
  ASSERT_TRUE(StringUtils::StartsWithIgnoreCase("abc", "ABC"));
  ASSERT_FALSE(StringUtils::StartsWithIgnoreCase("abc", "abd"));
  // Empty prefix always matches; a prefix longer than the string does not.
  ASSERT_TRUE(StringUtils::StartsWithIgnoreCase("abc", ""));
  ASSERT_FALSE(StringUtils::StartsWithIgnoreCase("ab", "abcd"));
  // Regression (#760): lower-casing can change byte length, so the prefix must not be
  // matched by byte-slicing. "İ" (U+0130 = 0xC4 0xB0) lower-cases to "i", so "İx"
  // starts with "i" ...
  ASSERT_TRUE(StringUtils::StartsWithIgnoreCase("\xC4\xB0x", "i"));
  // ... and "i" starts with "İ" (both lower-case to "i"), which the old byte-length
  // guard wrongly rejected.
  ASSERT_TRUE(StringUtils::StartsWithIgnoreCase("i", "\xC4\xB0"));
  // A matching Unicode prefix: "CAFÉbar" starts with "café".
  ASSERT_TRUE(
      StringUtils::StartsWithIgnoreCase("CAF\xC3\x89"
                                        "bar",
                                        "caf\xC3\xA9"));
  // Invalid UTF-8 bytes compare verbatim in the prefix as well.
  ASSERT_TRUE(
      StringUtils::StartsWithIgnoreCase("AB\xFF"
                                        "x",
                                        "ab\xFF"));
  ASSERT_FALSE(StringUtils::StartsWithIgnoreCase("ab\xFE", "ab\xFF"));
}

// The ASCII fast paths in EqualsIgnoreCase / StartsWithIgnoreCase must agree with their
// documented ToLower-based semantics for every input, including length-changing case
// mappings and invalid UTF-8. Rather than enumerate cases by hand, exhaustively compare
// both functions against the ToLower oracle over all short strings built from a small
// alphabet that straddles those boundaries. This is the mechanical form of the #760
// regression, where a fast path disagreed with ToLower on a length-changing mapping.
TEST(StringUtilsTest, IgnoreCaseAgreesWithToLowerOracle) {
  // Atoms mix ASCII (upper/lower, including the lowercase targets of the multi-byte
  // mappings) with a 2-byte code point that lower-cases to one byte ("İ" U+0130 -> "i"),
  // a 3-byte one that also shrinks to one byte ("K" U+212A -> "k"), an ordinary 2-byte
  // cased letter ("É"), and an invalid UTF-8 byte.
  const std::vector<std::string> atoms = {
      "a", "I", "i", "k", "\xC4\xB0", "\xE2\x84\xAA", "\xC3\x89", "\xFF"};

  // Build every string of 0..3 atoms, one generation (length) at a time.
  std::vector<std::string> inputs = {""};
  size_t generation_begin = 0;
  for (int len = 0; len < 3; ++len) {
    const size_t generation_end = inputs.size();
    for (size_t i = generation_begin; i < generation_end; ++i) {
      for (const auto& atom : atoms) {
        inputs.push_back(inputs[i] + atom);
      }
    }
    generation_begin = generation_end;
  }

  // Precompute the oracle so the O(n^2) comparison below does not re-lower each string.
  std::vector<std::string> lowered;
  lowered.reserve(inputs.size());
  for (const auto& s : inputs) {
    lowered.push_back(StringUtils::ToLower(s));
  }

  for (size_t i = 0; i < inputs.size(); ++i) {
    for (size_t j = 0; j < inputs.size(); ++j) {
      EXPECT_EQ(StringUtils::EqualsIgnoreCase(inputs[i], inputs[j]),
                lowered[i] == lowered[j])
          << "EqualsIgnoreCase disagreed for a=" << testing::PrintToString(inputs[i])
          << " b=" << testing::PrintToString(inputs[j]);
      EXPECT_EQ(StringUtils::StartsWithIgnoreCase(inputs[i], inputs[j]),
                lowered[i].starts_with(lowered[j]))
          << "StartsWithIgnoreCase disagreed for str="
          << testing::PrintToString(inputs[i])
          << " prefix=" << testing::PrintToString(inputs[j]);
    }
  }
}

}  // namespace iceberg
