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

#include "iceberg/util/truncate_util.h"

#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// The following tests are from
// https://iceberg.apache.org/spec/#truncate-transform-details
TEST(TruncateUtilTest, TruncateLiteral) {
  // Integer
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Int(1), 10), Literal::Int(0));
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Int(-1), 10), Literal::Int(-10));
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Long(1), 10), Literal::Long(0));
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Long(-1), 10), Literal::Long(-10));

  // Decimal
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::Decimal(1065, 4, 2), 50),
            Literal::Decimal(1050, 4, 2));

  // String
  EXPECT_EQ(TruncateUtils::TruncateLiteral(Literal::String("iceberg"), 3),
            Literal::String("ice"));

  // Binary
  std::string data = "\x01\x02\x03\x04\x05";
  std::string expected = "\x01\x02\x03";
  EXPECT_EQ(TruncateUtils::TruncateLiteral(
                Literal::Binary(std::vector<uint8_t>(data.begin(), data.end())), 3),
            Literal::Binary(std::vector<uint8_t>(expected.begin(), expected.end())));
}

TEST(TruncateUtilTest, TruncateBinaryMax) {
  std::vector<uint8_t> test1{1, 1, 2};
  std::vector<uint8_t> test2{1, 1, 0xFF, 2};
  std::vector<uint8_t> test3{0xFF, 0xFF, 0xFF, 2};
  std::vector<uint8_t> test4{1, 1, 0};
  std::vector<uint8_t> expected_output{1, 2};

  // Test1: truncate {1, 1, 2} to 2 bytes -> {1, 2}
  ICEBERG_UNWRAP_OR_FAIL(auto result1,
                         TruncateUtils::TruncateLiteralMax(Literal::Binary(test1), 2));
  EXPECT_EQ(result1, Literal::Binary(expected_output));

  // Test2: truncate {1, 1, 0xFF, 2} to 2 bytes -> {1, 2}
  ICEBERG_UNWRAP_OR_FAIL(auto result2,
                         TruncateUtils::TruncateLiteralMax(Literal::Binary(test2), 2));
  EXPECT_EQ(result2, Literal::Binary(expected_output));

  // Test2b: truncate {1, 1, 0xFF, 2} to 3 bytes -> {1, 2}
  ICEBERG_UNWRAP_OR_FAIL(auto result2b,
                         TruncateUtils::TruncateLiteralMax(Literal::Binary(test2), 3));
  EXPECT_EQ(result2b, Literal::Binary(expected_output));

  // Test3: no truncation needed when length >= input size
  ICEBERG_UNWRAP_OR_FAIL(auto result3,
                         TruncateUtils::TruncateLiteralMax(Literal::Binary(test3), 5));
  EXPECT_EQ(result3, Literal::Binary(test3));

  // Test3b: cannot truncate when first bytes are all 0xFF
  EXPECT_THAT(TruncateUtils::TruncateLiteralMax(Literal::Binary(test3), 2),
              IsError(ErrorKind::kInvalidArgument));

  // Test4: truncate {1, 1, 0} to 2 bytes -> {1, 2}
  ICEBERG_UNWRAP_OR_FAIL(auto result4,
                         TruncateUtils::TruncateLiteralMax(Literal::Binary(test4), 2));
  EXPECT_EQ(result4, Literal::Binary(expected_output));
}

TEST(TruncateUtilTest, TruncateStringMax) {
  // Test1: Japanese characters "イロハニホヘト"
  std::string test1 =
      "\xE3\x82\xA4\xE3\x83\xAD\xE3\x83\x8F\xE3\x83\x8B\xE3\x83\x9B\xE3\x83\x98\xE3\x83"
      "\x88";
  std::string test1_2_expected = "\xE3\x82\xA4\xE3\x83\xAE";              // "イヮ"
  std::string test1_3_expected = "\xE3\x82\xA4\xE3\x83\xAD\xE3\x83\x90";  // "イロバ"

  ICEBERG_UNWRAP_OR_FAIL(auto result1_2,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test1), 2));
  EXPECT_EQ(result1_2, Literal::String(test1_2_expected));

  ICEBERG_UNWRAP_OR_FAIL(auto result1_3,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test1), 3));
  EXPECT_EQ(result1_3, Literal::String(test1_3_expected));

  // No truncation needed when length >= input size
  ICEBERG_UNWRAP_OR_FAIL(auto result1_7,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test1), 7));
  EXPECT_EQ(result1_7, Literal::String(test1));

  ICEBERG_UNWRAP_OR_FAIL(auto result1_8,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test1), 8));
  EXPECT_EQ(result1_8, Literal::String(test1));

  // Test2: Mixed characters "щщаεはчωいにπάほхεろへσκζ"
  std::string test2 =
      "\xD1\x89\xD1\x89\xD0\xB0\xCE\xB5\xE3\x81\xAF\xD1\x87\xCF\x89\xE3\x81\x84\xE3\x81"
      "\xAB\xCF\x80\xCE\xAC\xE3\x81\xBB\xD1\x85\xCE\xB5\xE3\x82\x8D\xE3\x81\xB8\xCF\x83"
      "\xCE\xBA\xCE\xB6";
  std::string test2_7_expected =
      "\xD1\x89\xD1\x89\xD0\xB0\xCE\xB5\xE3\x81\xAF\xD1\x87\xCF\x8A";  // "щщаεはчϊ"

  ICEBERG_UNWRAP_OR_FAIL(auto result2_7,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test2), 7));
  EXPECT_EQ(result2_7, Literal::String(test2_7_expected));

  // Test3: String with max 3-byte UTF-8 character "aनि\uFFFF\uFFFF"
  std::string test3 = "a\xE0\xA4\xA8\xE0\xA4\xBF\xEF\xBF\xBF\xEF\xBF\xBF";
  std::string test3_3_expected = "a\xE0\xA4\xA8\xE0\xA5\x80";  // "aनी"

  ICEBERG_UNWRAP_OR_FAIL(auto result3_3,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test3), 3));
  EXPECT_EQ(result3_3, Literal::String(test3_3_expected));

  // Test4: Max 3-byte UTF-8 character "\uFFFF\uFFFF"
  std::string test4 = "\xEF\xBF\xBF\xEF\xBF\xBF";
  std::string test4_1_expected = "\xF0\x90\x80\x80";  // U+10000 (first 4-byte UTF-8 char)

  ICEBERG_UNWRAP_OR_FAIL(auto result4_1,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test4), 1));
  EXPECT_EQ(result4_1, Literal::String(test4_1_expected));

  // Test5: Max 4-byte UTF-8 characters "\uDBFF\uDFFF\uDBFF\uDFFF"
  std::string test5 = "\xF4\x8F\xBF\xBF\xF4\x8F\xBF\xBF";  // U+10FFFF U+10FFFF
  EXPECT_THAT(TruncateUtils::TruncateLiteralMax(Literal::String(test5), 1),
              IsError(ErrorKind::kInvalidArgument));

  // Test6: 4-byte UTF-8 character "\uD800\uDFFF\uD800\uDFFF"
  std::string test6 = "\xF0\x90\x8F\xBF\xF0\x90\x8F\xBF";  // U+103FF U+103FF
  std::string test6_1_expected = "\xF0\x90\x90\x80";       // U+10400

  ICEBERG_UNWRAP_OR_FAIL(auto result6_1,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test6), 1));
  EXPECT_EQ(result6_1, Literal::String(test6_1_expected));

  // Test7: Emoji "\uD83D\uDE02\uD83D\uDE02\uD83D\uDE02"
  std::string test7 = "\xF0\x9F\x98\x82\xF0\x9F\x98\x82\xF0\x9F\x98\x82";  // 😂😂😂
  std::string test7_2_expected = "\xF0\x9F\x98\x82\xF0\x9F\x98\x83";       // 😂😃
  std::string test7_1_expected = "\xF0\x9F\x98\x83";                       // 😃

  ICEBERG_UNWRAP_OR_FAIL(auto result7_2,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test7), 2));
  EXPECT_EQ(result7_2, Literal::String(test7_2_expected));

  ICEBERG_UNWRAP_OR_FAIL(auto result7_1,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test7), 1));
  EXPECT_EQ(result7_1, Literal::String(test7_1_expected));

  // Test8: Overflow case "a\uDBFF\uDFFFc"
  std::string test8 =
      "a\xF4\x8F\xBF\xBF"
      "c";  // a U+10FFFF c
  std::string test8_2_expected = "b";

  ICEBERG_UNWRAP_OR_FAIL(auto result8_2,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test8), 2));
  EXPECT_EQ(result8_2, Literal::String(test8_2_expected));

  // Test9: Skip surrogate range "a" + (char)(Character.MIN_SURROGATE - 1) + "b"
  std::string test9 =
      "a\xED\x9F\xBF"
      "b";                                         // a U+D7FF b
  std::string test9_2_expected = "a\xEE\x80\x80";  // a U+E000

  ICEBERG_UNWRAP_OR_FAIL(auto result9_2,
                         TruncateUtils::TruncateLiteralMax(Literal::String(test9), 2));
  EXPECT_EQ(result9_2, Literal::String(test9_2_expected));
}

}  // namespace iceberg
