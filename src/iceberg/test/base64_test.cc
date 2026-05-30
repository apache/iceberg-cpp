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

#include "iceberg/util/base64.h"

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(Base64Test, Encode) {
  // Empty string
  EXPECT_EQ("", Base64::Encode(""));

  // Single character
  EXPECT_EQ("YQ==", Base64::Encode("a"));
  EXPECT_EQ("YWI=", Base64::Encode("ab"));
  EXPECT_EQ("YWJj", Base64::Encode("abc"));

  // Multiple of 3 characters
  EXPECT_EQ("YWJjZGU=", Base64::Encode("abcde"));
  EXPECT_EQ("YWJjZGVm", Base64::Encode("abcdef"));

  // Common strings
  EXPECT_EQ("U29tZSBkYXRhIHdpdGggY2hhcmFjdGVycw==",
            Base64::Encode("Some data with characters"));
  EXPECT_EQ("aGVsbG8=", Base64::Encode("hello"));
  EXPECT_EQ("dGVzdCBzdHJpbmc=", Base64::Encode("test string"));

  // Unicode
  EXPECT_EQ("8J+EgA==", Base64::Encode("\xF0\x9F\x84\x80"));
  // Null byte
  EXPECT_EQ("AA==", Base64::Encode({"\x00", 1}));
}

TEST(Base64Test, Decode) {
  // Empty string
  ICEBERG_UNWRAP_OR_FAIL(auto empty, Base64::Decode(""));
  EXPECT_EQ("", empty);

  // Round-trip with Base64::Encode
  ICEBERG_UNWRAP_OR_FAIL(auto a, Base64::Decode("YQ=="));
  EXPECT_EQ("a", a);
  ICEBERG_UNWRAP_OR_FAIL(auto ab, Base64::Decode("YWI="));
  EXPECT_EQ("ab", ab);
  ICEBERG_UNWRAP_OR_FAIL(auto abc, Base64::Decode("YWJj"));
  EXPECT_EQ("abc", abc);
  ICEBERG_UNWRAP_OR_FAIL(auto abcde, Base64::Decode("YWJjZGU="));
  EXPECT_EQ("abcde", abcde);
  ICEBERG_UNWRAP_OR_FAIL(auto abcdef, Base64::Decode("YWJjZGVm"));
  EXPECT_EQ("abcdef", abcdef);
  ICEBERG_UNWRAP_OR_FAIL(auto hello, Base64::Decode("aGVsbG8="));
  EXPECT_EQ("hello", hello);
  ICEBERG_UNWRAP_OR_FAIL(auto test_str, Base64::Decode("dGVzdCBzdHJpbmc="));
  EXPECT_EQ("test string", test_str);

  // Without padding (should still work)
  ICEBERG_UNWRAP_OR_FAIL(auto a2, Base64::Decode("YQ"));
  EXPECT_EQ("a", a2);
  ICEBERG_UNWRAP_OR_FAIL(auto ab2, Base64::Decode("YWI"));
  EXPECT_EQ("ab", ab2);

  // Invalid characters return error
  EXPECT_THAT(Base64::Decode("!!!"), IsError(ErrorKind::kInvalidArgument));
}

TEST(Base64Test, UrlEncode) {
  // Empty string
  EXPECT_EQ("", Base64::UrlEncode(""));

  // No padding is emitted (unlike standard base64)
  EXPECT_EQ("YQ", Base64::UrlEncode("a"));
  EXPECT_EQ("YWI", Base64::UrlEncode("ab"));
  EXPECT_EQ("YWJj", Base64::UrlEncode("abc"));
  EXPECT_EQ("aGVsbG8", Base64::UrlEncode("hello"));

  // URL-safe characters: '-' and '_' instead of '+' and '/'
  // bytes {0xFB, 0xFF, 0xFE} encode to "+//+" in standard base64, "-__-" in base64url
  EXPECT_EQ("-__-", Base64::UrlEncode("\xFB\xFF\xFE"));

  // Round-trip with UrlDecode
  ICEBERG_UNWRAP_OR_FAIL(auto decoded, Base64::UrlDecode(Base64::UrlEncode("hello")));
  EXPECT_EQ("hello", decoded);
}

TEST(Base64Test, UrlDecode) {
  // Empty string
  ICEBERG_UNWRAP_OR_FAIL(auto empty, Base64::UrlDecode(""));
  EXPECT_EQ("", empty);

  // Standard cases (same as Base64::Decode for alphanumeric)
  ICEBERG_UNWRAP_OR_FAIL(auto hello, Base64::UrlDecode("aGVsbG8"));
  EXPECT_EQ("hello", hello);
  ICEBERG_UNWRAP_OR_FAIL(auto abc, Base64::UrlDecode("YWJj"));
  EXPECT_EQ("abc", abc);

  // URL-safe characters: '-' and '_' instead of '+' and '/'
  // bytes {0xFB, 0xFF, 0xFE} encode to "+//+" in standard base64, "-__-" in base64url
  ICEBERG_UNWRAP_OR_FAIL(auto decoded, Base64::UrlDecode("-__-"));
  EXPECT_EQ(3u, decoded.size());
  EXPECT_EQ('\xFB', decoded[0]);
  EXPECT_EQ('\xFF', decoded[1]);
  EXPECT_EQ('\xFE', decoded[2]);

  // Standard base64 chars '+' and '/' should be invalid in base64url
  EXPECT_THAT(Base64::UrlDecode("+//+"), IsError(ErrorKind::kInvalidArgument));

  // With padding (should handle gracefully)
  ICEBERG_UNWRAP_OR_FAIL(auto hello2, Base64::UrlDecode("aGVsbG8="));
  EXPECT_EQ("hello", hello2);

  // Invalid characters return error
  EXPECT_THAT(Base64::UrlDecode("!!!invalid!!!"), IsError(ErrorKind::kInvalidArgument));
}

}  // namespace iceberg
