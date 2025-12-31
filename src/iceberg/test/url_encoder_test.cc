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

#include "iceberg/util/url_encoder.h"

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(UrlEncoderTest, Encode) {
  // RFC 3986 unreserved characters should not be encoded
  EXPECT_THAT(UrlEncoder::Encode("abc123XYZ"), ::testing::Eq("abc123XYZ"));
  EXPECT_THAT(UrlEncoder::Encode("test-file_name.txt~backup"),
              ::testing::Eq("test-file_name.txt~backup"));

  // Spaces and special characters should be encoded
  EXPECT_THAT(UrlEncoder::Encode("hello world"), ::testing::Eq("hello%20world"));
  EXPECT_THAT(UrlEncoder::Encode("test@example.com"),
              ::testing::Eq("test%40example.com"));
  EXPECT_THAT(UrlEncoder::Encode("path/to/file"), ::testing::Eq("path%2Fto%2Ffile"));
  EXPECT_THAT(UrlEncoder::Encode("key=value&foo=bar"),
              ::testing::Eq("key%3Dvalue%26foo%3Dbar"));
  EXPECT_THAT(UrlEncoder::Encode("100%"), ::testing::Eq("100%25"));
  EXPECT_THAT(UrlEncoder::Encode("hello\x1fworld"), ::testing::Eq("hello%1Fworld"));
  EXPECT_THAT(UrlEncoder::Encode(""), ::testing::Eq(""));
}

TEST(UrlEncoderTest, Decode) {
  // Decode percent-encoded strings
  EXPECT_THAT(UrlEncoder::Decode("hello%20world"), ::testing::Eq("hello world"));
  EXPECT_THAT(UrlEncoder::Decode("test%40example.com"),
              ::testing::Eq("test@example.com"));
  EXPECT_THAT(UrlEncoder::Decode("path%2fto%2Ffile"), ::testing::Eq("path/to/file"));
  EXPECT_THAT(UrlEncoder::Decode("key%3dvalue%26foo%3Dbar"),
              ::testing::Eq("key=value&foo=bar"));
  EXPECT_THAT(UrlEncoder::Decode("100%25"), ::testing::Eq("100%"));

  // ASCII Unit Separator (0x1F)
  EXPECT_THAT(UrlEncoder::Decode("hello%1Fworld"), ::testing::Eq("hello\x1Fworld"));

  // Unreserved characters remain unchanged
  EXPECT_THAT(UrlEncoder::Decode("test-file_name.txt~backup"),
              ::testing::Eq("test-file_name.txt~backup"));
  EXPECT_THAT(UrlEncoder::Decode(""), ::testing::Eq(""));
}

TEST(UrlEncoderTest, EncodeDecodeRoundTrip) {
  std::vector<std::string> test_cases = {"hello world",
                                         "test@example.com",
                                         "path/to/file",
                                         "key=value&foo=bar",
                                         "100%",
                                         "hello\x1Fworld",
                                         "special!@#$%^&*()chars",
                                         "mixed-123_test.file~ok",
                                         ""};

  for (const auto& test : test_cases) {
    std::string encoded = UrlEncoder::Encode(test);
    std::string decoded = UrlEncoder::Decode(encoded);
    EXPECT_EQ(decoded, test) << "Round-trip failed for: " << test;
  }
}

}  // namespace iceberg
