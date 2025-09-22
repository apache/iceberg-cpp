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

#include "iceberg/util/uuid_util.h"

#include <vector>

#include <gtest/gtest.h>

#include "matchers.h"

namespace iceberg {

TEST(UUIDUtilTest, GenerateV4) {
  auto uuid = UUIDUtils::GenerateUuidV4();
  // just ensure it runs and produces a value
  EXPECT_EQ(uuid.size(), 16);
  // Version 4 UUIDs have the version number (4) in the 7th byte
  EXPECT_EQ((uuid[6] >> 4) & 0x0F, 4);
  // Variant is in the 9th byte, the two most significant bits should be 10
  EXPECT_EQ((uuid[8] >> 6) & 0x03, 0b10);
}

TEST(UUIDUtilTest, GenerateV7) {
  auto uuid = UUIDUtils::GenerateUuidV7();
  // just ensure it runs and produces a value
  EXPECT_EQ(uuid.size(), 16);
  // Version 7 UUIDs have the version number (7) in the 7th byte
  EXPECT_EQ((uuid[6] >> 4) & 0x0F, 7);
  // Variant is in the 9th byte, the two most significant bits should be 10
  EXPECT_EQ((uuid[8] >> 6) & 0x03, 0b10);
}

TEST(UUIDUtilTest, FromString) {
  std::vector<std::string> uuid_strings = {
      "123e4567-e89b-12d3-a456-426614174000",
      "550e8400-e29b-41d4-a716-446655440000",
      "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  };

  for (const auto& uuid_str : uuid_strings) {
    auto result = UUIDUtils::FromString(uuid_str);
    EXPECT_THAT(result, IsOk());
    auto uuid = result.value();
    EXPECT_EQ(UUIDUtils::ToString(uuid), uuid_str);
  }

  std::vector<std::pair<std::string, std::string>> uuid_string_pairs = {
      {"123e4567e89b12d3a456426614174000", "123e4567-e89b-12d3-a456-426614174000"},
      {"550E8400E29B41D4A716446655440000", "550e8400-e29b-41d4-a716-446655440000"},
      {"F47AC10B58CC4372A5670E02B2C3D479", "f47ac10b-58cc-4372-a567-0e02b2c3d479"},
  };

  for (const auto& [input_str, expected_str] : uuid_string_pairs) {
    auto result = UUIDUtils::FromString(input_str);
    EXPECT_THAT(result, IsOk());
    auto uuid = result.value();
    EXPECT_EQ(UUIDUtils::ToString(uuid), expected_str);
  }
}

TEST(UUIDUtilTest, FromStringInvalid) {
  std::vector<std::string> invalid_uuid_strings = {
      "123e4567-e89b-12d3-a456-42661417400",    // too short
      "123e4567-e89b-12d3-a456-4266141740000",  // too long
      "g23e4567-e89b-12d3-a456-426614174000",   // invalid character
      "123e4567e89b12d3a45642661417400",        // too short without dashes
      "123e4567e89b12d3a4564266141740000",      // too long without dashes
      "550e8400-e29b-41d4-a716-44665544000Z",   // invalid character at end
      "550e8400-e29b-41d4-a716-44665544000-",   // invalid character at end
      "550e8400-e29b-41d4-a716-4466554400",     // too short
  };

  for (const auto& uuid_str : invalid_uuid_strings) {
    auto result = UUIDUtils::FromString(uuid_str);
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("Invalid UUID string"));
  }
}

}  // namespace iceberg
