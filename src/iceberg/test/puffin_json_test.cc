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

#include <string>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/json_serde_internal.h"
#include "iceberg/test/matchers.h"

namespace iceberg::puffin {

// ==================== BlobMetadata Parameterized Tests ====================

struct BlobMetadataJsonParam {
  std::string name;
  BlobMetadata blob;
  std::string expected_json;
};

class BlobMetadataJsonTest : public ::testing::TestWithParam<BlobMetadataJsonParam> {};

TEST_P(BlobMetadataJsonTest, RoundTrip) {
  const auto& param = GetParam();
  auto expected = nlohmann::json::parse(param.expected_json);

  EXPECT_EQ(ToJson(param.blob), expected);

  ICEBERG_UNWRAP_OR_FAIL(auto result, BlobMetadataFromJson(expected));
  EXPECT_EQ(result, param.blob);
}

INSTANTIATE_TEST_SUITE_P(PuffinJson, BlobMetadataJsonTest,
                         ::testing::Values(
                             BlobMetadataJsonParam{
                                 .name = "AllFields",
                                 .blob = {.type = "apache-datasketches-theta-v1",
                                          .input_fields = {1, 2},
                                          .snapshot_id = 12345,
                                          .sequence_number = 67,
                                          .offset = 100,
                                          .length = 200,
                                          .compression_codec = "zstd",
                                          .properties = {{"key", "value"}}},
                                 .expected_json = R"({
              "type": "apache-datasketches-theta-v1",
              "fields": [1, 2],
              "snapshot-id": 12345,
              "sequence-number": 67,
              "offset": 100,
              "length": 200,
              "compression-codec": "zstd",
              "properties": {"key": "value"}
            })"},
                             BlobMetadataJsonParam{.name = "MinimalFields",
                                                   .blob = {.type = "test-type",
                                                            .input_fields = {1},
                                                            .snapshot_id = 100,
                                                            .sequence_number = 1,
                                                            .offset = 0,
                                                            .length = 50},
                                                   .expected_json = R"({
              "type": "test-type",
              "fields": [1],
              "snapshot-id": 100,
              "sequence-number": 1,
              "offset": 0,
              "length": 50
            })"}),
                         [](const ::testing::TestParamInfo<BlobMetadataJsonParam>& info) {
                           return info.param.name;
                         });

// ==================== BlobMetadata Invalid JSON Tests ====================

struct InvalidBlobMetadataJsonParam {
  std::string name;
  std::string json;
};

class InvalidBlobMetadataJsonTest
    : public ::testing::TestWithParam<InvalidBlobMetadataJsonParam> {};

TEST_P(InvalidBlobMetadataJsonTest, DeserializeFails) {
  auto json = nlohmann::json::parse(GetParam().json);
  EXPECT_THAT(BlobMetadataFromJson(json), IsError(ErrorKind::kJsonParseError));
}

INSTANTIATE_TEST_SUITE_P(
    PuffinJson, InvalidBlobMetadataJsonTest,
    ::testing::Values(
        InvalidBlobMetadataJsonParam{.name = "MissingType",
                                     .json = R"({"fields":[1],"snapshot-id":1,
                                       "sequence-number":1,"offset":0,"length":10})"},
        InvalidBlobMetadataJsonParam{.name = "MissingFields",
                                     .json = R"({"type":"t","snapshot-id":1,
                                       "sequence-number":1,"offset":0,"length":10})"},
        InvalidBlobMetadataJsonParam{.name = "MissingSnapshotId",
                                     .json = R"({"type":"t","fields":[1],
                                       "sequence-number":1,"offset":0,"length":10})"},
        InvalidBlobMetadataJsonParam{.name = "MissingSequenceNumber",
                                     .json = R"({"type":"t","fields":[1],
                                       "snapshot-id":1,"offset":0,"length":10})"},
        InvalidBlobMetadataJsonParam{.name = "MissingOffset",
                                     .json = R"({"type":"t","fields":[1],
                                       "snapshot-id":1,"sequence-number":1,"length":10})"},
        InvalidBlobMetadataJsonParam{.name = "MissingLength",
                                     .json = R"({"type":"t","fields":[1],
                                       "snapshot-id":1,"sequence-number":1,"offset":0})"}),
    [](const ::testing::TestParamInfo<InvalidBlobMetadataJsonParam>& info) {
      return info.param.name;
    });

// ==================== FileMetadata Tests ====================

TEST(PuffinJsonTest, FileMetadataRoundTrip) {
  BlobMetadata blob1{.type = "type-a",
                     .input_fields = {1},
                     .snapshot_id = 100,
                     .sequence_number = 1,
                     .offset = 4,
                     .length = 50,
                     .compression_codec = "lz4"};

  BlobMetadata blob2{.type = "type-b",
                     .input_fields = {2, 3},
                     .snapshot_id = 200,
                     .sequence_number = 2,
                     .offset = 54,
                     .length = 100};

  FileMetadata metadata{.blobs = {blob1, blob2},
                        .properties = {{"created-by", "iceberg-cpp-test"}}};

  nlohmann::json expected_json = R"({
    "blobs": [
      {
        "type": "type-a",
        "fields": [1],
        "snapshot-id": 100,
        "sequence-number": 1,
        "offset": 4,
        "length": 50,
        "compression-codec": "lz4"
      },
      {
        "type": "type-b",
        "fields": [2, 3],
        "snapshot-id": 200,
        "sequence-number": 2,
        "offset": 54,
        "length": 100
      }
    ],
    "properties": {"created-by": "iceberg-cpp-test"}
  })"_json;

  EXPECT_EQ(ToJson(metadata), expected_json);

  ICEBERG_UNWRAP_OR_FAIL(auto result, FileMetadataFromJson(expected_json));
  EXPECT_EQ(result, metadata);
}

TEST(PuffinJsonTest, FileMetadataStringRoundTrip) {
  FileMetadata metadata{.blobs = {{.type = "test",
                                   .input_fields = {1},
                                   .snapshot_id = 1,
                                   .sequence_number = 1,
                                   .offset = 0,
                                   .length = 10}}};

  auto json_str = ToJsonString(metadata);
  ICEBERG_UNWRAP_OR_FAIL(auto result, FileMetadataFromJsonString(json_str));
  EXPECT_EQ(result, metadata);
}

TEST(PuffinJsonTest, FileMetadataFromInvalidString) {
  EXPECT_THAT(FileMetadataFromJsonString(""), IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(FileMetadataFromJsonString("{invalid}"),
              IsError(ErrorKind::kJsonParseError));
}

}  // namespace iceberg::puffin
