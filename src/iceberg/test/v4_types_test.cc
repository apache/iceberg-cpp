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

#include "iceberg/manifest/v4_types.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/json_serde_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

// Independent wire-schema fixtures from the v4 manifest schema in the spec.
// Check IDs, names, order, types, and requiredness through the schema serializer.
void CheckSchema(const StructType& type, const nlohmann::json& expected) {
  ICEBERG_UNWRAP_OR_FAIL(auto actual, ToJson(type));
  EXPECT_EQ(actual, expected);
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, TypeFromJson(expected));
  EXPECT_EQ(type, *parsed);
}

TEST(V4TypesTest, FileTrackingSchema) {
  const auto expected = nlohmann::json::parse(R"({
    "type": "struct",
    "fields": [
      {"id": 0, "name": "status", "type": "int", "required": true},
      {"id": 1, "name": "snapshot_id", "type": "long", "required": false},
      {"id": 5, "name": "modified_snapshot_id", "type": "long", "required": false},
      {"id": 3, "name": "sequence_number", "type": "long", "required": false},
      {"id": 4, "name": "file_sequence_number", "type": "long", "required": false},
      {"id": 142, "name": "first_row_id", "type": "long", "required": false},
      {"id": 6, "name": "deleted_positions", "type": "binary", "required": false},
      {"id": 7, "name": "replaced_positions", "type": "binary", "required": false}
    ]
  })");
  CheckSchema(*FileTracking::Type(), expected);
}

TEST(V4TypesTest, DeletionVectorMetadataSchema) {
  const auto expected = nlohmann::json::parse(R"({
    "type": "struct",
    "fields": [
      {"id": 155, "name": "location", "type": "string", "required": true},
      {"id": 144, "name": "offset", "type": "long", "required": true},
      {"id": 145, "name": "size_in_bytes", "type": "long", "required": true},
      {"id": 156, "name": "cardinality", "type": "long", "required": true},
      {"id": 149, "name": "key_metadata", "type": "binary", "required": false}
    ]
  })");
  CheckSchema(*DeletionVectorMetadata::Type(), expected);
}

TEST(V4TypesTest, ManifestInfoSchema) {
  const auto expected = nlohmann::json::parse(R"({
    "type": "struct",
    "fields": [
      {"id": 521, "name": "format_version", "type": "int", "required": true},
      {"id": 522, "name": "dv", "type": "binary", "required": false},
      {"id": 504, "name": "added_files_count", "type": "int", "required": true},
      {"id": 505, "name": "existing_files_count", "type": "int", "required": true},
      {"id": 525, "name": "modified_files_count", "type": "int", "required": true},
      {"id": 506, "name": "deleted_files_count", "type": "int", "required": true},
      {"id": 523, "name": "replaced_files_count", "type": "int", "required": true},
      {"id": 512, "name": "added_rows_count", "type": "long", "required": true},
      {"id": 513, "name": "existing_rows_count", "type": "long", "required": true},
      {"id": 526, "name": "modified_rows_count", "type": "long", "required": true},
      {"id": 514, "name": "deleted_rows_count", "type": "long", "required": true},
      {"id": 524, "name": "replaced_rows_count", "type": "long", "required": true},
      {"id": 516, "name": "min_sequence_number", "type": "long", "required": true}
    ]
  })");
  CheckSchema(*ManifestInfo::Type(), expected);
}

struct TrackingStatusCase {
  int32_t value;
  FileTrackingStatus status;
  bool alive;
};

class FileTrackingStatusTest : public ::testing::TestWithParam<TrackingStatusCase> {};

TEST_P(FileTrackingStatusTest, DecodeAndPlanLiveEntries) {
  const auto& test_case = GetParam();
  ICEBERG_UNWRAP_OR_FAIL(auto status, FileTrackingStatusFromInt(test_case.value));
  EXPECT_EQ(status, test_case.status);
  EXPECT_EQ(static_cast<int32_t>(status), test_case.value);
  EXPECT_EQ(FileTracking{.status = status}.IsAlive(), test_case.alive);
}

INSTANTIATE_TEST_SUITE_P(
    AllStatuses, FileTrackingStatusTest,
    ::testing::Values(TrackingStatusCase{0, FileTrackingStatus::kExisting, true},
                      TrackingStatusCase{1, FileTrackingStatus::kAdded, true},
                      TrackingStatusCase{2, FileTrackingStatus::kDeleted, false},
                      TrackingStatusCase{3, FileTrackingStatus::kReplaced, false},
                      TrackingStatusCase{4, FileTrackingStatus::kModified, true}));

TEST(V4TypesTest, InvalidTrackingStatuses) {
  for (int32_t value : {-1, 5, std::numeric_limits<int32_t>::min(),
                        std::numeric_limits<int32_t>::max()}) {
    SCOPED_TRACE(value);
    EXPECT_THAT(FileTrackingStatusFromInt(value), IsError(ErrorKind::kInvalidArgument));
  }
}

TEST(V4TypesTest, LegacyStatusDecoderRejectsV4Statuses) {
  for (int32_t value : {0, 1, 2}) {
    EXPECT_THAT(ManifestStatusFromInt(value), IsOk());
  }
  for (int32_t value : {3, 4}) {
    EXPECT_THAT(ManifestStatusFromInt(value), IsError(ErrorKind::kInvalidArgument));
  }
}

TEST(V4TypesTest, TrackingPreservesMissingInheritanceFieldsAndBitmaps) {
  FileTracking tracking;
  EXPECT_FALSE(tracking.snapshot_id.has_value());
  EXPECT_FALSE(tracking.modified_snapshot_id.has_value());
  EXPECT_FALSE(tracking.sequence_number.has_value());
  EXPECT_FALSE(tracking.file_sequence_number.has_value());
  EXPECT_FALSE(tracking.first_row_id.has_value());
  EXPECT_FALSE(tracking.deleted_positions.has_value());
  EXPECT_FALSE(tracking.replaced_positions.has_value());

  // Missing sequence numbers must remain distinguishable from sequence number 0.
  tracking.sequence_number = 0;
  EXPECT_TRUE(tracking.sequence_number.has_value());
  EXPECT_EQ(tracking.sequence_number.value(), 0);

  tracking.deleted_positions = std::vector<uint8_t>{};
  const auto copy = tracking;
  ASSERT_TRUE(copy.deleted_positions.has_value());
  EXPECT_TRUE(copy.deleted_positions->empty());
  EXPECT_FALSE(copy.replaced_positions.has_value());
}

TEST(V4TypesTest, OptionalBinaryMetadataPreservesAbsence) {
  DeletionVectorMetadata dv;
  ManifestInfo manifest;
  EXPECT_FALSE(dv.key_metadata.has_value());
  EXPECT_FALSE(manifest.dv.has_value());
  dv.key_metadata = std::vector<uint8_t>{0, 255};
  manifest.dv = std::vector<uint8_t>{};
  const auto dv_copy = dv;
  const auto manifest_copy = manifest;
  EXPECT_EQ(dv_copy.key_metadata, dv.key_metadata);
  ASSERT_TRUE(manifest_copy.dv.has_value());
  EXPECT_TRUE(manifest_copy.dv->empty());
}

}  // namespace
}  // namespace iceberg
