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

#include "iceberg/snapshot.h"

#include <gtest/gtest.h>

namespace iceberg {

class SnapshotTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize some common test data
    summary1 = {{SnapshotSummaryFields::kOperation, DataOperation::kAppend},
                {SnapshotSummaryFields::kAddedDataFiles, "101"}};

    summary2 = {{SnapshotSummaryFields::kOperation, DataOperation::kAppend},
                {SnapshotSummaryFields::kAddedDataFiles, "101"}};

    summary3 = {{SnapshotSummaryFields::kOperation, DataOperation::kDelete},
                {SnapshotSummaryFields::kDeletedDataFiles, "20"}};
  }

  std::unordered_map<std::string, std::string> summary1;
  std::unordered_map<std::string, std::string> summary2;
  std::unordered_map<std::string, std::string> summary3;
};

TEST_F(SnapshotTest, ConstructionAndFieldAccess) {
  // Test the constructor and field access
  Snapshot snapshot{.snapshot_id = 12345,
                    .parent_snapshot_id = 54321,
                    .sequence_number = 1,
                    .timestamp_ms = 1615569200000,
                    .manifest_list = "s3://example/manifest_list.avro",
                    .summary = summary1,
                    .schema_id = 10};

  EXPECT_EQ(snapshot.snapshot_id, 12345);
  EXPECT_TRUE(snapshot.parent_snapshot_id.has_value());
  EXPECT_EQ(*snapshot.parent_snapshot_id, 54321);
  EXPECT_EQ(snapshot.sequence_number, 1);
  EXPECT_EQ(snapshot.timestamp_ms, 1615569200000);
  EXPECT_EQ(snapshot.manifest_list, "s3://example/manifest_list.avro");
  EXPECT_EQ(snapshot.operation().value(), DataOperation::kAppend);
  EXPECT_EQ(snapshot.summary.at(std::string(SnapshotSummaryFields::kAddedDataFiles)),
            "101");
  EXPECT_EQ(snapshot.summary.at(std::string(SnapshotSummaryFields::kOperation)),
            DataOperation::kAppend);
  EXPECT_TRUE(snapshot.schema_id.has_value());
  EXPECT_EQ(snapshot.schema_id.value(), 10);
}

TEST_F(SnapshotTest, EqualityComparison) {
  // Test the == and != operators
  Snapshot snapshot1(12345, {}, 1, 1615569200000, "s3://example/manifest_list.avro",
                     summary1, {});

  Snapshot snapshot2(12345, {}, 1, 1615569200000, "s3://example/manifest_list.avro",
                     summary2, {});

  Snapshot snapshot3(67890, {}, 1, 1615569200000, "s3://example/manifest_list.avro",
                     summary3, {});

  EXPECT_EQ(snapshot1, snapshot2);
  EXPECT_NE(snapshot1, snapshot3);
}

}  // namespace iceberg
