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

/// Test Summary
TEST(SummaryTest, DefaultConstruction) {
  Summary default_summary;
  EXPECT_EQ(default_summary.operation(), Summary::Operation::kAppend);
  EXPECT_TRUE(default_summary.properties().empty());
}

TEST(SummaryTest, CustomConstruction) {
  std::unordered_map<std::string, std::string> properties = {{"1", "value1"},
                                                             {"2", "value2"}};
  Summary custom_summary(Summary::Operation::kOverwrite, properties);

  EXPECT_EQ(custom_summary.operation(), Summary::Operation::kOverwrite);
  EXPECT_EQ(custom_summary.properties().size(), 2);
  EXPECT_EQ(custom_summary.properties().at("1"), "value1");
  EXPECT_EQ(custom_summary.properties().at("2"), "value2");
}

TEST(SummaryTest, ToStringRepresentation) {
  std::unordered_map<std::string, std::string> properties = {{"A", "valueA"},
                                                             {"B", "valueB"}};
  Summary summary(Summary::Operation::kReplace, properties);

  std::string to_string_result = summary.ToString();
  EXPECT_NE(to_string_result.find("operation"), std::string::npos);
  EXPECT_NE(to_string_result.find("replace"), std::string::npos);
  EXPECT_NE(to_string_result.find("A"), std::string::npos);
  EXPECT_NE(to_string_result.find("valueA"), std::string::npos);
  EXPECT_NE(to_string_result.find("B"), std::string::npos);
  EXPECT_NE(to_string_result.find("valueB"), std::string::npos);
}

/// Test Snapshot
TEST(SnapshotTest, ConstructionAndFieldAccess) {
  Summary summary(Summary::Operation::kAppend,
                  std::unordered_map<std::string, std::string>{});

  Snapshot snapshot(12345, 54321, 1, 1615569200000, "s3://example/manifest_list.avro",
                    summary, 10);

  EXPECT_EQ(snapshot.snapshot_id(), 12345);
  EXPECT_EQ(snapshot.parent_snapshot_id().value(), 54321);
  EXPECT_EQ(snapshot.sequence_number(), 1);
  EXPECT_EQ(snapshot.timestamp_ms(), 1615569200000);
  EXPECT_EQ(snapshot.manifest_list(), "s3://example/manifest_list.avro");
  EXPECT_EQ(snapshot.summary().operation(), Summary::Operation::kAppend);
  EXPECT_EQ(snapshot.schema_id().value(), 10);
}

TEST(SnapshotTest, ToStringRepresentation) {
  auto summary =
      Summary(Summary::Operation::kDelete,
              std::unordered_map<std::string, std::string>{
                  {std::string(SnapshotSummaryFields::kDeletedDataFiles), "100"}});

  Snapshot snapshot(67890, {}, 42, 1625569200000,
                    "s3://example/another_manifest_list.avro", summary, {});

  std::string to_string_result = snapshot.ToString();

  EXPECT_NE(to_string_result.find("67890"), std::string::npos);
  EXPECT_NE(to_string_result.find("sequence_number: 42"), std::string::npos);
  EXPECT_NE(
      to_string_result.find("manifest_list: s3://example/another_manifest_list.avro"),
      std::string::npos);
  EXPECT_NE(to_string_result.find(SnapshotSummaryFields::kDeletedDataFiles),
            std::string::npos);
}

TEST(SnapshotTest, EqualityComparison) {
  Summary summary1(Summary::Operation::kAppend,
                   std::unordered_map<std::string, std::string>{
                       {std::string(SnapshotSummaryFields::kAddedDataFiles), "101"}});
  Summary summary2(Summary::Operation::kAppend,
                   std::unordered_map<std::string, std::string>{
                       {std::string(SnapshotSummaryFields::kAddedDataFiles), "101"}});
  Summary summary3(Summary::Operation::kDelete,
                   std::unordered_map<std::string, std::string>{
                       {std::string(SnapshotSummaryFields::kDeletedDataFiles), "20"}});

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
