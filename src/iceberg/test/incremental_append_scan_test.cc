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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/test/scan_test_base.h"

namespace iceberg {

class IncrementalAppendScanTest : public ScanTestBase {};

TEST_P(IncrementalAppendScanTest, FromSnapshotInclusive) {
  auto version = GetParam();

  // Create 3 snapshots, each appending one file
  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});
  auto snapshot_c =
      MakeAppendSnapshot(version, 3000L, 2000L, 3L, {"/path/to/file_c.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_c}, 3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})}});

  // Test: from_snapshot_inclusive(snapshot_a) should return 3 files (A, B, C)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot(1000L, /*inclusive=*/true);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 3);
    EXPECT_THAT(GetPaths(tasks), testing::UnorderedElementsAre(
                                     "/path/to/file_a.parquet", "/path/to/file_b.parquet",
                                     "/path/to/file_c.parquet"));
  }

  // Test: from_snapshot_inclusive(snapshot_a).to_snapshot(snapshot_c) should return 3
  // files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot(1000L, /*inclusive=*/true).ToSnapshot(3000L);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 3);
  }
}

TEST_P(IncrementalAppendScanTest, FromSnapshotInclusiveWithNonExistingRef) {
  auto metadata = MakeTableMetadata({}, -1L);
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalAppendScanBuilder::Make(metadata, file_io_));
  builder->FromSnapshot("non_existing_ref", /*inclusive=*/true);
  EXPECT_THAT(builder->Build(),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Cannot find ref: non_existing_ref")));
}

TEST_P(IncrementalAppendScanTest, FromSnapshotInclusiveWithTag) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b = MakeAppendSnapshot(
      version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet", "/path/to/file_c.parquet"});
  auto snapshot_current = MakeAppendSnapshot(
      version, 3000L, 2000L, 3L, {"/path/to/file_d.parquet", "/path/to/file_a2.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_current}, 3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})},
       {"t1", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 1000L, .retention = SnapshotRef::Tag{}})},
       {"t2", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 2000L, .retention = SnapshotRef::Tag{}})}});

  // Test: from_snapshot_inclusive(t1) should return 5 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot("t1", /*inclusive=*/true);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 5);
  }

  // Test: from_snapshot_inclusive(t1).to_snapshot(t2) should return 3 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot("t1", /*inclusive=*/true).ToSnapshot("t2");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 3);
  }
}

TEST_P(IncrementalAppendScanTest, FromSnapshotInclusiveWithBranchShouldFail) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a}, 1000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 1000L, .retention = SnapshotRef::Branch{}})},
       {"b1", std::make_shared<SnapshotRef>(SnapshotRef{
                  .snapshot_id = 1000L, .retention = SnapshotRef::Branch{}})}});

  // Test: from_snapshot_inclusive(branch_name) should fail
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot("b1", /*inclusive=*/true);
    EXPECT_THAT(builder->Build(),
                ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                                 HasErrorMessage("Ref b1 is not a tag")));
  }

  // Test: to_snapshot(branch_name) should fail
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot(1000L, /*inclusive=*/true).ToSnapshot("b1");
    EXPECT_THAT(builder->Build(),
                ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                                 HasErrorMessage("Ref b1 is not a tag")));
  }
}

TEST_P(IncrementalAppendScanTest, UseBranch) {
  auto version = GetParam();

  // Common ancestor
  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  // Main branch snapshots
  auto snapshot_main_b = MakeAppendSnapshot(
      version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet", "/path/to/file_c.parquet"});
  auto snapshot_current = MakeAppendSnapshot(
      version, 3000L, 2000L, 3L, {"/path/to/file_d.parquet", "/path/to/file_a2.parquet"});
  // Branch b1 snapshots
  auto snapshot_branch_b =
      MakeAppendSnapshot(version, 4000L, 1000L, 2L, {"/path/to/file_c_branch.parquet"});
  auto snapshot_branch_c =
      MakeAppendSnapshot(version, 5000L, 4000L, 3L, {"/path/to/file_c_branch2.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_main_b, snapshot_current, snapshot_branch_b,
       snapshot_branch_c},
      3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})},
       {"t1", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 1000L, .retention = SnapshotRef::Tag{}})},
       {"t2", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 2000L, .retention = SnapshotRef::Tag{}})},
       {"b1", std::make_shared<SnapshotRef>(SnapshotRef{
                  .snapshot_id = 5000L, .retention = SnapshotRef::Branch{}})}});

  // Test: from_snapshot_inclusive(t1) on main should return 5 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot("t1", /*inclusive=*/true);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 5);
  }

  // Test: from_snapshot_inclusive(t1).use_branch(b1) should return 3 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot("t1", /*inclusive=*/true).UseBranch("b1");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 3);
  }

  // Test: to_snapshot(snapshot_branch_b).use_branch(b1) should return 2 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->ToSnapshot(4000L).UseBranch("b1");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 2);
  }

  // Test: to_snapshot(snapshot_branch_c).use_branch(b1) should return 3 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->ToSnapshot(5000L).UseBranch("b1");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 3);
  }

  // Test: from_snapshot_exclusive(t1).to_snapshot(snapshot_branch_b).use_branch(b1)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot("t1", /*inclusive=*/false).ToSnapshot(4000L).UseBranch("b1");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 1);
  }
}

TEST_P(IncrementalAppendScanTest, UseBranchWithTagShouldFail) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a}, 1000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 1000L, .retention = SnapshotRef::Branch{}})},
       {"t1", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 1000L, .retention = SnapshotRef::Tag{}})}});

  // Test: use_branch(tag_name) should fail
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalAppendScanBuilder::Make(metadata, file_io_));
  builder->FromSnapshot(1000L, /*inclusive=*/true).UseBranch("t1");
  EXPECT_THAT(builder->Build(),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Ref t1 is not a branch")));
}

TEST_P(IncrementalAppendScanTest, UseBranchWithInvalidSnapshotShouldFail) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_main_b = MakeAppendSnapshot(
      version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet", "/path/to/file_c.parquet"});
  auto snapshot_branch_b =
      MakeAppendSnapshot(version, 3000L, 1000L, 2L, {"/path/to/file_d.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_main_b, snapshot_branch_b}, 2000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 2000L, .retention = SnapshotRef::Branch{}})},
       {"b1", std::make_shared<SnapshotRef>(SnapshotRef{
                  .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})}});

  // Test: to_snapshot(snapshot_main_b).use_branch(b1) should fail
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->ToSnapshot(2000L).UseBranch("b1");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    EXPECT_THAT(
        scan->PlanFiles(),
        ::testing::AllOf(
            IsError(ErrorKind::kValidationFailed),
            HasErrorMessage(
                "End snapshot is not a valid snapshot on the current branch: b1")));
  }

  // Test: from_snapshot_inclusive(snapshot_main_b).use_branch(b1) should fail
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot(2000L, /*inclusive=*/true).UseBranch("b1");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    EXPECT_THAT(
        scan->PlanFiles(),
        ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                         HasErrorMessage("Starting snapshot (inclusive) 2000 is not an "
                                         "ancestor of end snapshot 3000")));
  }
}

TEST_P(IncrementalAppendScanTest, UseBranchWithNonExistingRef) {
  auto metadata = MakeTableMetadata({}, -1L);
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalAppendScanBuilder::Make(metadata, file_io_));
  builder->UseBranch("non_existing_ref");
  EXPECT_THAT(builder->Build(),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Cannot find ref: non_existing_ref")));
}

TEST_P(IncrementalAppendScanTest, FromSnapshotExclusive) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});
  auto snapshot_c =
      MakeAppendSnapshot(version, 3000L, 2000L, 3L, {"/path/to/file_c.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_c}, 3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})}});

  // Test: from_snapshot_exclusive(snapshot_a) should return 2 files (B, C)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot(1000L, /*inclusive=*/false);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 2);
    EXPECT_THAT(GetPaths(tasks),
                testing::UnorderedElementsAre("/path/to/file_b.parquet",
                                              "/path/to/file_c.parquet"));
  }

  // Test: from_snapshot_exclusive(snapshot_a).to_snapshot(snapshot_b) should return 1
  // file (B)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot(1000L, /*inclusive=*/false).ToSnapshot(2000L);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(tasks[0]->data_file()->file_path, "/path/to/file_b.parquet");
  }
}

TEST_P(IncrementalAppendScanTest, FromSnapshotExclusiveWithNonExistingRef) {
  auto metadata = MakeTableMetadata({}, -1L);
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalAppendScanBuilder::Make(metadata, file_io_));
  builder->FromSnapshot("nonExistingRef", /*inclusive=*/false);
  EXPECT_THAT(builder->Build(),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Cannot find ref: nonExistingRef")));
}

TEST_P(IncrementalAppendScanTest, FromSnapshotExclusiveWithTag) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b = MakeAppendSnapshot(
      version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet", "/path/to/file_c.parquet"});
  auto snapshot_current = MakeAppendSnapshot(
      version, 3000L, 2000L, 3L, {"/path/to/file_d.parquet", "/path/to/file_a2.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_current}, 3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})},
       {"t1", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 1000L, .retention = SnapshotRef::Tag{}})},
       {"t2", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 2000L, .retention = SnapshotRef::Tag{}})}});

  // Test: from_snapshot_exclusive(t1) should return 4 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot("t1", /*inclusive=*/false);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 4);
  }

  // Test: from_snapshot_exclusive(t1).to_snapshot(t2) should return 2 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot("t1", /*inclusive=*/false).ToSnapshot("t2");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 2);
  }
}

TEST_P(IncrementalAppendScanTest, FromSnapshotExclusiveWithBranchShouldFail) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a}, 1000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 1000L, .retention = SnapshotRef::Branch{}})},
       {"b1", std::make_shared<SnapshotRef>(SnapshotRef{
                  .snapshot_id = 1000L, .retention = SnapshotRef::Branch{}})}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalAppendScanBuilder::Make(metadata, file_io_));
  builder->FromSnapshot("b1", /*inclusive=*/false);
  EXPECT_THAT(builder->Build(), ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                                                 HasErrorMessage("Ref b1 is not a tag")));
}

TEST_P(IncrementalAppendScanTest, ToSnapshot) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});
  auto snapshot_c =
      MakeAppendSnapshot(version, 3000L, 2000L, 3L, {"/path/to/file_c.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_c}, 3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})}});

  // Test: to_snapshot(snapshot_b) should return 2 files (A, B)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->ToSnapshot(2000L);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 2);
    EXPECT_THAT(GetPaths(tasks),
                testing::UnorderedElementsAre("/path/to/file_a.parquet",
                                              "/path/to/file_b.parquet"));
  }
}

TEST_P(IncrementalAppendScanTest, ToSnapshotWithTag) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});
  auto snapshot_current =
      MakeAppendSnapshot(version, 3000L, 2000L, 3L, {"/path/to/file_b2.parquet"});
  auto snapshot_branch_b =
      MakeAppendSnapshot(version, 4000L, 2000L, 3L, {"/path/to/file_c.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_current, snapshot_branch_b}, 3000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 3000L, .retention = SnapshotRef::Branch{}})},
       {"b1", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 4000L, .retention = SnapshotRef::Branch{}})},
       {"t1", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 2000L, .retention = SnapshotRef::Tag{}})},
       {"t2", std::make_shared<SnapshotRef>(
                  SnapshotRef{.snapshot_id = 4000L, .retention = SnapshotRef::Tag{}})}});

  // Test: to_snapshot(t1) should return 2 files
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->ToSnapshot("t1");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 2);
  }

  // Test: to_snapshot(t2) should return 3 files (on branch b1)
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->ToSnapshot("t2");
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 3);
  }
}

TEST_P(IncrementalAppendScanTest, ToSnapshotWithNonExistingRef) {
  auto metadata = MakeTableMetadata({}, -1L);
  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalAppendScanBuilder::Make(metadata, file_io_));
  builder->ToSnapshot("non_existing_ref");
  EXPECT_THAT(builder->Build(),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Cannot find ref: non_existing_ref")));
}

TEST_P(IncrementalAppendScanTest, ToSnapshotWithBranchShouldFail) {
  auto version = GetParam();

  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});

  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b}, 2000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 2000L, .retention = SnapshotRef::Branch{}})},
       {"b1", std::make_shared<SnapshotRef>(SnapshotRef{
                  .snapshot_id = 2000L, .retention = SnapshotRef::Branch{}})}});

  ICEBERG_UNWRAP_OR_FAIL(auto builder,
                         IncrementalAppendScanBuilder::Make(metadata, file_io_));
  builder->ToSnapshot("b1");
  EXPECT_THAT(builder->Build(), ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                                                 HasErrorMessage("Ref b1 is not a tag")));
}

TEST_P(IncrementalAppendScanTest, MultipleRootSnapshots) {
  auto version = GetParam();

  // Snapshot A (will be "expired" by not having it as parent of C)
  auto snapshot_a =
      MakeAppendSnapshot(version, 1000L, std::nullopt, 1L, {"/path/to/file_a.parquet"});
  // Snapshot B (staged, orphaned - not an ancestor of main branch)
  auto snapshot_b =
      MakeAppendSnapshot(version, 2000L, 1000L, 2L, {"/path/to/file_b.parquet"});
  // Snapshot C (new root after A is expired)
  auto snapshot_c =
      MakeAppendSnapshot(version, 3000L, std::nullopt, 3L, {"/path/to/file_c.parquet"});
  // Snapshot D
  auto snapshot_d =
      MakeAppendSnapshot(version, 4000L, 3000L, 4L, {"/path/to/file_d.parquet"});

  // Note: snapshot_a is kept in metadata but not in the parent chain of C/D
  // This simulates expiring snapshot A, creating two root snapshots (B and C)
  auto metadata = MakeTableMetadata(
      {snapshot_a, snapshot_b, snapshot_c, snapshot_d}, 4000L,
      {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                    .snapshot_id = 4000L, .retention = SnapshotRef::Branch{}})}});

  // Test: to_snapshot(snapshot_d) should discover snapshots C and D only
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->ToSnapshot(4000L);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_EQ(tasks.size(), 2);
    EXPECT_THAT(GetPaths(tasks),
                testing::UnorderedElementsAre("/path/to/file_c.parquet",
                                              "/path/to/file_d.parquet"));
  }

  // Test: from_snapshot_exclusive(snapshot_b).to_snapshot(snapshot_d) should fail
  // because B is not a parent ancestor of D
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot(2000L, /*inclusive=*/false).ToSnapshot(4000L);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    EXPECT_THAT(
        scan->PlanFiles(),
        ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                         HasErrorMessage("Starting snapshot (exclusive) 2000 is not a "
                                         "parent ancestor of end snapshot 4000")));
  }

  // Test: from_snapshot_inclusive(snapshot_b).to_snapshot(snapshot_d) should fail
  // because B is not an ancestor of D
  {
    ICEBERG_UNWRAP_OR_FAIL(auto builder,
                           IncrementalAppendScanBuilder::Make(metadata, file_io_));
    builder->FromSnapshot(2000L, /*inclusive=*/true).ToSnapshot(4000L);
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    EXPECT_THAT(
        scan->PlanFiles(),
        ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                         HasErrorMessage("Starting snapshot (inclusive) 2000 is not an "
                                         "ancestor of end snapshot 4000")));
  }
}

INSTANTIATE_TEST_SUITE_P(IncrementalAppendScanVersions, IncrementalAppendScanTest,
                         testing::Values(1, 2, 3));

}  // namespace iceberg
