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

#include "iceberg/update/expire_snapshots.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Basic API tests for ExpireSnapshots
// Full functional tests will be added when the implementation is complete

TEST(ExpireSnapshotsTest, FluentApiChaining) {
  // Test that the fluent API works correctly with method chaining
  ExpireSnapshots expire(nullptr);

  auto& result =
      expire.ExpireSnapshotId(123).ExpireOlderThan(1000000).RetainLast(5).SetCleanupLevel(
          CleanupLevel::kMetadataOnly);

  // Verify that chaining returns the same object
  EXPECT_EQ(&result, &expire);
}

TEST(ExpireSnapshotsTest, ExpireSnapshotId) {
  ExpireSnapshots expire(nullptr);
  expire.ExpireSnapshotId(123);

  // Currently returns NotImplemented - this test will be expanded
  // when the actual implementation is added
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, ExpireMultipleSnapshotIds) {
  ExpireSnapshots expire(nullptr);
  expire.ExpireSnapshotId(100).ExpireSnapshotId(200).ExpireSnapshotId(300);

  // Currently returns NotImplemented
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, ExpireOlderThan) {
  ExpireSnapshots expire(nullptr);
  int64_t timestamp = 1609459200000;  // 2021-01-01 00:00:00 UTC
  expire.ExpireOlderThan(timestamp);

  // Currently returns NotImplemented
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, RetainLast) {
  ExpireSnapshots expire(nullptr);
  expire.RetainLast(10);

  // Currently returns NotImplemented
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, DeleteWithCallback) {
  ExpireSnapshots expire(nullptr);
  std::vector<std::string> deleted_files;

  expire.DeleteWith(
      [&deleted_files](std::string_view file) { deleted_files.emplace_back(file); });

  // Currently returns NotImplemented
  auto result = expire.Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, CleanupLevelNone) {
  ExpireSnapshots expire(nullptr);
  expire.SetCleanupLevel(CleanupLevel::kNone);

  // Currently returns NotImplemented
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, CleanupLevelMetadataOnly) {
  ExpireSnapshots expire(nullptr);
  expire.SetCleanupLevel(CleanupLevel::kMetadataOnly);

  // Currently returns NotImplemented
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, CleanupLevelAll) {
  ExpireSnapshots expire(nullptr);
  expire.SetCleanupLevel(CleanupLevel::kAll);

  // Currently returns NotImplemented
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, CombinedConfiguration) {
  ExpireSnapshots expire(nullptr);
  int64_t timestamp = 1609459200000;

  expire.ExpireSnapshotId(100)
      .ExpireSnapshotId(200)
      .ExpireOlderThan(timestamp)
      .RetainLast(5)
      .SetCleanupLevel(CleanupLevel::kMetadataOnly);

  // Currently returns NotImplemented
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, CommitNotImplemented) {
  ExpireSnapshots expire(nullptr);
  expire.ExpireSnapshotId(123);

  // Currently returns NotImplemented
  auto result = expire.Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST(ExpireSnapshotsTest, ApplyNotImplemented) {
  ExpireSnapshots expire(nullptr);
  expire.ExpireOlderThan(1000000);

  // Currently returns NotImplemented
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

}  // namespace iceberg
