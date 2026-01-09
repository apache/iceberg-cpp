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

#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"

namespace iceberg {

class ExpireSnapshotsTest : public UpdateTestBase {
 protected:
};

TEST_F(ExpireSnapshotsTest, Empty) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_THAT(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_THAT(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
  EXPECT_THAT(result.ref_to_remove.empty(), true);
  EXPECT_THAT(result.schema_ids_to_remove.empty(), true);
  EXPECT_THAT(result.partition_spec_ids_to_remove.empty(), true);
}

TEST_F(ExpireSnapshotsTest, Keep2) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->RetainLast(2);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_THAT(result.snapshot_ids_to_remove.empty(), true);
  EXPECT_THAT(result.ref_to_remove.empty(), true);
  EXPECT_THAT(result.schema_ids_to_remove.empty(), true);
  EXPECT_THAT(result.partition_spec_ids_to_remove.empty(), true);
}

TEST_F(ExpireSnapshotsTest, ExpireById) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->ExpireSnapshotId(3051729675574597004);
  update->RetainLast(2);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_THAT(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_THAT(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
  EXPECT_THAT(result.ref_to_remove.empty(), true);
  EXPECT_THAT(result.schema_ids_to_remove.empty(), true);
  EXPECT_THAT(result.partition_spec_ids_to_remove.empty(), true);
}

TEST_F(ExpireSnapshotsTest, ExpireByIdNotExist) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->ExpireSnapshotId(3055729675574597003);
  update->RetainLast(2);
  auto result = update->Apply();
  EXPECT_THAT(result.has_value(), false);
  EXPECT_THAT(result.error().message.contains("Snapshot:3055729675574597003 not exist"),
              true);
}

TEST_F(ExpireSnapshotsTest, ExpireOlderThan1) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->ExpireOlderThan(1515100955770 - 1);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_THAT(result.snapshot_ids_to_remove.empty(), true);
  EXPECT_THAT(result.ref_to_remove.empty(), true);
  EXPECT_THAT(result.schema_ids_to_remove.empty(), true);
  EXPECT_THAT(result.partition_spec_ids_to_remove.empty(), true);
}

TEST_F(ExpireSnapshotsTest, ExpireOlderThan2) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->ExpireOlderThan(1515100955770 + 1);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_THAT(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_THAT(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
  EXPECT_THAT(result.ref_to_remove.empty(), true);
  EXPECT_THAT(result.schema_ids_to_remove.empty(), true);
  EXPECT_THAT(result.partition_spec_ids_to_remove.empty(), true);
}

}  // namespace iceberg
