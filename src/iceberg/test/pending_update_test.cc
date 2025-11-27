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

#include "iceberg/pending_update.h"

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Mock implementation for testing the interface
class MockSnapshot {};

class MockPendingUpdate : public PendingUpdateTyped<MockSnapshot> {
 public:
  MockPendingUpdate() = default;

  Result<MockSnapshot> Apply() override {
    if (should_fail_) {
      return ValidationFailed("Mock validation failed");
    }
    apply_called_ = true;
    return MockSnapshot{};
  }

  Status Commit() override {
    if (should_fail_commit_) {
      return CommitFailed("Mock commit failed");
    }
    commit_called_ = true;
    return {};
  }

  void SetShouldFail(bool fail) { should_fail_ = fail; }
  void SetShouldFailCommit(bool fail) { should_fail_commit_ = fail; }
  bool ApplyCalled() const { return apply_called_; }
  bool CommitCalled() const { return commit_called_; }

 private:
  bool should_fail_ = false;
  bool should_fail_commit_ = false;
  bool apply_called_ = false;
  bool commit_called_ = false;
};

TEST(PendingUpdateTest, ApplySuccess) {
  MockPendingUpdate update;
  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(PendingUpdateTest, ApplyValidationFailed) {
  MockPendingUpdate update;
  update.SetShouldFail(true);
  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Mock validation failed"));
}

TEST(PendingUpdateTest, CommitSuccess) {
  MockPendingUpdate update;
  auto status = update.Commit();
  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(update.CommitCalled());
}

TEST(PendingUpdateTest, CommitFailed) {
  MockPendingUpdate update;
  update.SetShouldFailCommit(true);
  auto status = update.Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("Mock commit failed"));
}

TEST(PendingUpdateTest, BaseClassPolymorphism) {
  std::unique_ptr<PendingUpdate> base_ptr = std::make_unique<MockPendingUpdate>();
  auto status = base_ptr->Commit();
  EXPECT_THAT(status, IsOk());
}

}  // namespace iceberg
