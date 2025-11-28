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
class MockSnapshot {
 public:
  std::string name;
};

class MockPendingUpdate : public PendingUpdateTyped<MockSnapshot> {
 public:
  MockPendingUpdate() = default;

  // Builder-style methods that use error collection
  MockPendingUpdate& SetName(std::string_view name) {
    if (name.empty()) {
      AddError(ErrorKind::kInvalidArgument, "Name cannot be empty");
      return *this;
    }
    if (name.length() > 100) {
      AddError(ErrorKind::kInvalidArgument, "Name cannot exceed 100 characters");
      return *this;
    }
    name_ = name;
    return *this;
  }

  MockPendingUpdate& SetId(int64_t id) {
    if (id < 0) {
      AddError(ErrorKind::kInvalidArgument, "ID must be non-negative");
      return *this;
    }
    id_ = id;
    return *this;
  }

  Result<MockSnapshot> Apply() override {
    // Check for accumulated errors first
    auto error_status = CheckErrors();
    if (!error_status.has_value()) {
      return std::unexpected(error_status.error());
    }

    if (should_fail_) {
      return ValidationFailed("Mock validation failed");
    }
    apply_called_ = true;
    MockSnapshot snapshot;
    snapshot.name = name_;
    return snapshot;
  }

  Status Commit() override {
    // Check for accumulated errors first
    auto error_status = CheckErrors();
    if (!error_status.has_value()) {
      return error_status;
    }

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

  // Test helper to expose protected AddError(Error) method
  void TestAddExistingError(Error err) { AddError(std::move(err)); }

 private:
  std::string name_;
  int64_t id_ = 0;
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

// Error collection tests
TEST(PendingUpdateTest, ErrorCollectionSingleError) {
  MockPendingUpdate update;
  update.SetName("");  // This should add an error

  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Name cannot be empty"));
}

TEST(PendingUpdateTest, ErrorCollectionMultipleErrors) {
  MockPendingUpdate update;
  update.SetName("");  // Error: empty name
  update.SetId(-5);    // Error: negative ID

  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  // Should contain both error messages
  EXPECT_THAT(result, HasErrorMessage("Name cannot be empty"));
  EXPECT_THAT(result, HasErrorMessage("ID must be non-negative"));
}

TEST(PendingUpdateTest, ErrorCollectionInCommit) {
  MockPendingUpdate update;
  update.SetName("");  // This should add an error

  auto status = update.Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(status, HasErrorMessage("Name cannot be empty"));
}

TEST(PendingUpdateTest, ErrorCollectionValidInputNoErrors) {
  MockPendingUpdate update;
  update.SetName("valid_name");
  update.SetId(42);

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(result->name, "valid_name");
}

TEST(PendingUpdateTest, ErrorCollectionBuilderChaining) {
  MockPendingUpdate update;
  // Test that builder methods can be chained even when errors occur
  update.SetName("").SetId(-1);

  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  // Should contain both errors
  EXPECT_THAT(result, HasErrorMessage("Name cannot be empty"));
  EXPECT_THAT(result, HasErrorMessage("ID must be non-negative"));
}

TEST(PendingUpdateTest, ErrorCollectionPartialValidation) {
  MockPendingUpdate update;
  update.SetName("valid_name");  // Valid
  update.SetId(-1);              // Error

  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("ID must be non-negative"));
}

TEST(PendingUpdateTest, ErrorCollectionAddErrorOverload) {
  // Test the AddError(Error err) overload for adding existing error objects
  MockPendingUpdate update;

  // Create an error externally and add it
  Error external_error{.kind = ErrorKind::kInvalidArgument,
                       .message = "External error message"};
  update.TestAddExistingError(std::move(external_error));

  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("External error message"));
}

}  // namespace iceberg
