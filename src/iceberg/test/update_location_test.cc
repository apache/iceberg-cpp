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

#include "iceberg/update/update_location.h"

#include <memory>
#include <string>

#include <arrow/filesystem/mockfs.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/result.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"

namespace iceberg {

class UpdateLocationTest : public UpdateTestBase {};

TEST_F(UpdateLocationTest, SetLocationSuccess) {
  const std::string new_location = "/warehouse/new_location";

  // Create metadata directory for the new location
  auto arrow_fs = std::dynamic_pointer_cast<::arrow::fs::internal::MockFileSystem>(
      static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_).fs());
  ASSERT_TRUE(arrow_fs != nullptr);
  ASSERT_TRUE(arrow_fs->CreateDir(new_location + "/metadata").ok());

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateLocation());
  update->SetLocation(new_location);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result, new_location);

  // Commit and verify the location was persisted
  EXPECT_THAT(update->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  EXPECT_EQ(reloaded->location(), new_location);
}

TEST_F(UpdateLocationTest, SetLocationMultipleTimes) {
  // Test that setting location multiple times uses the last value
  const std::string final_location = "/warehouse/final_location";

  // Create metadata directory for the new location
  auto arrow_fs = std::dynamic_pointer_cast<::arrow::fs::internal::MockFileSystem>(
      static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_).fs());
  ASSERT_TRUE(arrow_fs != nullptr);
  ASSERT_TRUE(arrow_fs->CreateDir(final_location + "/metadata").ok());

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateLocation());
  update->SetLocation("/warehouse/first_location")
      .SetLocation("/warehouse/second_location")
      .SetLocation(final_location);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result, final_location);

  // Commit and verify the final location was persisted
  EXPECT_THAT(update->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  EXPECT_EQ(reloaded->location(), final_location);
}

TEST_F(UpdateLocationTest, SetEmptyLocation) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateLocation());
  update->SetLocation("");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Location cannot be empty"));
}

TEST_F(UpdateLocationTest, ApplyWithoutSettingLocation) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateLocation());

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Location must be set before applying"));
}

TEST_F(UpdateLocationTest, MultipleUpdatesSequentially) {
  // Get arrow_fs for creating directories
  auto arrow_fs = std::dynamic_pointer_cast<::arrow::fs::internal::MockFileSystem>(
      static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_).fs());
  ASSERT_TRUE(arrow_fs != nullptr);

  // First update
  const std::string first_location = "/warehouse/first";
  ASSERT_TRUE(arrow_fs->CreateDir(first_location + "/metadata").ok());

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateLocation());
  update->SetLocation(first_location);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result, first_location);
  EXPECT_THAT(update->Commit(), IsOk());

  // Reload and verify
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  EXPECT_EQ(reloaded->location(), first_location);

  // Second update
  const std::string second_location = "/warehouse/second";
  ASSERT_TRUE(arrow_fs->CreateDir(second_location + "/metadata").ok());

  ICEBERG_UNWRAP_OR_FAIL(update, reloaded->NewUpdateLocation());
  update->SetLocation(second_location);
  ICEBERG_UNWRAP_OR_FAIL(result, update->Apply());
  EXPECT_EQ(result, second_location);
  EXPECT_THAT(update->Commit(), IsOk());

  // Reload and verify
  ICEBERG_UNWRAP_OR_FAIL(reloaded, catalog_->LoadTable(table_ident_));
  EXPECT_EQ(reloaded->location(), second_location);
}

}  // namespace iceberg
