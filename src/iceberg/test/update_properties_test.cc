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

#include "iceberg/update/update_properties.h"

#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"

namespace iceberg {

class UpdatePropertiesTest : public UpdateTestBase {};

TEST_F(UpdatePropertiesTest, SetProperty) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("key1", "value1").Set("key2", "value2");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.updates.size(), 1);
  EXPECT_EQ(result.updates[0]->kind(), table::SetProperties::Kind::kSetProperties);
}

TEST_F(UpdatePropertiesTest, RemoveProperty) {
  // First, add properties to remove
  ICEBERG_UNWRAP_OR_FAIL(auto setup_update, table_->NewUpdateProperties());
  setup_update->Set("key1", "value1").Set("key2", "value2");
  EXPECT_THAT(setup_update->Commit(), IsOk());

  // Reload and remove the properties
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateProperties());
  update->Remove("key1").Remove("key2");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.updates.size(), 1);
  EXPECT_EQ(result.updates[0]->kind(), table::RemoveProperties::Kind::kRemoveProperties);
}

TEST_F(UpdatePropertiesTest, SetThenRemoveSameKey) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("key1", "value1").Remove("key1");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("already marked for update"));
}

TEST_F(UpdatePropertiesTest, RemoveThenSetSameKey) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Remove("key1").Set("key1", "value1");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("already marked for removal"));
}

TEST_F(UpdatePropertiesTest, SetAndRemoveDifferentKeys) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("key1", "value1").Remove("key2");
  EXPECT_THAT(update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  const auto& props = reloaded->properties().configs();
  EXPECT_EQ(props.at("key1"), "value1");
  EXPECT_FALSE(props.contains("key2"));
}

TEST_F(UpdatePropertiesTest, UpgradeFormatVersionValid) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("format-version", "2");

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.updates.size(), 1);
  EXPECT_EQ(result.updates[0]->kind(),
            table::UpgradeFormatVersion::Kind::kUpgradeFormatVersion);
}

TEST_F(UpdatePropertiesTest, UpgradeFormatVersionInvalidString) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("format-version", "invalid");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Invalid format version"));
}

TEST_F(UpdatePropertiesTest, UpgradeFormatVersionOutOfRange) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("format-version", "5000000000");

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("out of range"));
}

TEST_F(UpdatePropertiesTest, UpgradeFormatVersionUnsupported) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("format-version",
              std::to_string(TableMetadata::kSupportedTableFormatVersion + 1));

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("unsupported format version"));
}

TEST_F(UpdatePropertiesTest, CommitSuccess) {
  ICEBERG_UNWRAP_OR_FAIL(auto empty_update, table_->NewUpdateProperties());
  EXPECT_THAT(empty_update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("new.property", "new.value");
  update->Set("format-version", "3");

  EXPECT_THAT(update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  const auto& props = reloaded->properties().configs();
  EXPECT_EQ(props.at("new.property"), "new.value");
  const auto& format_version = reloaded->metadata()->format_version;
  EXPECT_EQ(format_version, 3);
}

}  // namespace iceberg
