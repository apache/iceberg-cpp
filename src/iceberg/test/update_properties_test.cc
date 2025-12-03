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

#include <cstddef>
#include <memory>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/file_format.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"

namespace iceberg {

class UpdatePropertiesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a simple schema
    SchemaField f(1, "col1", std::make_shared<LongType>(), false);
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{f}, 1);

    // Create basic table metadata
    metadata_ = std::make_shared<TableMetadata>();
    metadata_->schemas.push_back(schema_);

    // Create catalog and table identifier
    catalog_ = std::make_shared<MockCatalog>();
    identifier_ = TableIdentifier(Namespace({"test"}), "table");
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<TableMetadata> metadata_;
  std::shared_ptr<MockCatalog> catalog_;
  TableIdentifier identifier_;
};

TEST_F(UpdatePropertiesTest, EmptyUpdates) {
  UpdateProperties update(identifier_, catalog_, metadata_);

  auto result = update.Commit();
  EXPECT_THAT(result, IsOk());
}

TEST_F(UpdatePropertiesTest, SetProperty) {
  UpdateProperties update(identifier_, catalog_, metadata_);
  update.Set("key1", "value1").Set("key2", "value2");

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST_F(UpdatePropertiesTest, RemoveProperty) {
  UpdateProperties update(identifier_, catalog_, metadata_);
  update.Remove("key1").Remove("key2");

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST_F(UpdatePropertiesTest, SetRemoveConflict) {
  {
    // Set a property that is already marked for removal
    UpdateProperties update(identifier_, catalog_, metadata_);
    update.Set("key1", "value1").Remove("key1");

    auto result = update.Apply();
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("already marked for update"));
  }

  {
    // Remove a property that is already marked for update
    UpdateProperties update(identifier_, catalog_, metadata_);
    update.Remove("key1").Set("key1", "value1");

    auto result = update.Apply();
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("already marked for removal"));
  }
}

TEST_F(UpdatePropertiesTest, UpgradeFormatVersion) {
  {
    // Valid format-version upgrade
    UpdateProperties update(identifier_, catalog_, metadata_);
    update.Set("format-version", "2");

    auto result = update.Apply();
    EXPECT_THAT(result, IsOk());
  }

  {
    // Format-version is not a valid integer
    UpdateProperties update(identifier_, catalog_, metadata_);
    update.Set("format-version", "invalid");

    auto result = update.Apply();
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("Invalid format version"));
  }

  {
    // Format-version is out of range
    UpdateProperties update(identifier_, catalog_, metadata_);
    update.Set("format-version", "5000000000");

    auto result = update.Apply();
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("out of range"));
  }

  {
    // Format-version not supported
    UpdateProperties update(identifier_, catalog_, metadata_);
    update.Set("format-version",
               std::to_string(TableMetadata::kSupportedTableFormatVersion + 1));

    auto result = update.Apply();
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("unsupported format version"));
  }
}

TEST_F(UpdatePropertiesTest, InvalidTable) {
  {
    // catalog is null
    UpdateProperties update(identifier_, nullptr, metadata_);

    auto result = update.Apply();
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("Catalog is required"));
  }

  {
    // metadata is null
    UpdateProperties update(identifier_, catalog_, nullptr);

    auto result = update.Apply();
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("Base table metadata is required"));
  }
}

TEST_F(UpdatePropertiesTest, Commit) {
  {
    // Successful commit
    UpdateProperties update(identifier_, catalog_, metadata_);
    update.Set("key1", "value1");

    EXPECT_CALL(*catalog_, UpdateTable).Times(1).WillOnce(::testing::Return(nullptr));

    auto result = update.Commit();
    EXPECT_THAT(result, IsOk());
  }

  {
    // Failed commit
    UpdateProperties update(identifier_, catalog_, metadata_);
    update.Set("key1", "value1");

    EXPECT_CALL(*catalog_, UpdateTable)
        .WillOnce(::testing::Return(CommitFailed("Commit update failed")));
    auto result = update.Commit();
    EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  }
}

TEST_F(UpdatePropertiesTest, FluentInterface) {
  UpdateProperties update(identifier_, catalog_, metadata_);

  auto& ref = update.Set("key1", "value1").Remove("key2");

  // Should return reference to itself
  EXPECT_EQ(&ref, &update);

  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
}

}  // namespace iceberg
