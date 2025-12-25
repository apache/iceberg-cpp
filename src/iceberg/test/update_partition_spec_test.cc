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

#include "iceberg/update/update_partition_spec.h"

#include <format>
#include <memory>
#include <string>
#include <vector>

#include <arrow/filesystem/mockfs.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transaction.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

class UpdatePartitionSpecTest : public ::testing::TestWithParam<int8_t> {
 protected:
  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    catalog_ = InMemoryCatalog::Make("test_catalog", file_io_, "/warehouse/", {});
    format_version_ = GetParam();
    test_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeRequired(2, "ts", timestamp_tz()),
                                 SchemaField::MakeRequired(3, "category", string()),
                                 SchemaField::MakeOptional(4, "data", string())},
        0);

    // Create unpartitioned and partitioned specs matching Java test
    ICEBERG_UNWRAP_OR_FAIL(
        auto unpartitioned_spec,
        PartitionSpec::Make(PartitionSpec::kInitialSpecId, std::vector<PartitionField>{},
                            PartitionSpec::kLegacyPartitionDataIdStart - 1));
    ICEBERG_UNWRAP_OR_FAIL(
        partitioned_spec_,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Bucket(16))},
            1002));

    auto partitioned_metadata =
        CreateBaseMetadata(format_version_, test_schema_, partitioned_spec_);
    auto unpartitioned_metadata =
        CreateBaseMetadata(format_version_, test_schema_, std::move(unpartitioned_spec));

    // Write metadata files
    partitioned_metadata->location = partitioned_table_location_;
    unpartitioned_metadata->location = unpartitioned_table_location_;

    // Arrow MockFS cannot automatically create directories.
    auto arrow_fs = std::dynamic_pointer_cast<::arrow::fs::internal::MockFileSystem>(
        static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_).fs());
    ASSERT_TRUE(arrow_fs != nullptr);
    ASSERT_TRUE(arrow_fs->CreateDir(partitioned_table_location_ + "/metadata").ok());
    ASSERT_TRUE(arrow_fs->CreateDir(unpartitioned_table_location_ + "/metadata").ok());

    // Write table metadata to the table location.
    std::string partitioned_metadata_location =
        std::format("{}/metadata/00001-{}.metadata.json", partitioned_table_location_,
                    Uuid::GenerateV7().ToString());
    std::string unpartitioned_metadata_location =
        std::format("{}/metadata/00001-{}.metadata.json", unpartitioned_table_location_,
                    Uuid::GenerateV7().ToString());

    ASSERT_THAT(TableMetadataUtil::Write(*file_io_, partitioned_metadata_location,
                                         *partitioned_metadata),
                IsOk());
    ASSERT_THAT(TableMetadataUtil::Write(*file_io_, unpartitioned_metadata_location,
                                         *unpartitioned_metadata),
                IsOk());

    // Register the tables in the catalog.
    ICEBERG_UNWRAP_OR_FAIL(
        partitioned_table_,
        catalog_->RegisterTable(partitioned_table_ident_, partitioned_metadata_location));
    ICEBERG_UNWRAP_OR_FAIL(unpartitioned_table_,
                           catalog_->RegisterTable(unpartitioned_table_ident_,
                                                   unpartitioned_metadata_location));
  }

  // Helper to create base metadata with a specific partition spec
  std::unique_ptr<TableMetadata> CreateBaseMetadata(int8_t format_version,
                                                    std::shared_ptr<Schema> schema,
                                                    std::shared_ptr<PartitionSpec> spec) {
    auto metadata = std::make_unique<TableMetadata>();
    metadata->format_version = format_version;
    metadata->table_uuid = "test-uuid-1234";
    metadata->location = "/warehouse/test_table";
    metadata->last_sequence_number = 0;
    metadata->last_updated_ms = TimePointMs{std::chrono::milliseconds(1000)};
    metadata->last_column_id = 4;
    metadata->current_schema_id = 0;
    metadata->schemas.push_back(std::move(schema));
    metadata->default_spec_id = spec->spec_id();
    metadata->last_partition_id = spec->last_assigned_field_id();
    metadata->current_snapshot_id = Snapshot::kInvalidSnapshotId;
    metadata->default_sort_order_id = SortOrder::kUnsortedOrderId;
    metadata->sort_orders.push_back(SortOrder::Unsorted());
    metadata->next_row_id = TableMetadata::kInitialRowId;
    metadata->properties = TableProperties::default_properties();
    metadata->partition_specs.push_back(std::move(spec));
    return metadata;
  }

  // Helper to create UpdatePartitionSpec with a specific partition spec
  std::shared_ptr<UpdatePartitionSpec> CreateUpdatePartitionSpec(bool partitioned) {
    if (partitioned) {
      auto update_result = partitioned_table_->NewUpdatePartitionSpec();
      if (!update_result.has_value()) {
        ADD_FAILURE() << "Failed to create update: " << update_result.error().message;
        return nullptr;
      }
      return update_result.value();
    } else {
      auto update_result = unpartitioned_table_->NewUpdatePartitionSpec();
      if (!update_result.has_value()) {
        ADD_FAILURE() << "Failed to create update: " << update_result.error().message;
        return nullptr;
      }
      return update_result.value();
    }
  }

  // Helper to create an expected partition spec
  std::shared_ptr<PartitionSpec> MakeExpectedSpec(
      const std::vector<PartitionField>& fields, int32_t last_assigned_field_id) {
    auto spec_result = PartitionSpec::Make(PartitionSpec::kInitialSpecId, fields,
                                           last_assigned_field_id);
    if (!spec_result.has_value()) {
      ADD_FAILURE() << "Failed to create expected spec: " << spec_result.error().message;
      return nullptr;
    }
    return std::shared_ptr<PartitionSpec>(spec_result.value().release());
  }

  // Helper to apply update and get the resulting spec
  std::shared_ptr<PartitionSpec> ApplyUpdateAndGetSpec(
      std::shared_ptr<UpdatePartitionSpec> update) {
    auto result = update->Apply();
    if (!result.has_value()) {
      ADD_FAILURE() << "Failed to apply update: " << result.error().message;
      return nullptr;
    }
    return result.value().spec;
  }

  // Helper to apply update and assert spec equality
  void ApplyUpdateAndAssertSpec(std::shared_ptr<UpdatePartitionSpec> update,
                                const std::vector<PartitionField>& expected_fields,
                                int32_t last_assigned_field_id) {
    auto updated_spec = ApplyUpdateAndGetSpec(update);
    auto expected_spec = MakeExpectedSpec(expected_fields, last_assigned_field_id);
    AssertPartitionSpecEquals(*expected_spec, *updated_spec);
  }

  // Helper to assert partition spec equality
  void AssertPartitionSpecEquals(const PartitionSpec& expected,
                                 const PartitionSpec& actual) {
    ASSERT_EQ(expected.fields().size(), actual.fields().size());
    for (size_t i = 0; i < expected.fields().size(); ++i) {
      const auto& expected_field = expected.fields()[i];
      const auto& actual_field = actual.fields()[i];
      EXPECT_EQ(expected_field.source_id(), actual_field.source_id());
      EXPECT_EQ(expected_field.field_id(), actual_field.field_id());
      EXPECT_EQ(expected_field.name(), actual_field.name());
      EXPECT_EQ(*expected_field.transform(), *actual_field.transform());
    }
  }

  // Helper to expect an error with a specific message
  void ExpectError(std::shared_ptr<UpdatePartitionSpec> update, ErrorKind expected_kind,
                   const std::string& expected_message) {
    auto result = update->Apply();
    ASSERT_THAT(result, IsError(expected_kind));
    ASSERT_THAT(result, HasErrorMessage(expected_message));
  }

  // Helper to create a table with a custom partition spec
  std::shared_ptr<Table> CreateTableWithSpec(std::shared_ptr<PartitionSpec> spec,
                                             const std::string& table_name) {
    auto metadata = CreateBaseMetadata(format_version_, test_schema_, spec);
    TableIdentifier identifier{.ns = Namespace{.levels = {"test"}}, .name = table_name};
    std::string metadata_location =
        std::format("/warehouse/{}/metadata/00000-{}.metadata.json", table_name,
                    Uuid::GenerateV7().ToString());
    auto table_result =
        Table::Make(identifier, std::shared_ptr<TableMetadata>(metadata.release()),
                    metadata_location, file_io_, catalog_);
    if (!table_result.has_value()) {
      ADD_FAILURE() << "Failed to create table: " << table_result.error().message;
      return nullptr;
    }
    return table_result.value();
  }

  // Helper to create UpdatePartitionSpec from a table
  std::shared_ptr<UpdatePartitionSpec> CreateUpdateFromTable(
      std::shared_ptr<Table> table) {
    auto transaction_result =
        Transaction::Make(table, Transaction::Kind::kUpdate, /*auto_commit=*/false);
    if (!transaction_result.has_value()) {
      ADD_FAILURE() << "Failed to create transaction: "
                    << transaction_result.error().message;
      return nullptr;
    }
    auto update_result = UpdatePartitionSpec::Make(transaction_result.value());
    if (!update_result.has_value()) {
      ADD_FAILURE() << "Failed to create UpdatePartitionSpec: "
                    << update_result.error().message;
      return nullptr;
    }
    return update_result.value();
  }

  const TableIdentifier partitioned_table_ident_{.name = "partitioned_table"};
  const TableIdentifier unpartitioned_table_ident_{.name = "unpartitioned_table"};
  const std::string partitioned_table_location_{"/warehouse/partitioned_table"};
  const std::string unpartitioned_table_location_{"/warehouse/unpartitioned_table"};
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<InMemoryCatalog> catalog_;
  std::shared_ptr<Schema> test_schema_;
  std::shared_ptr<PartitionSpec> partitioned_spec_;
  std::shared_ptr<Table> partitioned_table_;
  std::shared_ptr<Table> unpartitioned_table_;
  int8_t format_version_;
};

INSTANTIATE_TEST_SUITE_P(FormatVersions, UpdatePartitionSpecTest, ::testing::Values(1, 2),
                         [](const ::testing::TestParamInfo<int8_t>& info) {
                           return std::format("V{}", info.param);
                         });

TEST_P(UpdatePartitionSpecTest, TestAddIdentityByName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField("category");
  ApplyUpdateAndAssertSpec(
      update, {PartitionField(3, 1000, "category", Transform::Identity())}, 1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddIdentityByTerm) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Ref("category"));
  ApplyUpdateAndAssertSpec(
      update, {PartitionField(3, 1000, "category", Transform::Identity())}, 1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddYear) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Year("ts"));
  ApplyUpdateAndAssertSpec(update,
                           {PartitionField(2, 1000, "ts_year", Transform::Year())}, 1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddMonth) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Month("ts"));
  ApplyUpdateAndAssertSpec(
      update, {PartitionField(2, 1000, "ts_month", Transform::Month())}, 1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddDay) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Day("ts"));
  ApplyUpdateAndAssertSpec(update, {PartitionField(2, 1000, "ts_day", Transform::Day())},
                           1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddHour) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Hour("ts"));
  ApplyUpdateAndAssertSpec(update,
                           {PartitionField(2, 1000, "ts_hour", Transform::Hour())}, 1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddBucket) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Bucket("id", 16));
  ApplyUpdateAndAssertSpec(
      update, {PartitionField(1, 1000, "id_bucket_16", Transform::Bucket(16))}, 1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddTruncate) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Truncate("data", 4));
  ApplyUpdateAndAssertSpec(
      update, {PartitionField(4, 1000, "data_trunc_4", Transform::Truncate(4))}, 1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddNamedPartition) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Bucket("id", 16), "shard");
  ApplyUpdateAndAssertSpec(
      update, {PartitionField(1, 1000, "shard", Transform::Bucket(16))}, 1000);
}

TEST_P(UpdatePartitionSpecTest, TestAddToExisting) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Truncate("data", 4));
  ApplyUpdateAndAssertSpec(
      update,
      {PartitionField(3, 1000, "category", Transform::Identity()),
       PartitionField(2, 1001, "ts_day", Transform::Day()),
       PartitionField(1, 1002, "shard", Transform::Bucket(16)),
       PartitionField(4, 1003, "data_trunc_4", Transform::Truncate(4))},
      1003);
}

TEST_P(UpdatePartitionSpecTest, TestMultipleAdds) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, unpartitioned_table_->NewUpdatePartitionSpec());
  update->AddField("category")
      .AddField(Expressions::Day("ts"))
      .AddField(Expressions::Bucket("id", 16), "shard")
      .AddField(Expressions::Truncate("data", 4), "prefix");
  ApplyUpdateAndAssertSpec(update,
                           {PartitionField(3, 1000, "category", Transform::Identity()),
                            PartitionField(2, 1001, "ts_day", Transform::Day()),
                            PartitionField(1, 1002, "shard", Transform::Bucket(16)),
                            PartitionField(4, 1003, "prefix", Transform::Truncate(4))},
                           1003);
}

TEST_P(UpdatePartitionSpecTest, TestAddHourToDay) {
  // First add day partition
  ICEBERG_UNWRAP_OR_FAIL(auto update1, unpartitioned_table_->NewUpdatePartitionSpec());
  update1->AddField(Expressions::Day("ts"));
  auto by_day_spec = ApplyUpdateAndGetSpec(update1);

  // Then add hour partition
  auto table = CreateTableWithSpec(by_day_spec, "test_table");
  auto update2 = CreateUpdateFromTable(table);
  update2->AddField(Expressions::Hour("ts"));
  auto by_hour_spec = ApplyUpdateAndGetSpec(update2);

  ASSERT_EQ(by_hour_spec->fields().size(), 2);
  EXPECT_EQ(by_hour_spec->fields()[0].source_id(), 2);
  EXPECT_EQ(by_hour_spec->fields()[0].name(), "ts_day");
  EXPECT_EQ(*by_hour_spec->fields()[0].transform(), *Transform::Day());
  EXPECT_EQ(by_hour_spec->fields()[1].source_id(), 2);
  EXPECT_EQ(by_hour_spec->fields()[1].name(), "ts_hour");
  EXPECT_EQ(*by_hour_spec->fields()[1].transform(), *Transform::Hour());
}

TEST_P(UpdatePartitionSpecTest, TestAddMultipleBuckets) {
  // First add bucket 16
  ICEBERG_UNWRAP_OR_FAIL(auto update1, unpartitioned_table_->NewUpdatePartitionSpec());
  update1->AddField(Expressions::Bucket("id", 16));
  auto bucket16_spec = ApplyUpdateAndGetSpec(update1);

  // Then add bucket 8
  auto table = CreateTableWithSpec(bucket16_spec, "test_table");
  auto update2 = CreateUpdateFromTable(table);
  update2->AddField(Expressions::Bucket("id", 8));
  ApplyUpdateAndAssertSpec(
      update2,
      {PartitionField(1, 1000, "id_bucket_16", Transform::Bucket(16)),
       PartitionField(1, 1001, "id_bucket_8", Transform::Bucket(8))},
      1001);
}

TEST_P(UpdatePartitionSpecTest, TestRemoveIdentityByName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField("category");
  auto updated_spec = ApplyUpdateAndGetSpec(update);
  if (format_version_ == 1) {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Void()),
                          PartitionField(2, 1001, "ts_day", Transform::Day()),
                          PartitionField(1, 1002, "shard", Transform::Bucket(16))},
                         1002);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    auto expected =
        MakeExpectedSpec({PartitionField(2, 1001, "ts_day", Transform::Day()),
                          PartitionField(1, 1002, "shard", Transform::Bucket(16))},
                         1002);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveBucketByName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField("shard");
  auto updated_spec = ApplyUpdateAndGetSpec(update);
  if (format_version_ == 1) {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Identity()),
                          PartitionField(2, 1001, "ts_day", Transform::Day()),
                          PartitionField(1, 1002, "shard", Transform::Void())},
                         1002);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Identity()),
                          PartitionField(2, 1001, "ts_day", Transform::Day())},
                         1001);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveIdentityByEquivalent) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField(Expressions::Ref("category"));
  auto updated_spec = ApplyUpdateAndGetSpec(update);
  if (format_version_ == 1) {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Void()),
                          PartitionField(2, 1001, "ts_day", Transform::Day()),
                          PartitionField(1, 1002, "shard", Transform::Bucket(16))},
                         1002);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    auto expected =
        MakeExpectedSpec({PartitionField(2, 1001, "ts_day", Transform::Day()),
                          PartitionField(1, 1002, "shard", Transform::Bucket(16))},
                         1002);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveDayByEquivalent) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField(Expressions::Day("ts"));
  auto updated_spec = ApplyUpdateAndGetSpec(update);
  if (format_version_ == 1) {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Identity()),
                          PartitionField(2, 1001, "ts_day", Transform::Void()),
                          PartitionField(1, 1002, "shard", Transform::Bucket(16))},
                         1002);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Identity()),
                          PartitionField(1, 1002, "shard", Transform::Bucket(16))},
                         1002);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveBucketByEquivalent) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField(Expressions::Bucket("id", 16));
  auto updated_spec = ApplyUpdateAndGetSpec(update);
  if (format_version_ == 1) {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Identity()),
                          PartitionField(2, 1001, "ts_day", Transform::Day()),
                          PartitionField(1, 1002, "shard", Transform::Void())},
                         1002);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Identity()),
                          PartitionField(2, 1001, "ts_day", Transform::Day())},
                         1001);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRename) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RenameField("shard", "id_bucket");
  ApplyUpdateAndAssertSpec(update,
                           {PartitionField(3, 1000, "category", Transform::Identity()),
                            PartitionField(2, 1001, "ts_day", Transform::Day()),
                            PartitionField(1, 1002, "id_bucket", Transform::Bucket(16))},
                           1002);
}

TEST_P(UpdatePartitionSpecTest, TestMultipleChanges) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RenameField("shard", "id_bucket")
      .RemoveField(Expressions::Day("ts"))
      .AddField(Expressions::Truncate("data", 4), "prefix");
  auto updated_spec = ApplyUpdateAndGetSpec(update);
  if (format_version_ == 1) {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Identity()),
                          PartitionField(2, 1001, "ts_day", Transform::Void()),
                          PartitionField(1, 1002, "id_bucket", Transform::Bucket(16)),
                          PartitionField(4, 1003, "prefix", Transform::Truncate(4))},
                         1003);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    auto expected =
        MakeExpectedSpec({PartitionField(3, 1000, "category", Transform::Identity()),
                          PartitionField(1, 1002, "id_bucket", Transform::Bucket(16)),
                          PartitionField(4, 1003, "prefix", Transform::Truncate(4))},
                         1003);
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestAddDeletedName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField(Expressions::Bucket("id", 16));
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  auto updated_spec = result.spec;

  if (format_version_ == 1) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Void())},
            1002));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day())},
            1001));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveNewlyAddedFieldByName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Truncate("data", 4), "prefix");
  update->RemoveField("prefix");
  ExpectError(update, ErrorKind::kValidationFailed, "Cannot delete newly added field");
}

TEST_P(UpdatePartitionSpecTest, TestRemoveNewlyAddedFieldByTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Truncate("data", 4), "prefix");
  update->RemoveField(Expressions::Truncate("data", 4));
  ExpectError(update, ErrorKind::kValidationFailed, "Cannot delete newly added field");
}

TEST_P(UpdatePartitionSpecTest, TestAddAlreadyAddedFieldByTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Truncate("data", 4), "prefix");
  update->AddField(Expressions::Truncate("data", 4));
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot add duplicate partition field");
}

TEST_P(UpdatePartitionSpecTest, TestAddAlreadyAddedFieldByName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Truncate("data", 4), "prefix");
  update->AddField(Expressions::Truncate("data", 6), "prefix");
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot add duplicate partition field");
}

TEST_P(UpdatePartitionSpecTest, TestAddRedundantTimePartition) {
  // Test day + hour conflict
  ICEBERG_UNWRAP_OR_FAIL(auto update1, unpartitioned_table_->NewUpdatePartitionSpec());
  update1->AddField(Expressions::Day("ts"));
  update1->AddField(Expressions::Hour("ts"));
  ExpectError(update1, ErrorKind::kValidationFailed,
              "Cannot add redundant partition field");

  // Test hour + month conflict after adding hour to existing day
  ICEBERG_UNWRAP_OR_FAIL(auto update2, partitioned_table_->NewUpdatePartitionSpec());
  update2->AddField(Expressions::Hour("ts"));   // day already exists, so hour is OK
  update2->AddField(Expressions::Month("ts"));  // conflicts with hour
  ExpectError(update2, ErrorKind::kValidationFailed,
              "Cannot add redundant partition field");
}

TEST_P(UpdatePartitionSpecTest, TestNoEffectAddDeletedSameFieldWithSameName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update1, partitioned_table_->NewUpdatePartitionSpec());
  update1->RemoveField("shard");
  update1->AddField(Expressions::Bucket("id", 16), "shard");
  ICEBERG_UNWRAP_OR_FAIL(auto result1, update1->Apply());
  auto spec1 = result1.spec;
  AssertPartitionSpecEquals(*partitioned_spec_, *spec1);

  ICEBERG_UNWRAP_OR_FAIL(auto update2, partitioned_table_->NewUpdatePartitionSpec());
  update2->RemoveField("shard");
  update2->AddField(Expressions::Bucket("id", 16));
  ICEBERG_UNWRAP_OR_FAIL(auto result2, update2->Apply());
  auto spec2 = result2.spec;
  AssertPartitionSpecEquals(*partitioned_spec_, *spec2);
}

TEST_P(UpdatePartitionSpecTest, TestGenerateNewSpecAddDeletedSameFieldWithDifferentName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField("shard");
  update->AddField(Expressions::Bucket("id", 16), "new_shard");
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  auto updated_spec = result.spec;

  ASSERT_EQ(updated_spec->fields().size(), 3);
  EXPECT_EQ(updated_spec->fields()[0].name(), "category");
  EXPECT_EQ(updated_spec->fields()[1].name(), "ts_day");
  EXPECT_EQ(updated_spec->fields()[2].name(), "new_shard");
  EXPECT_EQ(*updated_spec->fields()[0].transform(), *Transform::Identity());
  EXPECT_EQ(*updated_spec->fields()[1].transform(), *Transform::Day());
  EXPECT_EQ(*updated_spec->fields()[2].transform(), *Transform::Bucket(16));
}

TEST_P(UpdatePartitionSpecTest, TestAddDuplicateByName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField("category");
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot add duplicate partition field");
}

TEST_P(UpdatePartitionSpecTest, TestAddDuplicateByRef) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Ref("category"));
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot add duplicate partition field");
}

TEST_P(UpdatePartitionSpecTest, TestAddDuplicateTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Bucket("id", 16));
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot add duplicate partition field");
}

TEST_P(UpdatePartitionSpecTest, TestAddNamedDuplicate) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Bucket("id", 16), "b16");
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot add duplicate partition field");
}

TEST_P(UpdatePartitionSpecTest, TestRemoveUnknownFieldByName) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField("moon");
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot find partition field to remove");
}

TEST_P(UpdatePartitionSpecTest, TestRemoveUnknownFieldByEquivalent) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField(Expressions::Hour("ts"));  // day(ts) exists, not hour
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot find partition field to remove");
}

TEST_P(UpdatePartitionSpecTest, TestRenameUnknownField) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RenameField("shake", "seal");
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot find partition field to rename: shake");
}

TEST_P(UpdatePartitionSpecTest, TestRenameAfterAdd) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->AddField(Expressions::Truncate("data", 4), "data_trunc");
  update->RenameField("data_trunc", "prefix");
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot rename newly added partition field: data_trunc");
}

TEST_P(UpdatePartitionSpecTest, TestRenameAndDelete) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RenameField("shard", "id_bucket");
  update->RemoveField(Expressions::Bucket("id", 16));
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot rename and delete partition field: shard");
}

TEST_P(UpdatePartitionSpecTest, TestDeleteAndRename) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, partitioned_table_->NewUpdatePartitionSpec());
  update->RemoveField(Expressions::Bucket("id", 16));
  update->RenameField("shard", "id_bucket");
  ExpectError(update, ErrorKind::kValidationFailed,
              "Cannot delete and rename partition field: shard");
}

TEST_P(UpdatePartitionSpecTest, TestRemoveAndAddMultiTimes) {
  // Add first time
  ICEBERG_UNWRAP_OR_FAIL(auto update1, unpartitioned_table_->NewUpdatePartitionSpec());
  update1->AddField(Expressions::Day("ts"), "ts_date");
  auto add_first_time_spec = ApplyUpdateAndGetSpec(update1);

  // Remove first time
  auto table1 = CreateTableWithSpec(add_first_time_spec, "test_table");
  auto update2 = CreateUpdateFromTable(table1);
  update2->RemoveField(Expressions::Day("ts"));
  auto remove_first_time_spec = ApplyUpdateAndGetSpec(update2);

  // Add second time
  auto table2 = CreateTableWithSpec(remove_first_time_spec, "test_table2");
  auto update3 = CreateUpdateFromTable(table2);
  update3->AddField(Expressions::Day("ts"), "ts_date");
  auto add_second_time_spec = ApplyUpdateAndGetSpec(update3);

  // Remove second time
  auto table3 = CreateTableWithSpec(add_second_time_spec, "test_table3");
  auto update4 = CreateUpdateFromTable(table3);
  update4->RemoveField(Expressions::Day("ts"));
  auto remove_second_time_spec = ApplyUpdateAndGetSpec(update4);

  // Add third time with month
  auto table4 = CreateTableWithSpec(remove_second_time_spec, "test_table4");
  auto update5 = CreateUpdateFromTable(table4);
  update5->AddField(Expressions::Month("ts"));
  auto add_third_time_spec = ApplyUpdateAndGetSpec(update5);

  // Rename ts_month to ts_date
  auto table5 = CreateTableWithSpec(add_third_time_spec, "test_table5");
  auto update6 = CreateUpdateFromTable(table5);
  update6->RenameField("ts_month", "ts_date");
  auto updated_spec = ApplyUpdateAndGetSpec(update6);

  if (format_version_ == 1) {
    ASSERT_EQ(updated_spec->fields().size(), 3);
    // In V1, we expect void transforms for deleted fields
    EXPECT_TRUE(updated_spec->fields()[0].name().find("ts_date") == 0);
    EXPECT_TRUE(updated_spec->fields()[1].name().find("ts_date") == 0);
    EXPECT_EQ(updated_spec->fields()[2].name(), "ts_date");
    EXPECT_EQ(*updated_spec->fields()[0].transform(), *Transform::Void());
    EXPECT_EQ(*updated_spec->fields()[1].transform(), *Transform::Void());
    EXPECT_EQ(*updated_spec->fields()[2].transform(), *Transform::Month());
  } else {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                            std::vector<PartitionField>{
                                PartitionField(2, 1000, "ts_date", Transform::Month())},
                            1000));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveAndUpdateWithDifferentTransformation) {
  auto initial_spec = MakeExpectedSpec(
      {PartitionField(2, 1000, "ts_transformed", Transform::Month())}, 1000);
  auto table = CreateTableWithSpec(initial_spec, "test_table");
  auto update = CreateUpdateFromTable(table);
  update->RemoveField("ts_transformed");
  update->AddField(Expressions::Day("ts"), "ts_transformed");
  auto updated_spec = ApplyUpdateAndGetSpec(update);

  if (format_version_ == 1) {
    ASSERT_EQ(updated_spec->fields().size(), 2);
    EXPECT_TRUE(updated_spec->fields()[0].name().find("ts_transformed") == 0);
    EXPECT_EQ(updated_spec->fields()[1].name(), "ts_transformed");
    EXPECT_EQ(*updated_spec->fields()[0].transform(), *Transform::Void());
    EXPECT_EQ(*updated_spec->fields()[1].transform(), *Transform::Day());
  } else {
    ASSERT_EQ(updated_spec->fields().size(), 1);
    EXPECT_EQ(updated_spec->fields()[0].name(), "ts_transformed");
    EXPECT_EQ(*updated_spec->fields()[0].transform(), *Transform::Day());
  }
}

TEST_P(UpdatePartitionSpecTest, CommitSuccess) {
  // Test empty commit
  ICEBERG_UNWRAP_OR_FAIL(auto empty_update, partitioned_table_->NewUpdatePartitionSpec());
  EXPECT_THAT(empty_update->Commit(), IsOk());

  // Reload table after first commit
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(partitioned_table_ident_));

  // Test commit with partition spec changes
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdatePartitionSpec());
  update->AddField(Expressions::Truncate("data", 4), "prefix");

  EXPECT_THAT(update->Commit(), IsOk());

  // Verify the partition spec was committed
  ICEBERG_UNWRAP_OR_FAIL(auto final_table, catalog_->LoadTable(partitioned_table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto spec, final_table->spec());

  ASSERT_EQ(spec->fields().size(), 4);
  EXPECT_EQ(spec->fields()[0].name(), "category");
  EXPECT_EQ(spec->fields()[1].name(), "ts_day");
  EXPECT_EQ(spec->fields()[2].name(), "shard");
  EXPECT_EQ(spec->fields()[3].name(), "prefix");
  EXPECT_EQ(*spec->fields()[0].transform(), *Transform::Identity());
  EXPECT_EQ(*spec->fields()[1].transform(), *Transform::Day());
  EXPECT_EQ(*spec->fields()[2].transform(), *Transform::Bucket(16));
  EXPECT_EQ(*spec->fields()[3].transform(), *Transform::Truncate(4));
}

}  // namespace iceberg
