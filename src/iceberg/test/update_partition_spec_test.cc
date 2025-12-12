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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

// Test schema matching Java test
std::shared_ptr<Schema> CreateTestSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                               SchemaField::MakeRequired(2, "ts", timestamp_tz()),
                               SchemaField::MakeRequired(3, "category", string()),
                               SchemaField::MakeOptional(4, "data", string())},
      0);
}

// Create partitioned spec matching Java test
std::shared_ptr<PartitionSpec> CreatePartitionedSpec() {
  ICEBERG_ASSIGN_OR_THROW(
      auto spec_result,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{
                              PartitionField(3, 1000, "category", Transform::Identity()),
                              PartitionField(2, 1001, "ts_day", Transform::Day()),
                              PartitionField(1, 1002, "shard", Transform::Bucket(16))}));
  return spec_result;
}

// Create base metadata for testing
std::shared_ptr<TableMetadata> CreateBaseMetadata(int8_t format_version,
                                                  std::shared_ptr<PartitionSpec> spec) {
  auto metadata = std::make_shared<TableMetadata>();
  metadata->format_version = format_version;
  metadata->table_uuid = "test-uuid-1234";
  metadata->location = "s3://bucket/test";
  metadata->last_sequence_number = 0;
  metadata->last_updated_ms = TimePointMs{std::chrono::milliseconds(1000)};
  metadata->last_column_id = 4;
  metadata->current_schema_id = 0;
  metadata->schemas.push_back(CreateTestSchema());
  metadata->partition_specs.push_back(spec);
  metadata->default_spec_id = spec->spec_id();
  metadata->last_partition_id = spec->last_assigned_field_id();
  metadata->current_snapshot_id = Snapshot::kInvalidSnapshotId;
  metadata->default_sort_order_id = SortOrder::kInitialSortOrderId;
  metadata->sort_orders.push_back(SortOrder::Unsorted());
  metadata->next_row_id = TableMetadata::kInitialRowId;
  return metadata;
}

// Helper to create UpdatePartitionSpec
std::unique_ptr<UpdatePartitionSpec> CreateUpdatePartitionSpec(
    int8_t format_version, std::shared_ptr<PartitionSpec> base_spec) {
  auto catalog = std::make_shared<MockCatalog>();
  auto metadata = CreateBaseMetadata(format_version, base_spec);
  TableIdentifier identifier{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  return std::make_unique<UpdatePartitionSpec>(std::move(identifier), catalog, metadata);
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

}  // namespace

class UpdatePartitionSpecTest : public ::testing::TestWithParam<int8_t> {
 protected:
  void SetUp() override {
    schema_ = CreateTestSchema();
    unpartitioned_ = PartitionSpec::Unpartitioned();
    partitioned_ = CreatePartitionedSpec();
    format_version_ = GetParam();
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> unpartitioned_;
  std::shared_ptr<PartitionSpec> partitioned_;
  int8_t format_version_;
};

INSTANTIATE_TEST_SUITE_P(FormatVersions, UpdatePartitionSpecTest, ::testing::Values(1, 2),
                         [](const ::testing::TestParamInfo<int8_t>& info) {
                           return std::format("V{}", info.param);
                         });

TEST_P(UpdatePartitionSpecTest, TestAddIdentityByName) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField("category");
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{PartitionField(
                              3, 1000, "category", Transform::Identity())}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddIdentityByTerm) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  auto ref = Expressions::Ref("category");
  update->AddField(ref);
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{PartitionField(
                              3, 1000, "category", Transform::Identity())}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddYear) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField(Expressions::Year("ts"));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{
                              PartitionField(2, 1000, "ts_year", Transform::Year())}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddMonth) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField(Expressions::Month("ts"));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{
                              PartitionField(2, 1000, "ts_month", Transform::Month())}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddDay) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField(Expressions::Day("ts"));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(auto expected_spec,
                         PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                                             std::vector<PartitionField>{PartitionField(
                                                 2, 1000, "ts_day", Transform::Day())}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddHour) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField(Expressions::Hour("ts"));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{
                              PartitionField(2, 1000, "ts_hour", Transform::Hour())}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddBucket) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField(Expressions::Bucket("id", 16));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{PartitionField(
                              1, 1000, "id_bucket_16", Transform::Bucket(16))}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddTruncate) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField(Expressions::Truncate("data", 4));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{PartitionField(
                              4, 1000, "data_trunc_4", Transform::Truncate(4))}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddNamedPartition) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField("shard", Expressions::Bucket("id", 16));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{
                              PartitionField(1, 1000, "shard", Transform::Bucket(16))}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddToExisting) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField(Expressions::Truncate("data", 4));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(
          PartitionSpec::kInitialSpecId,
          std::vector<PartitionField>{
              PartitionField(3, 1000, "category", Transform::Identity()),
              PartitionField(2, 1001, "ts_day", Transform::Day()),
              PartitionField(1, 1002, "shard", Transform::Bucket(16)),
              PartitionField(4, 1003, "data_trunc_4", Transform::Truncate(4))}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestMultipleAdds) {
  auto update = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update->AddField("category")
      .AddField(Expressions::Day("ts"))
      .AddField("shard", Expressions::Bucket("id", 16))
      .AddField("prefix", Expressions::Truncate("data", 4));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(
          PartitionSpec::kInitialSpecId,
          std::vector<PartitionField>{
              PartitionField(3, 1000, "category", Transform::Identity()),
              PartitionField(2, 1001, "ts_day", Transform::Day()),
              PartitionField(1, 1002, "shard", Transform::Bucket(16)),
              PartitionField(4, 1003, "prefix", Transform::Truncate(4))}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestAddHourToDay) {
  // First add day partition
  auto update1 = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update1->AddField(Expressions::Day("ts"));
  ASSERT_THAT(update1->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto by_day_spec, update1->GetAppliedSpec());

  // Then add hour partition
  auto metadata = CreateBaseMetadata(format_version_, by_day_spec);
  auto catalog = std::make_shared<MockCatalog>();
  TableIdentifier identifier{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  auto update2 = std::make_unique<UpdatePartitionSpec>(identifier, catalog, metadata);
  update2->AddField(Expressions::Hour("ts"));
  ASSERT_THAT(update2->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto by_hour_spec, update2->GetAppliedSpec());

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
  auto update1 = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update1->AddField(Expressions::Bucket("id", 16));
  ASSERT_THAT(update1->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto bucket16_spec, update1->GetAppliedSpec());

  // Then add bucket 8
  auto metadata = CreateBaseMetadata(format_version_, bucket16_spec);
  auto catalog = std::make_shared<MockCatalog>();
  TableIdentifier identifier{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  auto update2 = std::make_unique<UpdatePartitionSpec>(identifier, catalog, metadata);
  update2->AddField(Expressions::Bucket("id", 8));
  ASSERT_THAT(update2->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto bucket8_spec, update2->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(
          PartitionSpec::kInitialSpecId,
          std::vector<PartitionField>{
              PartitionField(1, 1000, "id_bucket_16", Transform::Bucket(16)),
              PartitionField(1, 1001, "id_bucket_8", Transform::Bucket(8))}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *bucket8_spec);
}

TEST_P(UpdatePartitionSpecTest, TestRemoveIdentityByName) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField("category");
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  if (format_version_ == 1) {
    // V1: deleted fields are replaced with void transform
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Void()),
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Bucket(16))}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    // V2: deleted fields are removed
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Bucket(16))}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveBucketByName) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField("shard");
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  if (format_version_ == 1) {
    // V1: deleted fields are replaced with void transform
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Void())}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    // V2: deleted fields are removed
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day())}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveIdentityByEquivalent) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  auto ref = Expressions::Ref("category");
  update->RemoveField(ref);
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  if (format_version_ == 1) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Void()),
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Bucket(16))}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Bucket(16))}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveDayByEquivalent) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField(Expressions::Day("ts"));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  if (format_version_ == 1) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Void()),
                PartitionField(1, 1002, "shard", Transform::Bucket(16))}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(1, 1002, "shard", Transform::Bucket(16))}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveBucketByEquivalent) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField(Expressions::Bucket("id", 16));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  if (format_version_ == 1) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Void())}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day())}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRename) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RenameField("shard", "id_bucket");
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(
          PartitionSpec::kInitialSpecId,
          std::vector<PartitionField>{
              PartitionField(3, 1000, "category", Transform::Identity()),
              PartitionField(2, 1001, "ts_day", Transform::Day()),
              PartitionField(1, 1002, "id_bucket", Transform::Bucket(16))}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());

  AssertPartitionSpecEquals(*expected, *updated_spec);
}

TEST_P(UpdatePartitionSpecTest, TestMultipleChanges) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RenameField("shard", "id_bucket")
      .RemoveField(Expressions::Day("ts"))
      .AddField("prefix", Expressions::Truncate("data", 4));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  if (format_version_ == 1) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Void()),
                PartitionField(1, 1002, "id_bucket", Transform::Bucket(16)),
                PartitionField(4, 1003, "prefix", Transform::Truncate(4))}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(1, 1002, "id_bucket", Transform::Bucket(16)),
                PartitionField(4, 1003, "prefix", Transform::Truncate(4))}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestAddDeletedName) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField(Expressions::Bucket("id", 16));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  if (format_version_ == 1) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day()),
                PartitionField(1, 1002, "shard", Transform::Void())}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  } else {
    ICEBERG_UNWRAP_OR_FAIL(
        auto expected_spec,
        PartitionSpec::Make(
            PartitionSpec::kInitialSpecId,
            std::vector<PartitionField>{
                PartitionField(3, 1000, "category", Transform::Identity()),
                PartitionField(2, 1001, "ts_day", Transform::Day())}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveNewlyAddedFieldByName) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField("prefix", Expressions::Truncate("data", 4));
  update->RemoveField("prefix");
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot delete newly added field"));
}

TEST_P(UpdatePartitionSpecTest, TestRemoveNewlyAddedFieldByTransform) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField("prefix", Expressions::Truncate("data", 4));
  update->RemoveField(Expressions::Truncate("data", 4));
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot delete newly added field"));
}

TEST_P(UpdatePartitionSpecTest, TestAddAlreadyAddedFieldByTransform) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField("prefix", Expressions::Truncate("data", 4));
  update->AddField(Expressions::Truncate("data", 4));
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot add duplicate partition field"));
}

TEST_P(UpdatePartitionSpecTest, TestAddAlreadyAddedFieldByName) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField("prefix", Expressions::Truncate("data", 4));
  update->AddField("prefix", Expressions::Truncate("data", 6));
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot add duplicate partition field"));
}

TEST_P(UpdatePartitionSpecTest, TestAddRedundantTimePartition) {
  // Test day + hour conflict
  auto update1 = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update1->AddField(Expressions::Day("ts"));
  update1->AddField(Expressions::Hour("ts"));
  EXPECT_THAT(update1->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update1->Apply(), HasErrorMessage("Cannot add redundant partition field"));

  // Test hour + month conflict after adding hour to existing day
  auto update2 = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update2->AddField(Expressions::Hour("ts"));   // day already exists, so hour is OK
  update2->AddField(Expressions::Month("ts"));  // conflicts with hour
  EXPECT_THAT(update2->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update2->Apply(), HasErrorMessage("Cannot add redundant partition"));
}

TEST_P(UpdatePartitionSpecTest, TestNoEffectAddDeletedSameFieldWithSameName) {
  auto update1 = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update1->RemoveField("shard");
  update1->AddField("shard", Expressions::Bucket("id", 16));
  ASSERT_THAT(update1->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto spec1, update1->GetAppliedSpec());
  AssertPartitionSpecEquals(*partitioned_, *spec1);

  auto update2 = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update2->RemoveField("shard");
  update2->AddField(Expressions::Bucket("id", 16));
  ASSERT_THAT(update2->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto spec2, update2->GetAppliedSpec());
  AssertPartitionSpecEquals(*partitioned_, *spec2);
}

TEST_P(UpdatePartitionSpecTest, TestGenerateNewSpecAddDeletedSameFieldWithDifferentName) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField("shard");
  update->AddField("new_shard", Expressions::Bucket("id", 16));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

  ASSERT_EQ(updated_spec->fields().size(), 3);
  EXPECT_EQ(updated_spec->fields()[0].name(), "category");
  EXPECT_EQ(updated_spec->fields()[1].name(), "ts_day");
  EXPECT_EQ(updated_spec->fields()[2].name(), "new_shard");
  EXPECT_EQ(*updated_spec->fields()[0].transform(), *Transform::Identity());
  EXPECT_EQ(*updated_spec->fields()[1].transform(), *Transform::Day());
  EXPECT_EQ(*updated_spec->fields()[2].transform(), *Transform::Bucket(16));
}

TEST_P(UpdatePartitionSpecTest, TestAddDuplicateByName) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField("category");
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot add duplicate partition field"));
}

TEST_P(UpdatePartitionSpecTest, TestAddDuplicateByRef) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  auto ref = Expressions::Ref("category");
  update->AddField(ref);
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot add duplicate partition field"));
}

TEST_P(UpdatePartitionSpecTest, TestAddDuplicateTransform) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField(Expressions::Bucket("id", 16));
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot add duplicate partition field"));
}

TEST_P(UpdatePartitionSpecTest, TestAddNamedDuplicate) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField("b16", Expressions::Bucket("id", 16));
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot add duplicate partition field"));
}

TEST_P(UpdatePartitionSpecTest, TestRemoveUnknownFieldByName) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField("moon");
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot find partition field to remove"));
}

TEST_P(UpdatePartitionSpecTest, TestRemoveUnknownFieldByEquivalent) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField(Expressions::Hour("ts"));  // day(ts) exists, not hour
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(), HasErrorMessage("Cannot find partition field to remove"));
}

TEST_P(UpdatePartitionSpecTest, TestRenameUnknownField) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RenameField("shake", "seal");
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(),
              HasErrorMessage("Cannot find partition field to rename: shake"));
}

TEST_P(UpdatePartitionSpecTest, TestRenameAfterAdd) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->AddField("data_trunc", Expressions::Truncate("data", 4));
  update->RenameField("data_trunc", "prefix");
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(),
              HasErrorMessage("Cannot rename newly added partition field: data_trunc"));
}

TEST_P(UpdatePartitionSpecTest, TestRenameAndDelete) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RenameField("shard", "id_bucket");
  update->RemoveField(Expressions::Bucket("id", 16));
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(),
              HasErrorMessage("Cannot rename and delete partition field: shard"));
}

TEST_P(UpdatePartitionSpecTest, TestDeleteAndRename) {
  auto update = CreateUpdatePartitionSpec(format_version_, partitioned_);
  update->RemoveField(Expressions::Bucket("id", 16));
  update->RenameField("shard", "id_bucket");
  EXPECT_THAT(update->Apply(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(update->Apply(),
              HasErrorMessage("Cannot delete and rename partition field: shard"));
}

TEST_P(UpdatePartitionSpecTest, TestRemoveAndAddMultiTimes) {
  // Add first time
  auto update1 = CreateUpdatePartitionSpec(format_version_, unpartitioned_);
  update1->AddField("ts_date", Expressions::Day("ts"));
  ASSERT_THAT(update1->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto add_first_time_spec, update1->GetAppliedSpec());

  // Remove first time
  auto metadata1 = CreateBaseMetadata(format_version_, add_first_time_spec);
  auto catalog1 = std::make_shared<MockCatalog>();
  TableIdentifier identifier1{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  auto update2 = std::make_unique<UpdatePartitionSpec>(identifier1, catalog1, metadata1);
  update2->RemoveField(Expressions::Day("ts"));
  ASSERT_THAT(update2->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto remove_first_time_spec, update2->GetAppliedSpec());

  // Add second time
  auto metadata2 = CreateBaseMetadata(format_version_, remove_first_time_spec);
  auto catalog2 = std::make_shared<MockCatalog>();
  TableIdentifier identifier2{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  auto update3 = std::make_unique<UpdatePartitionSpec>(identifier2, catalog2, metadata2);
  update3->AddField("ts_date", Expressions::Day("ts"));
  ASSERT_THAT(update3->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto add_second_time_spec, update3->GetAppliedSpec());

  // Remove second time
  auto metadata3 = CreateBaseMetadata(format_version_, add_second_time_spec);
  auto catalog3 = std::make_shared<MockCatalog>();
  TableIdentifier identifier3{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  auto update4 = std::make_unique<UpdatePartitionSpec>(identifier3, catalog3, metadata3);
  update4->RemoveField(Expressions::Day("ts"));
  ASSERT_THAT(update4->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto remove_second_time_spec, update4->GetAppliedSpec());

  // Add third time with month
  auto metadata4 = CreateBaseMetadata(format_version_, remove_second_time_spec);
  auto catalog4 = std::make_shared<MockCatalog>();
  TableIdentifier identifier4{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  auto update5 = std::make_unique<UpdatePartitionSpec>(identifier4, catalog4, metadata4);
  update5->AddField(Expressions::Month("ts"));
  ASSERT_THAT(update5->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto add_third_time_spec, update5->GetAppliedSpec());

  // Rename ts_month to ts_date
  auto metadata5 = CreateBaseMetadata(format_version_, add_third_time_spec);
  auto catalog5 = std::make_shared<MockCatalog>();
  TableIdentifier identifier5{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  auto update6 = std::make_unique<UpdatePartitionSpec>(identifier5, catalog5, metadata5);
  update6->RenameField("ts_month", "ts_date");
  ASSERT_THAT(update6->Apply(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update6->GetAppliedSpec());

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
                                PartitionField(2, 1000, "ts_date", Transform::Month())}));
    auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
    AssertPartitionSpecEquals(*expected, *updated_spec);
  }
}

TEST_P(UpdatePartitionSpecTest, TestRemoveAndUpdateWithDifferentTransformation) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          std::vector<PartitionField>{PartitionField(
                              2, 1000, "ts_transformed", Transform::Month())}));
  auto expected = std::shared_ptr<PartitionSpec>(expected_spec.release());
  auto metadata = CreateBaseMetadata(format_version_, expected);
  auto catalog = std::make_shared<MockCatalog>();
  TableIdentifier identifier{.ns = Namespace{.levels = {"test"}}, .name = "test_table"};
  auto update = std::make_unique<UpdatePartitionSpec>(identifier, catalog, metadata);
  update->RemoveField("ts_transformed");
  update->AddField("ts_transformed", Expressions::Day("ts"));
  ASSERT_THAT(update->Apply(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_spec, update->GetAppliedSpec());

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

}  // namespace iceberg
