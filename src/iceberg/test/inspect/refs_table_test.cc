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

#include "iceberg/inspect/refs_table.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "iceberg/constants.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/inspect/metadata_table_test_base.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

class RefsTableTest : public MetadataTableTestBase,
                      public ::testing::WithParamInterface<int8_t> {
 protected:
  void SetUp() override {
    MetadataTableTestBase::SetUp();
    metadata_->format_version = GetParam();
  }
};

TEST_P(RefsTableTest, ScansBranchesAndTagsAsRefs) {
  ICEBERG_UNWRAP_OR_FAIL(auto main_ref, SnapshotRef::MakeBranch(2));
  ICEBERG_UNWRAP_OR_FAIL(auto dev_ref, SnapshotRef::MakeBranch(1, 3, 2000, 1000));
  ICEBERG_UNWRAP_OR_FAIL(auto release_ref, SnapshotRef::MakeTag(1, 5000));
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  refs.emplace("main", std::move(main_ref));
  refs.emplace("dev", std::move(dev_ref));
  refs.emplace("release", std::move(release_ref));

  auto [first, second] = MakeTestSnapshots();
  metadata_->snapshots = {first, second};
  metadata_->current_snapshot_id = 2;
  metadata_->refs = std::move(refs);
  ICEBERG_UNWRAP_OR_FAIL(auto refs_table, MetadataTable::Make<RefsTable>(table_));
  EXPECT_EQ(refs_table->kind(), MetadataTable::Kind::kRefs);
  EXPECT_FALSE(refs_table->supports_time_travel());
  Schema expected_schema({
      SchemaField::MakeRequired(1, "name", string()),
      SchemaField::MakeRequired(2, "type", string()),
      SchemaField::MakeRequired(3, "snapshot_id", int64()),
      SchemaField::MakeOptional(4, "max_reference_age_in_ms", int64()),
      SchemaField::MakeOptional(5, "min_snapshots_to_keep", int32()),
      SchemaField::MakeOptional(6, "max_snapshot_age_in_ms", int64()),
  });
  EXPECT_EQ(*refs_table->schema(), expected_schema);
  ICEBERG_UNWRAP_OR_FAIL(auto stream, refs_table->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), 3);
  ASSERT_EQ(batches[0]->num_columns(), 6);
  ASSERT_EQ(batches[0]->column(0)->type_id(), ::arrow::Type::STRING);
  ASSERT_EQ(batches[0]->column(1)->type_id(), ::arrow::Type::STRING);
  ASSERT_EQ(batches[0]->column(2)->type_id(), ::arrow::Type::INT64);
  ASSERT_EQ(batches[0]->column(3)->type_id(), ::arrow::Type::INT64);
  ASSERT_EQ(batches[0]->column(4)->type_id(), ::arrow::Type::INT32);
  ASSERT_EQ(batches[0]->column(5)->type_id(), ::arrow::Type::INT64);
  auto names = std::static_pointer_cast<::arrow::StringArray>(batches[0]->column(0));
  auto types = std::static_pointer_cast<::arrow::StringArray>(batches[0]->column(1));
  auto ids = std::static_pointer_cast<::arrow::Int64Array>(batches[0]->column(2));
  auto ref_age = std::static_pointer_cast<::arrow::Int64Array>(batches[0]->column(3));
  auto min_snapshots =
      std::static_pointer_cast<::arrow::Int32Array>(batches[0]->column(4));
  auto snapshot_age =
      std::static_pointer_cast<::arrow::Int64Array>(batches[0]->column(5));
  EXPECT_EQ(names->GetString(0), "dev");
  EXPECT_EQ(types->GetString(0), "BRANCH");
  EXPECT_EQ(ids->Value(0), 1);
  EXPECT_EQ(ref_age->Value(0), 1000);
  EXPECT_EQ(min_snapshots->Value(0), 3);
  EXPECT_EQ(snapshot_age->Value(0), 2000);
  EXPECT_EQ(names->GetString(1), "main");
  EXPECT_EQ(types->GetString(1), "BRANCH");
  EXPECT_EQ(ids->Value(1), 2);
  EXPECT_TRUE(ref_age->IsNull(1));
  EXPECT_TRUE(min_snapshots->IsNull(1));
  EXPECT_TRUE(snapshot_age->IsNull(1));
  EXPECT_EQ(names->GetString(2), "release");
  EXPECT_EQ(types->GetString(2), "TAG");
  EXPECT_EQ(ids->Value(2), 1);
  EXPECT_EQ(ref_age->Value(2), 5000);
  EXPECT_TRUE(min_snapshots->IsNull(2));
  EXPECT_TRUE(snapshot_age->IsNull(2));
}

TEST_P(RefsTableTest, ScanEmpty) {
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<RefsTable>(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, metadata_table->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

TEST_P(RefsTableTest, ScansRefsAcrossBatchBoundary) {
  for (int64_t i = 0; i < MetadataTable::kBatchSize + 1; ++i) {
    ICEBERG_UNWRAP_OR_FAIL(auto ref, SnapshotRef::MakeTag(i + 1));
    metadata_->refs.emplace("tag-" + std::to_string(i), std::move(ref));
  }
  ICEBERG_UNWRAP_OR_FAIL(auto refs_table, RefsTable::Make(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, refs_table->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 2);
  EXPECT_EQ(batches[0]->num_rows(), MetadataTable::kBatchSize);
  EXPECT_EQ(batches[1]->num_rows(), 1);
  std::string previous;
  for (const auto& batch : batches) {
    ASSERT_EQ(batch->num_columns(), 6);
    ASSERT_EQ(batch->column(0)->type_id(), ::arrow::Type::STRING);
    auto names = std::static_pointer_cast<::arrow::StringArray>(batch->column(0));
    for (int64_t i = 0; i < batch->num_rows(); ++i) {
      auto name = names->GetString(i);
      EXPECT_LT(previous, name);
      previous = std::move(name);
    }
  }
}

TEST(RefsTableConstructionTest, RejectsNullTable) {
  EXPECT_THAT(RefsTable::Make(nullptr), IsError(ErrorKind::kInvalidArgument));
}

TEST_P(RefsTableTest, RejectsNullReferenceMetadata) {
  metadata_->refs.emplace("broken", nullptr);
  ICEBERG_UNWRAP_OR_FAIL(auto refs_table, RefsTable::Make(table_));
  EXPECT_THAT(refs_table->Scan(),
              ::testing::AllOf(IsError(ErrorKind::kInvalid),
                               HasErrorMessage("Snapshot reference 'broken' is null")));
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, RefsTableTest, ::testing::Values(1, 2, 3));

}  // namespace
}  // namespace iceberg
