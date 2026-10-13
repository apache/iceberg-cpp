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

#include "iceberg/inspect/manifests_table.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "iceberg/constants.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/inspect/metadata_table_scan_test_base.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

class ManifestsTableTest : public MetadataTableScanTestBase {};

TEST_P(ManifestsTableTest, Scan) {
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"s3://bucket/data.parquet", PartitionValues(Literal::Int(7))}},
      partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests_stream, manifests->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests_batches,
                         ReadAllBatches(std::move(manifests_stream)));
  ASSERT_EQ(manifests_batches.size(), 1);
  ASSERT_EQ(manifests_batches[0]->num_rows(), 1);
  auto manifest_paths = std::static_pointer_cast<::arrow::StringArray>(
      manifests_batches[0]->GetColumnByName("path"));
  auto added_files = std::static_pointer_cast<::arrow::Int32Array>(
      manifests_batches[0]->GetColumnByName("added_data_files_count"));
  EXPECT_FALSE(manifest_paths->GetString(0).empty());
  EXPECT_EQ(added_files->Value(0), 1);
}

TEST_P(ManifestsTableTest, RejectsCombiningNamedRefsWithTimeTravel) {
  auto first = MakeAppendSnapshot(GetParam(), 1, std::nullopt, 1, {"first.parquet"});
  auto second = MakeAppendSnapshot(GetParam(), 2, 1, 2, {"second.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto branch, SnapshotRef::MakeBranch(1));
  ICEBERG_UNWRAP_OR_FAIL(auto tag, SnapshotRef::MakeTag(1));
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  refs.emplace("dev", std::move(branch));
  refs.emplace("release", std::move(tag));
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second}, 2, std::move(refs)));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<ManifestsTable>(table));
  for (const std::string ref_name : {"dev", "release"}) {
    for (const SnapshotSelection selection :
         {SnapshotSelection{.snapshot = int64_t{1}, .ref_name = ref_name},
          SnapshotSelection{.snapshot = first->timestamp_ms, .ref_name = ref_name}}) {
      auto result = metadata_table->Scan(selection);
      ASSERT_FALSE(result.has_value());
      EXPECT_EQ(result.error().kind, ErrorKind::kInvalidArgument);
    }
  }
}

TEST_P(ManifestsTableTest, RejectsUnknownReference) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<ManifestsTable>(table));
  EXPECT_THAT(metadata_table->Scan(SnapshotSelection{.ref_name = "missing"}),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_P(ManifestsTableTest, PreservesUnknownManifestCounts) {
  // Upgraded tables can still reference v1 manifest lists, where counts are optional.
  auto unknown = WriteDataManifest(
      1, 10, {MakeEntry(ManifestStatus::kAdded, 10, 0, MakeDataFile("data.parquet"))});
  unknown.added_files_count.reset();
  unknown.existing_files_count.reset();
  unknown.deleted_files_count.reset();
  auto empty = WriteDataManifest(1, 10, {});
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 0,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list = WriteManifestList(1, 10, 0, 0, {unknown, empty}),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, manifests->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), 2);
  for (int index = 5; index <= 10; ++index) {
    auto counts =
        std::static_pointer_cast<::arrow::Int32Array>(batches[0]->column(index));
    if (index < 8) {
      EXPECT_TRUE(counts->IsNull(0));
    } else {
      // Delete counts are definitively zero in a data manifest.
      EXPECT_FALSE(counts->IsNull(0));
      EXPECT_EQ(counts->Value(0), 0);
    }
    // A known zero must remain distinguishable from an unknown count.
    EXPECT_FALSE(counts->IsNull(1));
    EXPECT_EQ(counts->Value(1), 0);
    EXPECT_TRUE(batches[0]->schema()->field(index)->nullable());
    EXPECT_TRUE(manifests->schema()->fields()[index].optional());
  }
}

TEST_P(ManifestsTableTest, ManifestNullBoundsUseJavaRendering) {
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"null.parquet", PartitionValues(Literal::Null(int32()))}}, partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, manifests->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto summaries = std::static_pointer_cast<::arrow::ListArray>(
      batches[0]->GetColumnByName("partition_summaries"));
  auto values = std::static_pointer_cast<::arrow::StructArray>(summaries->values());
  ASSERT_EQ(values->length(), 1);
  for (int index : {2, 3}) {
    auto bound = std::static_pointer_cast<::arrow::StringArray>(values->field(index));
    ASSERT_FALSE(bound->IsNull(0));
    EXPECT_EQ(bound->GetString(0), "null");
  }
}

TEST_P(ManifestsTableTest, ManifestBoundsSurviveDroppedBucketSource) {
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"data.parquet", PartitionValues(Literal::Int(7))}}, partitioned_spec_);
  auto metadata = MakeTableMetadata({snapshot}, 10);
  metadata->schemas.push_back(std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())}, 1));
  metadata->current_schema_id = 1;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, Table::Make(TableIdentifier{.name = "table"}, std::move(metadata),
                              "s3://bucket/metadata.json", file_io_, catalog_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, manifests->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto summaries = std::static_pointer_cast<::arrow::ListArray>(
      batches[0]->GetColumnByName("partition_summaries"));
  auto values = std::static_pointer_cast<::arrow::StructArray>(summaries->values());
  ASSERT_EQ(values->length(), 1);
  for (int index : {2, 3}) {
    auto bound = std::static_pointer_cast<::arrow::StringArray>(values->field(index));
    ASSERT_FALSE(bound->IsNull(0));
    EXPECT_EQ(bound->GetString(0), "7");
  }
}

TEST_P(ManifestsTableTest, ScanEmpty) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, metadata_table->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, ManifestsTableTest, ::testing::Values(2, 3));

}  // namespace
}  // namespace iceberg
