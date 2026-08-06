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

#include <chrono>
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
#include "iceberg/inspect/branches_table.h"
#include "iceberg/inspect/files_table.h"
#include "iceberg/inspect/manifests_table.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/partitions_table.h"
#include "iceberg/inspect/tags_table.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

class SystemMetadataTablesTest : public ScanTestBase {
 protected:
  void SetUp() override {
    ScanTestBase::SetUp();
    catalog_ = std::make_shared<MockCatalog>();
  }

  Result<std::shared_ptr<Table>> MakeTable(
      std::vector<std::shared_ptr<Snapshot>> snapshots, int64_t current_snapshot_id,
      std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs = {},
      std::shared_ptr<PartitionSpec> spec = nullptr) {
    auto metadata = MakeTableMetadata(snapshots, current_snapshot_id, refs, spec);
    return Table::Make(
        TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = "table"},
        std::move(metadata), "s3://bucket/metadata.json", file_io_, catalog_);
  }

  static Result<std::vector<std::shared_ptr<::arrow::RecordBatch>>> ReadAllBatches(
      ArrowArrayStream&& stream) {
    auto reader = ::arrow::ImportRecordBatchReader(&stream);
    if (!reader.ok()) {
      return InvalidArrowData(reader.status().ToString());
    }
    auto batches = reader.ValueUnsafe()->ToRecordBatches();
    if (!batches.ok()) {
      return InvalidArrowData(batches.status().ToString());
    }
    return std::move(batches).MoveValueUnsafe();
  }

  std::shared_ptr<MockCatalog> catalog_;
};

TEST_P(SystemMetadataTablesTest, ScansBranchesAndTagsSeparately) {
  ICEBERG_UNWRAP_OR_FAIL(auto main_ref, SnapshotRef::MakeBranch(2));
  ICEBERG_UNWRAP_OR_FAIL(auto dev_ref, SnapshotRef::MakeBranch(1, 3, 2000, 1000));
  ICEBERG_UNWRAP_OR_FAIL(auto release_ref, SnapshotRef::MakeTag(1, 5000));
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  refs.emplace("main", std::move(main_ref));
  refs.emplace("dev", std::move(dev_ref));
  refs.emplace("release", std::move(release_ref));

  auto first = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 1,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list = "unused-1.avro",
  });
  auto second = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 2,
      .parent_snapshot_id = 1,
      .sequence_number = 2,
      .timestamp_ms = TimePointMsFromUnixMs(2000),
      .manifest_list = "unused-2.avro",
  });
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second}, 2, std::move(refs)));

  ICEBERG_UNWRAP_OR_FAIL(auto branches, MetadataTable::Make<BranchesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto branch_stream, branches->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto branch_batches, ReadAllBatches(std::move(branch_stream)));
  ASSERT_EQ(branch_batches.size(), 1);
  ASSERT_EQ(branch_batches[0]->num_rows(), 2);
  auto branch_names = std::static_pointer_cast<::arrow::StringArray>(
      branch_batches[0]->GetColumnByName("name"));
  auto branch_ids = std::static_pointer_cast<::arrow::Int64Array>(
      branch_batches[0]->GetColumnByName("snapshot_id"));
  EXPECT_EQ(branch_names->GetString(0), "dev");
  EXPECT_EQ(branch_ids->Value(0), 1);
  EXPECT_EQ(branch_names->GetString(1), "main");
  EXPECT_EQ(branch_ids->Value(1), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto tags, MetadataTable::Make<TagsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto tag_stream, tags->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto tag_batches, ReadAllBatches(std::move(tag_stream)));
  ASSERT_EQ(tag_batches.size(), 1);
  ASSERT_EQ(tag_batches[0]->num_rows(), 1);
  auto tag_names = std::static_pointer_cast<::arrow::StringArray>(
      tag_batches[0]->GetColumnByName("name"));
  auto max_ref_age = std::static_pointer_cast<::arrow::Int64Array>(
      tag_batches[0]->GetColumnByName("max_reference_age_in_ms"));
  EXPECT_EQ(tag_names->GetString(0), "release");
  EXPECT_EQ(max_ref_age->Value(0), 5000);
}

TEST_P(SystemMetadataTablesTest, ScansFilesManifestsAndPartitions) {
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"s3://bucket/data.parquet", PartitionValues(Literal::Int(7))}},
      partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));

  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto files_stream, files->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto files_batches, ReadAllBatches(std::move(files_stream)));
  ASSERT_EQ(files_batches.size(), 1);
  ASSERT_EQ(files_batches[0]->num_rows(), 1);
  auto paths = std::static_pointer_cast<::arrow::StringArray>(
      files_batches[0]->GetColumnByName("file_path"));
  auto formats = std::static_pointer_cast<::arrow::StringArray>(
      files_batches[0]->GetColumnByName("file_format"));
  auto spec_ids = std::static_pointer_cast<::arrow::Int32Array>(
      files_batches[0]->GetColumnByName("spec_id"));
  auto file_partitions = std::static_pointer_cast<::arrow::StructArray>(
      files_batches[0]->GetColumnByName("partition"));
  auto partition_values =
      std::static_pointer_cast<::arrow::Int32Array>(file_partitions->field(0));
  EXPECT_EQ(paths->GetString(0), "s3://bucket/data.parquet");
  EXPECT_EQ(formats->GetString(0), "PARQUET");
  EXPECT_EQ(spec_ids->Value(0), partitioned_spec_->spec_id());
  EXPECT_EQ(partition_values->Value(0), 7);

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

  ICEBERG_UNWRAP_OR_FAIL(auto partitions, MetadataTable::Make<PartitionsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto partitions_stream, partitions->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto partition_batches,
                         ReadAllBatches(std::move(partitions_stream)));
  ASSERT_EQ(partition_batches.size(), 1);
  ASSERT_EQ(partition_batches[0]->num_rows(), 1);
  auto records = std::static_pointer_cast<::arrow::Int64Array>(
      partition_batches[0]->GetColumnByName("record_count"));
  auto file_counts = std::static_pointer_cast<::arrow::Int32Array>(
      partition_batches[0]->GetColumnByName("file_count"));
  auto updated_snapshot_ids = std::static_pointer_cast<::arrow::Int64Array>(
      partition_batches[0]->GetColumnByName("last_updated_snapshot_id"));
  EXPECT_EQ(records->Value(0), 1);
  EXPECT_EQ(file_counts->Value(0), 1);
  EXPECT_EQ(updated_snapshot_ids->Value(0), 10);
}

TEST_P(SystemMetadataTablesTest, SupportsTimeTravelForSnapshotScopedTables) {
  auto first =
      MakeAppendSnapshot(GetParam(), 1, std::nullopt, 1, {"s3://bucket/first.parquet"});
  auto second = MakeAppendSnapshot(GetParam(), 2, 1, 2, {"s3://bucket/second.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second}, 2));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         files->Scan(SnapshotSelection{.snapshot = int64_t{1}}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto paths = std::static_pointer_cast<::arrow::StringArray>(
      batches[0]->GetColumnByName("file_path"));
  ASSERT_EQ(paths->length(), 1);
  EXPECT_EQ(paths->GetString(0), "s3://bucket/first.parquet");
}

TEST_P(SystemMetadataTablesTest, StopsTimestampTraversalAtExpiredParent) {
  auto snapshot =
      MakeAppendSnapshot(GetParam(), 2, 1, 2, {"s3://bucket/current.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto dev_ref, SnapshotRef::MakeBranch(2));
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  refs.emplace("dev", std::move(dev_ref));
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 2, std::move(refs)));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         files->Scan(SnapshotSelection{.snapshot = snapshot->timestamp_ms,
                                                       .ref_name = "dev"}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), 1);
}

TEST_P(SystemMetadataTablesTest, MainTimestampSelectionUsesSnapshotLogAfterRollback) {
  auto first =
      MakeAppendSnapshot(GetParam(), 1, std::nullopt, 1, {"s3://bucket/first.parquet"});
  auto second = MakeAppendSnapshot(GetParam(), 2, 1, 2, {"s3://bucket/second.parquet"});
  auto third = MakeAppendSnapshot(GetParam(), 3, 2, 3, {"s3://bucket/third.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second, third}, 1));
  table->metadata()->snapshot_log = {
      SnapshotLogEntry{.timestamp_ms = first->timestamp_ms, .snapshot_id = 1},
      SnapshotLogEntry{.timestamp_ms = second->timestamp_ms, .snapshot_id = 2},
      SnapshotLogEntry{.timestamp_ms = third->timestamp_ms, .snapshot_id = 3},
      SnapshotLogEntry{.timestamp_ms = third->timestamp_ms + std::chrono::milliseconds(1),
                       .snapshot_id = 1},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         files->Scan(SnapshotSelection{.snapshot = third->timestamp_ms}));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto paths = std::static_pointer_cast<::arrow::StringArray>(
      batches[0]->GetColumnByName("file_path"));
  ASSERT_EQ(paths->length(), 1);
  EXPECT_EQ(paths->GetString(0), "s3://bucket/third.parquet");
}

TEST_P(SystemMetadataTablesTest, FilesSchemaIncludesReadableMetrics) {
  ICEBERG_UNWRAP_OR_FAIL(auto table,
                         MakeTable({}, kInvalidSnapshotId, {}, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto spec_id_field,
                         files->schema()->FindFieldById(DataFile::kSpecIdFieldId));
  ASSERT_TRUE(spec_id_field.has_value());
  EXPECT_TRUE(spec_id_field->get().optional());

  ICEBERG_UNWRAP_OR_FAIL(auto readable_metrics,
                         files->schema()->FindFieldByName("readable_metrics"));
  ASSERT_TRUE(readable_metrics.has_value());
  EXPECT_TRUE(readable_metrics->get().optional());
  auto metrics_type =
      std::static_pointer_cast<StructType>(readable_metrics->get().type());
  ASSERT_EQ(metrics_type->fields().size(), 2);
  EXPECT_EQ(metrics_type->fields()[0].name(), "data");
  EXPECT_EQ(metrics_type->fields()[1].name(), "id");
  for (const auto& field : metrics_type->fields()) {
    auto column_metrics = std::static_pointer_cast<StructType>(field.type());
    EXPECT_EQ(column_metrics->fields().size(), 6);
  }
}

TEST_P(SystemMetadataTablesTest, SupportsLegacyVoidPartitionEvolution) {
  ICEBERG_UNWRAP_OR_FAIL(auto older_spec,
                         PartitionSpec::Make(1, {PartitionField(2, 1000, "old_bucket",
                                                                Transform::Bucket(16))}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto latest_spec,
      PartitionSpec::Make(2, {PartitionField(2, 1000, "new_name", Transform::Void())}));
  auto metadata = MakeTableMetadata({}, kInvalidSnapshotId);
  metadata->partition_specs = {
      std::shared_ptr<PartitionSpec>(std::move(older_spec)),
      std::shared_ptr<PartitionSpec>(std::move(latest_spec)),
  };
  metadata->default_spec_id = 2;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      Table::Make(TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = "table"},
                  std::move(metadata), "s3://bucket/metadata.json", file_io_, catalog_));

  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto partition_field,
                         files->schema()->FindFieldById(DataFile::kPartitionFieldId));
  ASSERT_TRUE(partition_field.has_value());
  auto partition_type =
      std::static_pointer_cast<StructType>(partition_field->get().type());
  ASSERT_EQ(partition_type->fields().size(), 1);
  EXPECT_EQ(partition_type->fields()[0].name(), "new_name");
  EXPECT_EQ(partition_type->fields()[0].type()->type_id(), TypeId::kInt);
}

TEST_P(SystemMetadataTablesTest, GroupsNullPartitionValues) {
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"s3://bucket/first.parquet", PartitionValues(Literal::Null(int32()))},
       {"s3://bucket/second.parquet", PartitionValues(Literal::Null(int32()))}},
      partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto partitions, MetadataTable::Make<PartitionsTable>(table));

  ICEBERG_UNWRAP_OR_FAIL(auto stream, partitions->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), 1);
  auto file_counts = std::static_pointer_cast<::arrow::Int32Array>(
      batches[0]->GetColumnByName("file_count"));
  EXPECT_EQ(file_counts->Value(0), 2);
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, SystemMetadataTablesTest,
                         ::testing::Values(2, 3));

}  // namespace
}  // namespace iceberg
