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
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/constants.h"
#include "iceberg/inspect/metadata_scan.h"
#include "iceberg/inspect/metadata_scan_task_reader.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/logging/logger.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/logging_test_helpers.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {
namespace {

class ManifestsTableTest : public ScanTestBase {
 protected:
  Result<std::shared_ptr<Table>> MakeTable(
      const std::vector<std::shared_ptr<Snapshot>>& snapshots,
      int64_t current_snapshot_id, std::shared_ptr<PartitionSpec> spec = nullptr) {
    auto metadata =
        MakeTableMetadata(snapshots, current_snapshot_id, {}, std::move(spec));
    return Table::Make(
        TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = "table"},
        std::move(metadata), "s3://bucket/metadata.json", file_io_, catalog_);
  }

  static Result<ArrowArrayStream> Scan(const MetadataTable& table) {
    ICEBERG_ASSIGN_OR_RAISE(auto builder, table.NewScan());
    ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
    return scan->ToArrowArrayStream();
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

  Status RefreshTableSchemaAndPartitionSpec(const std::shared_ptr<Table>& table,
                                            const std::shared_ptr<Snapshot>& first) {
    auto old_schema = schema_;
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeRequired(2, "data", string()),
                                 SchemaField::MakeOptional(3, "extra", int64())},
        1);
    std::shared_ptr<PartitionSpec> new_spec;
    ICEBERG_ASSIGN_OR_RAISE(
        new_spec,
        PartitionSpec::Make(
            2, {PartitionField(2, 1000, "data_bucket_16_2", Transform::Bucket(16)),
                PartitionField(1, 1001, "id", Transform::Identity())}));
    auto second = MakeAppendSnapshotWithPartitionValues(
        GetParam(), 20, 10, 2,
        {{"one.parquet", PartitionValues({Literal::Int(7), Literal::Int(1)})},
         {"two.parquet", PartitionValues({Literal::Int(7), Literal::Int(2)})}},
        new_spec);
    auto metadata = MakeTableMetadata({first, second}, 20, {}, new_spec);
    metadata->schemas.insert(metadata->schemas.begin(), old_schema);
    metadata->partition_specs.push_back(new_spec);
    metadata->last_column_id = 3;
    metadata->last_partition_id = 1001;
    ICEBERG_ASSIGN_OR_RAISE(
        auto refreshed,
        Table::Make(table->name(), std::move(metadata), "s3://bucket/refreshed.json",
                    arrow::MakeMockFileIO(), catalog_));
    EXPECT_CALL(*catalog_, LoadTable(::testing::_))
        .WillOnce(::testing::Return(refreshed));
    return table->Refresh();
  }

  const std::shared_ptr<MockCatalog> catalog_ = std::make_shared<MockCatalog>();
};

TEST_P(ManifestsTableTest, Scan) {
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"s3://bucket/data.parquet", PartitionValues(Literal::Int(7))}},
      partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, partitioned_spec_));

  ICEBERG_UNWRAP_OR_FAIL(auto manifests, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests_stream, Scan(*manifests));
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

TEST_P(ManifestsTableTest, CountsMatchManifestContent) {
  auto data_manifest = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kAdded, 10, 1, MakeDataFile("data.parquet"))});
  auto delete_file = MakeDataFile("delete.parquet");
  delete_file->content = DataFile::Content::kEqualityDeletes;
  delete_file->equality_ids = {1};
  auto delete_manifest = WriteDeleteManifest(
      GetParam(), 10, {MakeEntry(ManifestStatus::kAdded, 10, 1, std::move(delete_file))},
      unpartitioned_spec_);
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list =
          WriteManifestList(GetParam(), 10, 0, 1, {data_manifest, delete_manifest}),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, ManifestsTable::Make(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*manifests));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  const auto& batch = batches.front();
  ASSERT_EQ(batch->num_rows(), 2);
  auto content = std::static_pointer_cast<::arrow::Int32Array>(batch->column(0));
  EXPECT_EQ(content->Value(0), static_cast<int32_t>(ManifestContent::kData));
  EXPECT_EQ(content->Value(1), static_cast<int32_t>(ManifestContent::kDeletes));
  for (int index = 5; index <= 10; ++index) {
    SCOPED_TRACE(batch->schema()->field(index)->name());
    auto counts = std::static_pointer_cast<::arrow::Int32Array>(batch->column(index));
    EXPECT_EQ(counts->null_count(), 0);
    EXPECT_EQ(counts->Value(0), index == 5 ? 1 : 0);
    EXPECT_EQ(counts->Value(1), index == 8 ? 1 : 0);
  }
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
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*manifests));
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
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*manifests));
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

TEST_P(ManifestsTableTest, FloatingPointBoundsRoundTrip) {
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "f32", float32()),
      SchemaField::MakeRequired(2, "f64", float64()),
  });
  ICEBERG_UNWRAP_OR_FAIL(
      partitioned_spec_,
      PartitionSpec::Make(1, {PartitionField(1, 1000, "f32", Transform::Identity()),
                              PartitionField(2, 1001, "f64", Transform::Identity())}));
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"low.parquet", PartitionValues({Literal::Float(1e-7F), Literal::Double(1e-7)})},
       {"high.parquet", PartitionValues({Literal::Float(2e-7F), Literal::Double(2e-7)})}},
      partitioned_spec_);
  auto metadata = MakeTableMetadata({snapshot}, 10, {}, partitioned_spec_);
  metadata->last_partition_id = 1001;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, Table::Make(TableIdentifier{.name = "table"}, std::move(metadata),
                              "s3://bucket/metadata.json", file_io_, catalog_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, ManifestsTable::Make(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*manifests));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto summaries = std::static_pointer_cast<::arrow::ListArray>(
      batches[0]->GetColumnByName("partition_summaries"));
  auto values = std::static_pointer_cast<::arrow::StructArray>(summaries->values());
  ASSERT_EQ(values->length(), 2);
  auto lower = std::static_pointer_cast<::arrow::StringArray>(values->field(2));
  auto upper = std::static_pointer_cast<::arrow::StringArray>(values->field(3));
  EXPECT_EQ(std::stof(lower->GetString(0)), 1e-7F);
  EXPECT_EQ(std::stof(upper->GetString(0)), 2e-7F);
  EXPECT_EQ(std::stod(lower->GetString(1)), 1e-7);
  EXPECT_EQ(std::stod(upper->GetString(1)), 2e-7);
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
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*manifests));
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

TEST_P(ManifestsTableTest, ManifestBoundsUsePromotedType) {
  std::shared_ptr<PartitionSpec> spec;
  ICEBERG_UNWRAP_OR_FAIL(
      spec, PartitionSpec::Make(
                1, {PartitionField(1, 1000, "id", Transform::Identity()),
                    PartitionField(2, 1001, "data_prefix", Transform::Truncate(3))}));
  auto write_manifest = [&](const std::string& path, Literal id) {
    return WriteDataManifest(
        GetParam(), 10,
        {MakeEntry(
            ManifestStatus::kAdded, 10, 1,
            MakeDataFile(path, PartitionValues({std::move(id), Literal::String("abc")}),
                         spec))},
        spec);
  };
  auto original = schema_;
  auto old_manifest = write_manifest("int.parquet", Literal::Int(7));
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                               SchemaField::MakeRequired(2, "data", string())},
      1);
  auto new_manifest = write_manifest("long.parquet", Literal::Long(4294967296LL));
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list =
          WriteManifestList(GetParam(), 10, 0, 1, {old_manifest, new_manifest}),
  });
  auto metadata = MakeTableMetadata({snapshot}, 10);
  metadata->schemas.insert(metadata->schemas.begin(), original);
  metadata->partition_specs = {unpartitioned_spec_, spec};
  metadata->last_partition_id = 1001;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, Table::Make(TableIdentifier{.name = "table"}, std::move(metadata),
                              "s3://bucket/metadata.json", file_io_, catalog_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, ManifestsTable::Make(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*manifests));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto summaries = std::static_pointer_cast<::arrow::ListArray>(
      batches[0]->GetColumnByName("partition_summaries"));
  auto values = std::static_pointer_cast<::arrow::StructArray>(summaries->values());
  ASSERT_EQ(values->length(), 4);
  for (int index : {2, 3}) {
    auto bounds = std::static_pointer_cast<::arrow::StringArray>(values->field(index));
    EXPECT_EQ(bounds->GetString(0), "7");
    EXPECT_EQ(bounds->GetString(1), "abc");
    EXPECT_EQ(bounds->GetString(2), "4294967296");
    EXPECT_EQ(bounds->GetString(3), "abc");
  }
}

TEST_P(ManifestsTableTest, ManifestBoundsWithDroppedSourceUseJavaRendering) {
  std::shared_ptr<PartitionSpec> spec;
  ICEBERG_UNWRAP_OR_FAIL(
      spec, PartitionSpec::Make(
                1, {PartitionField(1, 1000, "id", Transform::Identity()),
                    PartitionField(1, 1001, "id_bucket", Transform::Bucket(16)),
                    PartitionField(2, 1002, "data_prefix", Transform::Truncate(3))}));
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"data.parquet",
        PartitionValues({Literal::Int(7), Literal::Int(3), Literal::String("abc")})}},
      spec);
  auto metadata = MakeTableMetadata({snapshot}, 10);
  metadata->schemas.push_back(std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(2, "data", string())}, 1));
  metadata->current_schema_id = 1;
  metadata->partition_specs = {unpartitioned_spec_, spec};
  metadata->last_partition_id = 1002;
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, Table::Make(TableIdentifier{.name = "table"}, std::move(metadata),
                              "s3://bucket/metadata.json", file_io_, catalog_));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, ManifestsTable::Make(table));

  auto logger = std::make_shared<CapturingLogger>();
  logger->SetLevel(LogLevel::kWarn);
  ScopedLogger bind(logger);
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*manifests));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  auto summaries = std::static_pointer_cast<::arrow::ListArray>(
      batches[0]->GetColumnByName("partition_summaries"));
  auto values = std::static_pointer_cast<::arrow::StructArray>(summaries->values());
  ASSERT_EQ(values->length(), 3);
  for (int index : {2, 3}) {
    auto bounds = std::static_pointer_cast<::arrow::StringArray>(values->field(index));
    ASSERT_EQ(bounds->null_count(), 0);
    EXPECT_EQ(bounds->GetString(0), "null");
    EXPECT_EQ(bounds->GetString(1), "3");
    EXPECT_EQ(bounds->GetString(2), "abc");
  }

  const auto warnings = logger->records();
  ASSERT_EQ(warnings.size(), 1);
  EXPECT_EQ(warnings[0].level, LogLevel::kWarn);
  EXPECT_THAT(warnings[0].message,
              ::testing::HasSubstr("spec 1, field 'id' (1000), source column 1"));
}

TEST_P(ManifestsTableTest, ScanEmpty) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Scan(*metadata_table));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

TEST_P(ManifestsTableTest, TasksRetainProjectionAndMetadataWithCustomBatchSize) {
  std::vector<ManifestFile> manifests;
  for (int32_t index = 0; index < 3; ++index) {
    manifests.push_back(WriteDataManifest(
        GetParam(), 10,
        {MakeEntry(
            ManifestStatus::kAdded, 10, 1,
            MakeDataFile(std::format("file-{}.parquet", index),
                         PartitionValues(Literal::Int(index)), partitioned_spec_))},
        partitioned_spec_));
  }
  auto first = std::make_shared<Snapshot>(
      Snapshot{.snapshot_id = 10,
               .sequence_number = 1,
               .timestamp_ms = TimePointMsFromUnixMs(1000),
               .manifest_list = WriteManifestList(GetParam(), 10, 0, 1, manifests)});
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first}, 10, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table,
                         table->Inspect(MetadataTableKind::kManifests));
  const auto canonical_schema = metadata_table->schema();
  const auto& fields = canonical_schema->fields();
  auto projection =
      std::make_shared<Schema>(std::vector<SchemaField>{fields[1], fields[0]});
  const std::string first_column(projection->fields()[0].name());
  const std::string second_column(projection->fields()[1].name());
  ICEBERG_UNWRAP_OR_FAIL(auto builder, metadata_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto empty_builder, metadata_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto invalid_builder, metadata_table->NewScan());
  auto invalid_projection = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(fields[0].field_id(), "wrong_type", int64())});
  EXPECT_FALSE(invalid_builder->Project(invalid_projection).Build().has_value());

  // Builders retain metadata even when the source refreshes before Build().
  ASSERT_THAT(RefreshTableSchemaAndPartitionSpec(table, first), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Project(projection).Build());
  ICEBERG_UNWRAP_OR_FAIL(auto empty_scan, empty_builder->Select({}).Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  ICEBERG_UNWRAP_OR_FAIL(auto empty_tasks, empty_scan->PlanTasks());
  ASSERT_EQ(tasks.size(), 1);
  ASSERT_EQ(empty_tasks.size(), 1);
  builder.reset();
  empty_builder.reset();
  invalid_builder.reset();
  scan.reset();
  empty_scan.reset();
  metadata_table.reset();
  table.reset();

  ICEBERG_UNWRAP_OR_FAIL(auto reader, MetadataScanTaskReader::Make({.batch_size = 2}));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(*tasks[0]));
  ICEBERG_UNWRAP_OR_FAIL(auto repeat_stream, reader->Open(*tasks[0]));
  ICEBERG_UNWRAP_OR_FAIL(auto empty_stream, reader->Open(*empty_tasks[0]));
  reader.reset();
  tasks.clear();
  empty_tasks.clear();
  for (auto* output : {&stream, &repeat_stream}) {
    ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(*output)));
    ASSERT_EQ(batches.size(), 2);
    EXPECT_EQ(batches[0]->num_rows(), 2);
    EXPECT_EQ(batches[1]->num_rows(), 1);
    ASSERT_EQ(batches[0]->num_columns(), 2);
    EXPECT_EQ(batches[0]->schema()->field(0)->name(), first_column);
    EXPECT_EQ(batches[0]->schema()->field(1)->name(), second_column);
  }
  ICEBERG_UNWRAP_OR_FAIL(auto empty_batches, ReadAllBatches(std::move(empty_stream)));
  ASSERT_EQ(empty_batches.size(), 2);
  EXPECT_EQ(empty_batches[0]->num_rows(), 2);
  EXPECT_EQ(empty_batches[1]->num_rows(), 1);
  EXPECT_EQ(empty_batches[0]->num_columns(), 0);
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, ManifestsTableTest, ::testing::Values(2, 3));

}  // namespace
}  // namespace iceberg
