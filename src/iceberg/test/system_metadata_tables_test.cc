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

#include <bit>
#include <chrono>
#include <cstdint>
#include <format>
#include <limits>
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
#include "iceberg/inspect/files_table.h"
#include "iceberg/inspect/manifests_table.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/partitions_table.h"
#include "iceberg/inspect/refs_table.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/util/conversions.h"
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

TEST_P(SystemMetadataTablesTest, ScansBranchesAndTagsAsRefs) {
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

  ICEBERG_UNWRAP_OR_FAIL(auto refs_table, MetadataTable::Make<RefsTable>(table));
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

TEST_P(SystemMetadataTablesTest, RejectsCombiningNamedRefsWithTimeTravel) {
  auto first = MakeAppendSnapshot(GetParam(), 1, std::nullopt, 1, {"first.parquet"});
  auto second = MakeAppendSnapshot(GetParam(), 2, 1, 2, {"second.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto branch, SnapshotRef::MakeBranch(1));
  ICEBERG_UNWRAP_OR_FAIL(auto tag, SnapshotRef::MakeTag(1));
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  refs.emplace("dev", std::move(branch));
  refs.emplace("release", std::move(tag));
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second}, 2, std::move(refs)));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto partitions, MetadataTable::Make<PartitionsTable>(table));
  for (TimeTravelMetadataTable* metadata_table :
       {static_cast<TimeTravelMetadataTable*>(files.get()),
        static_cast<TimeTravelMetadataTable*>(manifests.get()),
        static_cast<TimeTravelMetadataTable*>(partitions.get())}) {
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

  // A named ref alone selects its head; main is a no-op in Java's useRef.
  for (const std::string ref_name : {"dev", "release", "main"}) {
    SnapshotSelection selection{.ref_name = ref_name};
    if (ref_name == "main") {
      selection.snapshot = int64_t{1};
    }
    ICEBERG_UNWRAP_OR_FAIL(auto stream, files->Scan(selection));
    ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0]->num_rows(), 1);
    auto paths = std::static_pointer_cast<::arrow::StringArray>(
        batches[0]->GetColumnByName("file_path"));
    EXPECT_EQ(paths->GetString(0), "first.parquet");
  }
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

TEST_P(SystemMetadataTablesTest, ManifestNullBoundsUseJavaRendering) {
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

TEST_P(SystemMetadataTablesTest, ManifestBoundsSurviveDroppedBucketSource) {
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

TEST_P(SystemMetadataTablesTest, GroupsAllNaNsButDistinguishesSignedZeros) {
  // Exercise both floating-point types, including NaNs with different payloads.
  for (const bool use_float : {false, true}) {
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "id", int32()),
        SchemaField::MakeRequired(
            2, "data", use_float ? std::shared_ptr<Type>(float32()) : float64())});
    ICEBERG_UNWRAP_OR_FAIL(
        partitioned_spec_,
        PartitionSpec::Make(1, {PartitionField(2, 1000, "data", Transform::Identity())}));
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto literal = [use_float](double value) {
      return use_float ? Literal::Float(static_cast<float>(value))
                       : Literal::Double(value);
    };
    auto payload_nan =
        use_float ? Literal::Float(std::bit_cast<float>(uint32_t{0x7fc00001}))
                  : Literal::Double(std::bit_cast<double>(uint64_t{0x7ff8000000000001}));
    auto snapshot = MakeAppendSnapshotWithPartitionValues(
        GetParam(), 10, std::nullopt, 1,
        {{"positive_nan.parquet", PartitionValues(literal(nan))},
         {"negative_nan.parquet", PartitionValues(literal(-nan))},
         {"payload_nan.parquet", PartitionValues(payload_nan)},
         {"positive_zero.parquet", PartitionValues(literal(0.0))},
         {"negative_zero.parquet", PartitionValues(literal(-0.0))}},
        partitioned_spec_);
    ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));
    ICEBERG_UNWRAP_OR_FAIL(auto partitions, MetadataTable::Make<PartitionsTable>(table));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, partitions->Scan());
    ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0]->num_rows(), 3);
    auto counts = std::static_pointer_cast<::arrow::Int32Array>(
        batches[0]->GetColumnByName("file_count"));
    EXPECT_THAT(
        (std::vector<int32_t>{counts->Value(0), counts->Value(1), counts->Value(2)}),
        ::testing::UnorderedElementsAre(3, 1, 1));
  }
}

TEST_P(SystemMetadataTablesTest, ScansDecimalPartitionsAndReadableBounds) {
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeRequired(2, "amount", decimal(38, 2))});
  ICEBERG_UNWRAP_OR_FAIL(
      partitioned_spec_,
      PartitionSpec::Make(1, {PartitionField(2, 1000, "amount", Transform::Identity())}));
  const int128_t large = (static_cast<int128_t>(1) << 80) + 123;
  std::vector<ManifestEntry> entries;
  const std::vector<int128_t> values{123, -456, large, -large, 0};
  for (size_t index = 0; index < values.size(); ++index) {
    auto literal = Literal::Decimal(values[index], 38, 2);
    auto file = MakeDataFile(std::format("decimal-{}.parquet", index),
                             PartitionValues(literal), partitioned_spec_);
    ICEBERG_UNWRAP_OR_FAIL(auto bound, Conversions::ToBytes(literal));
    file->lower_bounds.emplace(2, bound);
    file->upper_bounds.emplace(2, bound);
    entries.push_back(MakeEntry(ManifestStatus::kAdded, 10, 1, std::move(file)));
  }
  auto manifest =
      WriteDataManifest(GetParam(), 10, std::move(entries), partitioned_spec_);
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list = WriteManifestList(GetParam(), 10, 0, 1, {manifest}),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, files->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), values.size());
  auto partition = std::static_pointer_cast<::arrow::StructArray>(
      batches[0]->GetColumnByName("partition"));
  auto amounts = std::static_pointer_cast<::arrow::Decimal128Array>(partition->field(0));
  auto metrics = std::static_pointer_cast<::arrow::StructArray>(
      batches[0]->GetColumnByName("readable_metrics"));
  auto amount_metrics =
      std::static_pointer_cast<::arrow::StructArray>(metrics->GetFieldByName("amount"));
  auto lower = std::static_pointer_cast<::arrow::Decimal128Array>(
      amount_metrics->GetFieldByName("lower_bound"));
  auto upper = std::static_pointer_cast<::arrow::Decimal128Array>(
      amount_metrics->GetFieldByName("upper_bound"));
  for (size_t index = 0; index < values.size(); ++index) {
    ICEBERG_UNWRAP_OR_FAIL(auto expected, Decimal(values[index]).ToString(2));
    EXPECT_EQ(amounts->FormatValue(index), expected);
    EXPECT_EQ(lower->FormatValue(index), expected);
    EXPECT_EQ(upper->FormatValue(index), expected);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto partitions, MetadataTable::Make<PartitionsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto partition_stream, partitions->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto partition_batches,
                         ReadAllBatches(std::move(partition_stream)));
  ASSERT_EQ(partition_batches.size(), 1);
  ASSERT_EQ(partition_batches[0]->num_rows(), values.size());
  auto partition_values = std::static_pointer_cast<::arrow::StructArray>(
      partition_batches[0]->GetColumnByName("partition"));
  auto partition_amounts =
      std::static_pointer_cast<::arrow::Decimal128Array>(partition_values->field(0));
  for (size_t index = 0; index < values.size(); ++index) {
    ICEBERG_UNWRAP_OR_FAIL(auto expected, Decimal(values[index]).ToString(2));
    EXPECT_EQ(partition_amounts->FormatValue(index), expected);
  }
}

TEST_P(SystemMetadataTablesTest, FilesScanReadsManifestsOnDemand) {
  std::vector<ManifestEntry> entries;
  for (int64_t index = 0; index < MetadataTable::kBatchSize; ++index) {
    entries.push_back(MakeEntry(ManifestStatus::kAdded, 10, 1,
                                MakeDataFile(std::format("data-{}.parquet", index))));
  }
  auto first_manifest = WriteDataManifest(GetParam(), 10, std::move(entries));
  auto second_manifest = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kAdded, 10, 1, MakeDataFile("last.parquet"))});
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list =
          WriteManifestList(GetParam(), 10, 0, 1, {first_manifest, second_manifest}),
  });
  ASSERT_THAT(file_io_->DeleteFile(second_manifest.manifest_path), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, files->Scan());
  auto imported = ::arrow::ImportRecordBatchReader(&stream);
  ASSERT_TRUE(imported.ok()) << imported.status().ToString();
  auto reader = *imported;
  // The stream owns everything it needs, independent of the source table's lifetime.
  files.reset();
  table.reset();
  auto first = reader->Next();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_NE(*first, nullptr);
  EXPECT_EQ((*first)->num_rows(), MetadataTable::kBatchSize);
  // Opening the absent second manifest is deferred until the next batch.
  EXPECT_FALSE(reader->Next().ok());
}

TEST_P(SystemMetadataTablesTest, ScansEmptyMetadataTables) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto refs, MetadataTable::Make<RefsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, MetadataTable::Make<ManifestsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto partitions, MetadataTable::Make<PartitionsTable>(table));
  for (auto* metadata_table : std::vector<MetadataTable*>{
           refs.get(), files.get(), manifests.get(), partitions.get()}) {
    ICEBERG_UNWRAP_OR_FAIL(auto stream, metadata_table->Scan());
    ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
    EXPECT_TRUE(batches.empty());
  }
}

TEST_P(SystemMetadataTablesTest, FilesScanSkipsEmptyAndDeletedEntries) {
  auto first = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kAdded, 10, 1, MakeDataFile("first.parquet"))});
  auto empty = WriteDataManifest(GetParam(), 10, {});
  auto last = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kDeleted, 10, 1, MakeDataFile("deleted.parquet")),
       MakeEntry(ManifestStatus::kExisting, 10, 1, MakeDataFile("last.parquet"))});
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list = WriteManifestList(GetParam(), 10, 0, 1, {first, empty, last}),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10));
  ICEBERG_UNWRAP_OR_FAIL(auto files, MetadataTable::Make<FilesTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, files->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), 2);
  auto paths = std::static_pointer_cast<::arrow::StringArray>(
      batches[0]->GetColumnByName("file_path"));
  EXPECT_EQ(paths->GetString(0), "first.parquet");
  EXPECT_EQ(paths->GetString(1), "last.parquet");
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, SystemMetadataTablesTest,
                         ::testing::Values(2, 3));

}  // namespace
}  // namespace iceberg
