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

#include "iceberg/inspect/position_deletes_table.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_register.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/deletes/dv_writer.h"
#include "iceberg/file_format.h"
#include "iceberg/file_writer.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

using ::testing::ElementsAre;

class PositionDeletesTableTest : public ScanTestBase {
 protected:
  void SetUp() override {
    ScanTestBase::SetUp();
    parquet::RegisterAll();
    arrow::RegisterAll();
  }

  std::shared_ptr<DataFile> WritePositionDeletes(
      std::string path, const std::vector<std::pair<std::string, int64_t>>& deletes,
      const std::shared_ptr<PartitionSpec>& spec, const PartitionValues& partition) {
    PositionDeleteWriterOptions options{
        .path = std::move(path),
        .schema = schema_,
        .spec = spec,
        .partition = partition,
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .flush_threshold = 10000,
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };
    auto writer = PositionDeleteWriter::Make(options).value();
    for (const auto& [file_path, pos] : deletes) {
      ICEBERG_THROW_NOT_OK(writer->WriteDelete(file_path, pos));
    }
    ICEBERG_THROW_NOT_OK(writer->Close());
    return writer->Metadata().value().data_files.front();
  }

  Result<std::shared_ptr<DataFile>> WritePositionDeletesWithRows(
      std::string path, std::string_view json, std::vector<SchemaField> row_fields,
      bool row_required, const std::shared_ptr<PartitionSpec>& spec,
      const PartitionValues& partition) {
    auto row_type = std::make_shared<StructType>(std::move(row_fields));
    auto row_field =
        row_required ? SchemaField::MakeRequired(MetadataColumns::kDeleteFileRowColumnId,
                                                 MetadataColumns::kDeleteFileRowFieldName,
                                                 std::move(row_type),
                                                 MetadataColumns::kDeleteFileRowDoc)
                     : SchemaField::MakeOptional(MetadataColumns::kDeleteFileRowColumnId,
                                                 MetadataColumns::kDeleteFileRowFieldName,
                                                 std::move(row_type),
                                                 MetadataColumns::kDeleteFileRowDoc);
    auto delete_schema = std::make_shared<Schema>(std::vector<SchemaField>{
        MetadataColumns::kDeleteFilePath,
        MetadataColumns::kDeleteFilePos,
        std::move(row_field),
    });
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, WriterFactoryRegistry::Open(
                         FileFormatType::kParquet,
                         WriterOptions{
                             .path = path,
                             .schema = delete_schema,
                             .io = file_io_,
                             .properties = WriterProperties::FromMap(
                                 {{"write.parquet.compression-codec", "uncompressed"}}),
                         }));

    ArrowSchema arrow_c_schema;
    ICEBERG_THROW_NOT_OK(ToArrowSchema(*delete_schema, &arrow_c_schema));
    auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
    auto rows = ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_type->fields()),
                                                   std::string(json))
                    .ValueOrDie();
    ArrowArray arrow_array;
    ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportArray(*rows, &arrow_array));
    ICEBERG_RETURN_UNEXPECTED(writer->Write(&arrow_array));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto length, writer->length());

    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = std::move(path),
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = rows->length(),
        .file_size_in_bytes = length,
        .partition_spec_id = spec->spec_id(),
    });
  }

  std::vector<std::shared_ptr<DataFile>> WriteDeletionVectors(
      std::string path,
      const std::vector<std::pair<std::string, std::vector<int64_t>>>& deletes,
      const std::shared_ptr<PartitionSpec>& spec, const PartitionValues& partition) {
    auto writer = DVWriter::Make(DVWriterOptions{
                                     .path = std::move(path),
                                     .io = file_io_,
                                     .load_previous_deletes = [](std::string_view)
                                         -> Result<std::optional<PositionDeleteIndex>> {
                                       return std::nullopt;
                                     },
                                 })
                      .value();
    for (const auto& [file_path, positions] : deletes) {
      for (int64_t pos : positions) {
        ICEBERG_THROW_NOT_OK(writer->Delete(file_path, pos, spec, partition));
      }
    }
    ICEBERG_THROW_NOT_OK(writer->Close());
    return writer->Metadata().value().data_files;
  }

  std::shared_ptr<Snapshot> MakeSnapshot(int8_t table_format_version, int64_t snapshot_id,
                                         int64_t sequence_number,
                                         const std::vector<ManifestFile>& manifests) {
    auto manifest_list = WriteManifestList(table_format_version, snapshot_id, 0,
                                           sequence_number, manifests);
    return std::make_shared<Snapshot>(Snapshot{
        .snapshot_id = snapshot_id,
        .parent_snapshot_id = std::nullopt,
        .sequence_number = sequence_number,
        .timestamp_ms = TimePointMsFromUnixMs(1609459200000L),
        .manifest_list = std::move(manifest_list),
        .summary = {{"operation", "delete"}},
        .schema_id = schema_->schema_id(),
    });
  }

  std::shared_ptr<Table> MakeTable(int8_t format_version,
                                   const std::shared_ptr<PartitionSpec>& spec,
                                   std::shared_ptr<Snapshot> snapshot = nullptr) {
    return MakeTableWithSpecs(format_version, {spec}, spec, std::move(snapshot));
  }

  std::shared_ptr<Table> MakeTableWithSpecs(
      int8_t format_version, std::vector<std::shared_ptr<PartitionSpec>> specs,
      const std::shared_ptr<PartitionSpec>& default_spec,
      std::shared_ptr<Snapshot> snapshot = nullptr) {
    std::vector<std::shared_ptr<Snapshot>> snapshots;
    if (snapshot != nullptr) {
      snapshots.push_back(snapshot);
    }
    int32_t last_partition_id = PartitionSpec::kInvalidPartitionFieldId;
    for (const auto& spec : specs) {
      last_partition_id = std::max(last_partition_id, spec->last_assigned_field_id());
    }
    auto metadata = std::make_shared<TableMetadata>(TableMetadata{
        .format_version = format_version,
        .table_uuid = "test-table-uuid",
        .location = "/tmp/table",
        .last_sequence_number = snapshot == nullptr ? 0 : snapshot->sequence_number,
        .last_updated_ms = TimePointMsFromUnixMs(1609459200000L),
        .last_column_id = schema_->HighestFieldId().value(),
        .schemas = {schema_},
        .current_schema_id = schema_->schema_id(),
        .partition_specs = std::move(specs),
        .default_spec_id = default_spec->spec_id(),
        .last_partition_id = last_partition_id,
        .current_snapshot_id =
            snapshot == nullptr ? kInvalidSnapshotId : snapshot->snapshot_id,
        .snapshots = std::move(snapshots),
        .default_sort_order_id = 0,
    });
    return Table::Make(TableIdentifier{.name = "table"}, std::move(metadata),
                       "/tmp/table/metadata.json", file_io_,
                       std::make_shared<::testing::NiceMock<MockCatalog>>())
        .value();
  }

  std::unique_ptr<MetadataTable> MakePositionDeletesTable(
      const std::shared_ptr<Table>& table) {
    return MetadataTable::Make(table, MetadataTable::Kind::kPositionDeletes).value();
  }

  static std::shared_ptr<::arrow::RecordBatch> Import(ArrowArray array,
                                                      const Schema& schema) {
    ArrowSchema c_schema;
    ICEBERG_THROW_NOT_OK(ToArrowSchema(schema, &c_schema));
    auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
    return ::arrow::ImportRecordBatch(&array, arrow_schema).ValueOrDie();
  }
};

TEST_P(PositionDeletesTableTest, SchemaAndEmptyScan) {
  auto table = MakePositionDeletesTable(MakeTable(GetParam(), unpartitioned_spec_));

  EXPECT_EQ(table->kind(), MetadataTable::Kind::kPositionDeletes);
  EXPECT_EQ(table->name().name, "table.position_deletes");

  std::vector<std::string> names;
  for (const auto& field : table->schema()->fields()) {
    names.emplace_back(field.name());
  }
  std::vector<std::string> expected{"file_path", "pos", "row", "spec_id",
                                    "delete_file_path"};
  if (GetParam() >= 3) {
    expected.push_back("content_offset");
    expected.push_back("content_size_in_bytes");
  }
  EXPECT_EQ(names, expected);

  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan());
  auto batch = Import(std::move(array), *table->schema());
  EXPECT_EQ(batch->num_rows(), 0);
}

TEST_P(PositionDeletesTableTest, ExpandsParquetAndProjectsColumns) {
  auto delete_file = WritePositionDeletes(
      "position-deletes.parquet", {{"data-a.parquet", 2}, {"data-b.parquet", 7}},
      partitioned_spec_, PartitionValues(Literal::Int(11)));
  constexpr int64_t kSnapshotId = 10;
  auto manifest = WriteDeleteManifest(
      GetParam(), kSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kSnapshotId, 1, delete_file)},
      partitioned_spec_);
  auto table = MakePositionDeletesTable(
      MakeTable(GetParam(), partitioned_spec_,
                MakeSnapshot(GetParam(), kSnapshotId, 1, {manifest})));

  const auto& fields = table->schema()->fields();
  auto projected =
      std::make_unique<Schema>(std::vector<SchemaField>{fields[5], fields[1], fields[3]});
  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan(*projected));
  auto batch = Import(std::move(array), *projected);

  ASSERT_EQ(batch->num_rows(), 2);
  EXPECT_EQ(batch->schema()->field(0)->name(), "delete_file_path");
  EXPECT_EQ(batch->schema()->field(1)->name(), "pos");
  EXPECT_EQ(batch->schema()->field(2)->name(), "partition");
  auto delete_paths = std::static_pointer_cast<::arrow::StringArray>(batch->column(0));
  auto positions = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
  auto partitions = std::static_pointer_cast<::arrow::StructArray>(batch->column(2));
  auto buckets = std::static_pointer_cast<::arrow::Int32Array>(partitions->field(0));
  std::vector<std::string> actual_delete_paths{delete_paths->GetString(0),
                                               delete_paths->GetString(1)};
  std::vector<int64_t> actual_positions{positions->Value(0), positions->Value(1)};
  std::vector<int32_t> actual_buckets{buckets->Value(0), buckets->Value(1)};
  EXPECT_THAT(actual_delete_paths,
              ElementsAre("position-deletes.parquet", "position-deletes.parquet"));
  EXPECT_THAT(actual_positions, ElementsAre(2, 7));
  EXPECT_THAT(actual_buckets, ElementsAre(11, 11));
}

TEST_F(PositionDeletesTableTest, SurfacesDeletedRowsFromParquet) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto delete_file,
      WritePositionDeletesWithRows(
          "position-deletes-with-rows.parquet",
          R"([["data.parquet", 5, [42, "deleted"]]])",
          std::vector<SchemaField>(schema_->fields().begin(), schema_->fields().end()),
          true, unpartitioned_spec_, PartitionValues{}));
  constexpr int64_t kSnapshotId = 15;
  auto manifest = WriteDeleteManifest(
      2, kSnapshotId, {MakeEntry(ManifestStatus::kAdded, kSnapshotId, 1, delete_file)},
      unpartitioned_spec_);
  auto table = MakePositionDeletesTable(
      MakeTable(2, unpartitioned_spec_, MakeSnapshot(2, kSnapshotId, 1, {manifest})));

  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan());
  auto batch = Import(std::move(array), *table->schema());
  ASSERT_EQ(batch->num_rows(), 1);

  auto rows = std::static_pointer_cast<::arrow::StructArray>(batch->column(2));
  ASSERT_FALSE(rows->IsNull(0));
  auto ids = std::static_pointer_cast<::arrow::Int32Array>(rows->field(0));
  auto data = std::static_pointer_cast<::arrow::StringArray>(rows->field(1));
  EXPECT_EQ(ids->Value(0), 42);
  EXPECT_EQ(data->GetString(0), "deleted");
}

TEST_F(PositionDeletesTableTest, ProjectsSubsetDeletedRowsFromParquet) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto delete_file,
      WritePositionDeletesWithRows("position-deletes-with-subset-rows.parquet",
                                   R"([["data.parquet", 5, ["deleted"]]])",
                                   {schema_->fields()[1]}, true, unpartitioned_spec_,
                                   PartitionValues{}));
  constexpr int64_t kSnapshotId = 16;
  auto manifest = WriteDeleteManifest(
      2, kSnapshotId, {MakeEntry(ManifestStatus::kAdded, kSnapshotId, 1, delete_file)},
      unpartitioned_spec_);
  auto table = MakePositionDeletesTable(
      MakeTable(2, unpartitioned_spec_, MakeSnapshot(2, kSnapshotId, 1, {manifest})));

  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan());
  auto batch = Import(std::move(array), *table->schema());
  ASSERT_EQ(batch->num_rows(), 1);

  auto rows = std::static_pointer_cast<::arrow::StructArray>(batch->column(2));
  ASSERT_FALSE(rows->IsNull(0));
  auto ids = std::static_pointer_cast<::arrow::Int32Array>(rows->field(0));
  auto data = std::static_pointer_cast<::arrow::StringArray>(rows->field(1));
  EXPECT_TRUE(ids->IsNull(0));
  EXPECT_EQ(data->GetString(0), "deleted");
}

TEST_F(PositionDeletesTableTest, DoesNotDefaultOmittedDeletedRowFields) {
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int32())
              .WithInitialDefault(std::make_shared<const Literal>(Literal::Int(99))),
          SchemaField::MakeRequired(2, "data", string()),
      },
      2);
  ICEBERG_UNWRAP_OR_FAIL(
      auto delete_file,
      WritePositionDeletesWithRows("position-deletes-with-defaulted-subset-row.parquet",
                                   R"([["data.parquet", 5, ["deleted"]]])",
                                   {schema_->fields()[1]}, true, unpartitioned_spec_,
                                   PartitionValues{}));
  constexpr int64_t kSnapshotId = 17;
  auto manifest = WriteDeleteManifest(
      3, kSnapshotId, {MakeEntry(ManifestStatus::kAdded, kSnapshotId, 1, delete_file)},
      unpartitioned_spec_);
  auto table = MakePositionDeletesTable(
      MakeTable(3, unpartitioned_spec_, MakeSnapshot(3, kSnapshotId, 1, {manifest})));

  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan());
  auto batch = Import(std::move(array), *table->schema());
  ASSERT_EQ(batch->num_rows(), 1);

  auto rows = std::static_pointer_cast<::arrow::StructArray>(batch->column(2));
  auto ids = std::static_pointer_cast<::arrow::Int32Array>(rows->field(0));
  auto data = std::static_pointer_cast<::arrow::StringArray>(rows->field(1));
  EXPECT_TRUE(ids->IsNull(0));
  EXPECT_EQ(data->GetString(0), "deleted");
}

TEST_F(PositionDeletesTableTest, RejectsNullDeletedRowsFromParquet) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto delete_file,
      WritePositionDeletesWithRows(
          "position-deletes-with-null-row.parquet", R"([["data.parquet", 5, null]])",
          std::vector<SchemaField>(schema_->fields().begin(), schema_->fields().end()),
          false, unpartitioned_spec_, PartitionValues{}));
  constexpr int64_t kSnapshotId = 19;
  auto manifest = WriteDeleteManifest(
      2, kSnapshotId, {MakeEntry(ManifestStatus::kAdded, kSnapshotId, 1, delete_file)},
      unpartitioned_spec_);
  auto table = MakePositionDeletesTable(
      MakeTable(2, unpartitioned_spec_, MakeSnapshot(2, kSnapshotId, 1, {manifest})));

  auto result = table->Scan();
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArrowData));
  EXPECT_THAT(result, HasErrorMessage("null row values"));
}

TEST_F(PositionDeletesTableTest, ReassignsPartitionIdsCollidingWithNestedRows) {
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "payload",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeRequired(1000, "id", int32()),
                                })),
      SchemaField::MakeRequired(2, "data", string()),
  });
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(2, {PartitionField(1000, 1000, "id", Transform::Identity())}));
  auto shared_spec = std::shared_ptr<PartitionSpec>(std::move(spec));
  auto table = MakePositionDeletesTable(MakeTable(2, shared_spec));

  EXPECT_THAT(table->schema()->HighestFieldId(), IsOk());
  const auto& fields = table->schema()->fields();
  auto partition_type = std::static_pointer_cast<StructType>(fields[3].type());
  ASSERT_EQ(partition_type->fields().size(), 1);
  EXPECT_NE(partition_type->fields()[0].field_id(), 1000);
  EXPECT_NE(partition_type->fields()[0].field_id(), MetadataColumns::kPartitionColumnId);
}

TEST_F(PositionDeletesTableTest, RejectsIncompatiblePartitionEvolution) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto old_spec,
      PartitionSpec::Make(1, {PartitionField(2, 1000, "data", Transform::Identity())}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_spec,
      PartitionSpec::Make(2, {PartitionField(2, 1000, "data", Transform::Bucket(16))}));
  auto shared_old_spec = std::shared_ptr<PartitionSpec>(std::move(old_spec));
  auto shared_new_spec = std::shared_ptr<PartitionSpec>(std::move(new_spec));
  auto source =
      MakeTableWithSpecs(2, {shared_old_spec, shared_new_spec}, shared_new_spec);

  auto table = MetadataTable::Make(source, MetadataTable::Kind::kPositionDeletes);
  EXPECT_THAT(table, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(table, HasErrorMessage("incompatible transforms"));
}

TEST_F(PositionDeletesTableTest, PromotesHistoricalPartitionValues) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(1, {PartitionField(1, 1000, "id", Transform::Identity())}));
  auto shared_spec = std::shared_ptr<PartitionSpec>(std::move(spec));
  auto historical_schema = schema_;
  auto delete_file = WritePositionDeletes("position-deletes-int-partition.parquet",
                                          {{"data.parquet", 5}}, shared_spec,
                                          PartitionValues(Literal::Int(11)));
  constexpr int64_t kSnapshotId = 18;
  auto manifest = WriteDeleteManifest(
      2, kSnapshotId, {MakeEntry(ManifestStatus::kAdded, kSnapshotId, 1, delete_file)},
      shared_spec);

  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int64()),
          SchemaField::MakeRequired(2, "data", string()),
      },
      2);
  auto source = MakeTable(2, shared_spec, MakeSnapshot(2, kSnapshotId, 1, {manifest}));
  source->metadata()->schemas.insert(source->metadata()->schemas.begin(),
                                     historical_schema);

  auto table = MakePositionDeletesTable(source);
  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan());
  auto batch = Import(std::move(array), *table->schema());
  ASSERT_EQ(batch->num_rows(), 1);
  auto partitions = std::static_pointer_cast<::arrow::StructArray>(batch->column(3));
  auto ids = std::static_pointer_cast<::arrow::Int64Array>(partitions->field(0));
  EXPECT_EQ(ids->Value(0), 11);
}

TEST_F(PositionDeletesTableTest, OmitsDroppedHistoricalPartitionSources) {
  auto historical_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int32()),
          SchemaField::MakeRequired(2, "data", string()),
          SchemaField::MakeOptional(3, "dropped", string()),
      },
      1);
  ICEBERG_UNWRAP_OR_FAIL(
      auto old_spec, PartitionSpec::Make(
                         1, {PartitionField(3, 1000, "dropped", Transform::Identity())}));
  auto shared_old_spec = std::shared_ptr<PartitionSpec>(std::move(old_spec));
  auto source =
      MakeTableWithSpecs(2, {shared_old_spec, unpartitioned_spec_}, unpartitioned_spec_);
  source->metadata()->schemas.insert(source->metadata()->schemas.begin(),
                                     historical_schema);

  auto table = MakePositionDeletesTable(source);
  std::vector<std::string> names;
  for (const auto& field : table->schema()->fields()) {
    names.emplace_back(field.name());
  }
  EXPECT_THAT(names,
              ElementsAre("file_path", "pos", "row", "spec_id", "delete_file_path"));
}

TEST_F(PositionDeletesTableTest, ProjectsEmptyScan) {
  auto table = MakePositionDeletesTable(MakeTable(3, unpartitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto projected, table->schema()->Select(std::vector<std::string>{
                                             "pos", "delete_file_path"}));

  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan(*projected));
  auto batch = Import(std::move(array), *projected);
  EXPECT_EQ(batch->num_rows(), 0);
  ASSERT_EQ(batch->num_columns(), 2);
  EXPECT_EQ(batch->schema()->field(0)->name(), "pos");
  EXPECT_EQ(batch->schema()->field(1)->name(), "delete_file_path");
}

TEST_F(PositionDeletesTableTest, RejectsNestedRowProjection) {
  auto table = MakePositionDeletesTable(MakeTable(3, unpartitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto projected,
                         table->schema()->Select(std::vector<std::string>{"row.id"}));

  auto result = table->Scan(*projected);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("only supports complete top-level fields"));
}

TEST_F(PositionDeletesTableTest, ExpandsUpgradedMixedTable) {
  auto parquet_file = WritePositionDeletes(
      "old-position-deletes.parquet", {{"old-data.parquet", 3}, {"old-data.parquet", 9}},
      unpartitioned_spec_, PartitionValues{});
  auto dv_files =
      WriteDeletionVectors("new-deletes.puffin", {{"new-data.parquet", {4, 12}}},
                           unpartitioned_spec_, PartitionValues{});

  constexpr int64_t kSnapshotId = 20;
  auto old_manifest = WriteDeleteManifest(
      2, kSnapshotId,
      {MakeEntry(ManifestStatus::kExisting, kSnapshotId, 1, parquet_file)},
      unpartitioned_spec_);
  auto new_manifest = WriteDeleteManifest(
      3, kSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kSnapshotId, 2, dv_files.front())},
      unpartitioned_spec_);
  auto table = MakePositionDeletesTable(
      MakeTable(3, unpartitioned_spec_,
                MakeSnapshot(3, kSnapshotId, 2, {old_manifest, new_manifest})));

  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan());
  auto batch = Import(std::move(array), *table->schema());
  ASSERT_EQ(batch->num_rows(), 4);

  auto data_paths = std::static_pointer_cast<::arrow::StringArray>(batch->column(0));
  auto positions = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
  auto rows = std::static_pointer_cast<::arrow::StructArray>(batch->column(2));
  auto delete_paths = std::static_pointer_cast<::arrow::StringArray>(batch->column(4));
  auto offsets = std::static_pointer_cast<::arrow::Int64Array>(batch->column(5));
  auto sizes = std::static_pointer_cast<::arrow::Int64Array>(batch->column(6));

  EXPECT_EQ(rows->null_count(), 4);
  std::vector<std::string> actual_data_paths{
      data_paths->GetString(0), data_paths->GetString(1), data_paths->GetString(2),
      data_paths->GetString(3)};
  std::vector<int64_t> actual_positions{positions->Value(0), positions->Value(1),
                                        positions->Value(2), positions->Value(3)};
  std::vector<std::string> actual_delete_paths{delete_paths->GetString(0),
                                               delete_paths->GetString(2)};
  EXPECT_THAT(actual_data_paths, ElementsAre("old-data.parquet", "old-data.parquet",
                                             "new-data.parquet", "new-data.parquet"));
  EXPECT_THAT(actual_positions, ElementsAre(3, 9, 4, 12));
  EXPECT_THAT(actual_delete_paths,
              ElementsAre("old-position-deletes.parquet", "new-deletes.puffin"));
  EXPECT_TRUE(offsets->IsNull(0));
  EXPECT_TRUE(sizes->IsNull(0));
  EXPECT_FALSE(offsets->IsNull(2));
  EXPECT_FALSE(sizes->IsNull(2));
  EXPECT_EQ(offsets->Value(2), *dv_files.front()->content_offset);
  EXPECT_EQ(sizes->Value(2), *dv_files.front()->content_size_in_bytes);
}

TEST_F(PositionDeletesTableTest, ExpandsMultiplePuffinBlobs) {
  auto dv_files =
      WriteDeletionVectors("multi-deletes.puffin",
                           {{"data-a.parquet", {1, 5}}, {"data-b.parquet", {2, 8, 13}}},
                           unpartitioned_spec_, PartitionValues{});
  ASSERT_EQ(dv_files.size(), 2);

  constexpr int64_t kSnapshotId = 30;
  std::vector<ManifestEntry> entries;
  for (const auto& file : dv_files) {
    entries.push_back(MakeEntry(ManifestStatus::kAdded, kSnapshotId, 1, file));
  }
  auto manifest =
      WriteDeleteManifest(3, kSnapshotId, std::move(entries), unpartitioned_spec_);
  auto table = MakePositionDeletesTable(
      MakeTable(3, unpartitioned_spec_, MakeSnapshot(3, kSnapshotId, 1, {manifest})));

  ICEBERG_UNWRAP_OR_FAIL(auto array, table->Scan());
  auto batch = Import(std::move(array), *table->schema());
  ASSERT_EQ(batch->num_rows(), 5);

  auto data_paths = std::static_pointer_cast<::arrow::StringArray>(batch->column(0));
  auto positions = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
  std::vector<std::tuple<std::string, int64_t>> rows;
  for (int64_t i = 0; i < batch->num_rows(); ++i) {
    rows.emplace_back(data_paths->GetString(i), positions->Value(i));
  }
  EXPECT_THAT(rows, ElementsAre(std::tuple{"data-a.parquet", int64_t{1}},
                                std::tuple{"data-a.parquet", int64_t{5}},
                                std::tuple{"data-b.parquet", int64_t{2}},
                                std::tuple{"data-b.parquet", int64_t{8}},
                                std::tuple{"data-b.parquet", int64_t{13}}));
  EXPECT_NE(dv_files[0]->content_offset, dv_files[1]->content_offset);
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, PositionDeletesTableTest,
                         ::testing::Values(2, 3));

}  // namespace
}  // namespace iceberg
