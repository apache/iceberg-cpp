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

#pragma once

#include <atomic>
#include <barrier>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include "iceberg/inspect/metadata_scan.h"
#include "iceberg/inspect/metadata_scan_task_reader.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/util/conversions.h"

namespace iceberg {

struct SnapshotSelection {
  std::variant<std::monostate, int64_t, TimePointMs> snapshot;
  std::string ref_name;
};

class MetadataTableScanTestBase : public ScanTestBase {
 protected:
  void CheckConcurrentBuilders(MetadataTableKind kind) {
    ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
    ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, table->Inspect(kind));
    const auto held_schema = metadata_table->schema();
    const size_t num_fields = held_schema->fields().size();
    std::atomic<int> failures = 0;
    std::barrier start(4);
    std::vector<std::thread> threads;
    for (int thread = 0; thread < 4; ++thread) {
      threads.emplace_back([&] {
        start.arrive_and_wait();
        for (int iteration = 0; iteration < 1000; ++iteration) {
          auto builder = iteration % 2 == 0 ? metadata_table->NewScan()
                                            : MetadataScanBuilder::Make(*metadata_table);
          if (!builder || !(*builder)->Build()) {
            ++failures;
          }
          auto schema = metadata_table->schema();
          if (!schema || schema->fields().size() != num_fields ||
              held_schema->fields().size() != num_fields) {
            ++failures;
          }
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(failures.load(), 0);
  }

  void CheckTaskProjectionAndLifetime(MetadataTableKind kind) {
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
    ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first}, 10, {}, partitioned_spec_));
    ICEBERG_UNWRAP_OR_FAIL(auto metadata_table, table->Inspect(kind));
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
    ASSERT_THAT(RefreshTableSchemaAndPartitionSpec(table, first, arrow::MakeMockFileIO()),
                IsOk());
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

  template <typename MetadataTableType>
  static Result<ArrowArrayStream> Scan(const std::unique_ptr<MetadataTableType>& table,
                                       const SnapshotSelection& selection = {}) {
    ICEBERG_ASSIGN_OR_RAISE(auto builder, table->NewScan());
    if (auto id = std::get_if<int64_t>(&selection.snapshot)) {
      builder->UseSnapshot(*id);
    } else if (auto time = std::get_if<TimePointMs>(&selection.snapshot)) {
      builder->AsOfTime(time->time_since_epoch().count());
    }
    if (!selection.ref_name.empty()) {
      builder->UseRef(selection.ref_name);
    }
    ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
    return scan->ToArrowArrayStream();
  }

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

  Status RefreshTableSchemaAndPartitionSpec(
      const std::shared_ptr<Table>& table, const std::shared_ptr<Snapshot>& first,
      std::shared_ptr<FileIO> refreshed_io = nullptr) {
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
                    refreshed_io != nullptr ? std::move(refreshed_io) : file_io_,
                    catalog_));
    EXPECT_CALL(*catalog_, LoadTable(::testing::_))
        .WillOnce(::testing::Return(refreshed));
    return table->Refresh();
  }

  Result<std::shared_ptr<Table>> MakeDecimalPartitionTable(
      const std::vector<int128_t>& values) {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeRequired(2, "amount", decimal(38, 2))});
    ICEBERG_ASSIGN_OR_RAISE(
        partitioned_spec_,
        PartitionSpec::Make(1,
                            {PartitionField(2, 1000, "amount", Transform::Identity())}));
    std::vector<ManifestEntry> entries;
    for (size_t index = 0; index < values.size(); ++index) {
      auto literal = Literal::Decimal(values[index], 38, 2);
      auto file = MakeDataFile(std::format("decimal-{}.parquet", index),
                               PartitionValues(literal), partitioned_spec_);
      ICEBERG_ASSIGN_OR_RAISE(auto bound, Conversions::ToBytes(literal));
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
    return MakeTable({snapshot}, 10, {}, partitioned_spec_);
  }

  std::shared_ptr<MockCatalog> catalog_;
};

}  // namespace iceberg
