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

#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include "iceberg/row/partition_values.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/util/conversions.h"

namespace iceberg {

class MetadataTableScanTestBase : public ScanTestBase {
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
        auto refreshed, Table::Make(table->name(), std::move(metadata),
                                    "s3://bucket/refreshed.json", file_io_, catalog_));
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
