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

#include "iceberg/inspect/partitions_table.h"

#include <bit>
#include <cstdint>
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
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/inspect/metadata_table_scan_test_base.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/util/conversions.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

class PartitionsTableTest : public MetadataTableScanTestBase {};

TEST_P(PartitionsTableTest, Scan) {
  auto snapshot = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"s3://bucket/data.parquet", PartitionValues(Literal::Int(7))}},
      partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));

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

TEST_P(PartitionsTableTest, RejectsCombiningNamedRefsWithTimeTravel) {
  auto first = MakeAppendSnapshot(GetParam(), 1, std::nullopt, 1, {"first.parquet"});
  auto second = MakeAppendSnapshot(GetParam(), 2, 1, 2, {"second.parquet"});
  ICEBERG_UNWRAP_OR_FAIL(auto branch, SnapshotRef::MakeBranch(1));
  ICEBERG_UNWRAP_OR_FAIL(auto tag, SnapshotRef::MakeTag(1));
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  refs.emplace("dev", std::move(branch));
  refs.emplace("release", std::move(tag));
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first, second}, 2, std::move(refs)));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table,
                         MetadataTable::Make<PartitionsTable>(table));
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

TEST_P(PartitionsTableTest, RejectsUnknownReference) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table,
                         MetadataTable::Make<PartitionsTable>(table));
  EXPECT_THAT(metadata_table->Scan(SnapshotSelection{.ref_name = "missing"}),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_P(PartitionsTableTest, ScansRefreshedSchemaAndPartitionSpec) {
  auto first = MakeAppendSnapshotWithPartitionValues(
      GetParam(), 10, std::nullopt, 1,
      {{"old.parquet", PartitionValues(Literal::Int(7))}}, partitioned_spec_);
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({first}, 10, {}, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto partitions, MetadataTable::Make<PartitionsTable>(table));
  auto old_partitions_schema = partitions->schema();
  ICEBERG_UNWRAP_OR_FAIL(auto old_stream, partitions->Scan());
  auto old_reader = ::arrow::ImportRecordBatchReader(&old_stream);
  ASSERT_TRUE(old_reader.ok()) << old_reader.status().ToString();

  ASSERT_THAT(RefreshTableSchemaAndPartitionSpec(table, first), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto partitions_stream, partitions->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto partition_batches,
                         ReadAllBatches(std::move(partitions_stream)));
  ASSERT_EQ(partition_batches.size(), 1);
  // Files that only differ in the newly added partition field must not merge.
  ASSERT_EQ(partition_batches[0]->num_rows(), 2);
  auto partition = std::static_pointer_cast<::arrow::StructArray>(
      partition_batches[0]->GetColumnByName("partition"));
  ASSERT_EQ(partition->num_fields(), 2);
  auto partition_ids = std::static_pointer_cast<::arrow::Int32Array>(partition->field(1));
  EXPECT_EQ(partition_ids->Value(0), 1);
  EXPECT_EQ(partition_ids->Value(1), 2);
  auto counts = std::static_pointer_cast<::arrow::Int32Array>(
      partition_batches[0]->GetColumnByName("file_count"));
  EXPECT_EQ(counts->Value(0), 1);
  EXPECT_EQ(counts->Value(1), 1);
  EXPECT_NE(partitions->schema(), old_partitions_schema);

  // Streams created before refresh retain their original metadata and schema.
  auto batches = (*old_reader)->ToRecordBatches();
  ASSERT_TRUE(batches.ok()) << batches.status().ToString();
  ASSERT_EQ(batches->size(), 1);
  EXPECT_EQ((*batches)[0]->num_rows(), 1);
  auto old_partition = std::static_pointer_cast<::arrow::StructArray>(
      (*batches)[0]->GetColumnByName("partition"));
  EXPECT_EQ(old_partition->num_fields(), 1);
}

TEST_P(PartitionsTableTest, ProjectsLiveFilesForPartitionAggregation) {
  auto data_file = MakeDataFile("data.parquet", PartitionValues(Literal::Int(7)),
                                partitioned_spec_, 3);
  data_file->column_sizes.emplace(1, 100);
  data_file->value_counts.emplace(1, 3);
  ICEBERG_UNWRAP_OR_FAIL(auto bound, Conversions::ToBytes(Literal::Int(42)));
  data_file->lower_bounds.emplace(1, bound);
  data_file->upper_bounds.emplace(1, bound);
  auto data = WriteDataManifest(GetParam(), 10,
                                {MakeEntry(ManifestStatus::kAdded, 10, 1, data_file)},
                                partitioned_spec_);
  auto delete_file = MakeDataFile("delete.parquet", PartitionValues(Literal::Int(7)),
                                  partitioned_spec_, 2);
  delete_file->content = DataFile::Content::kPositionDeletes;
  auto deletes = WriteDeleteManifest(
      GetParam(), 10, {MakeEntry(ManifestStatus::kAdded, 10, 1, delete_file)},
      partitioned_spec_);
  auto snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 10,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1000),
      .manifest_list = WriteManifestList(GetParam(), 10, 0, 1, {data, deletes}),
  });
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({snapshot}, 10, {}, partitioned_spec_));
  ICEBERG_UNWRAP_OR_FAIL(auto all_files, internal::LiveFiles(*table, snapshot));
  ICEBERG_UNWRAP_OR_FAIL(auto full_file, all_files->Next());
  ASSERT_TRUE(full_file.has_value());
  EXPECT_FALSE(full_file->file->column_sizes.empty());
  EXPECT_FALSE(full_file->file->lower_bounds.empty());

  ICEBERG_UNWRAP_OR_FAIL(auto projected_files,
                         internal::LiveFiles(*table, snapshot,
                                             {"content", "partition", "record_count",
                                              "file_size_in_bytes"}));
  ICEBERG_UNWRAP_OR_FAIL(auto projected_file, projected_files->Next());
  ASSERT_TRUE(projected_file.has_value());
  EXPECT_TRUE(projected_file->file->column_sizes.empty());
  EXPECT_TRUE(projected_file->file->value_counts.empty());
  EXPECT_TRUE(projected_file->file->lower_bounds.empty());
  EXPECT_TRUE(projected_file->file->upper_bounds.empty());
  EXPECT_EQ(projected_file->file->record_count, 3);
  EXPECT_EQ(projected_file->snapshot_id, 10);

  ICEBERG_UNWRAP_OR_FAIL(auto partitions, MetadataTable::Make<PartitionsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, partitions->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_EQ(batches[0]->num_rows(), 1);
  for (const auto& [name, expected] :
       std::vector<std::pair<std::string, int64_t>>{{"record_count", 3},
                                                    {"total_data_file_size_in_bytes", 10},
                                                    {"position_delete_record_count", 2},
                                                    {"last_updated_snapshot_id", 10}}) {
    auto values =
        std::static_pointer_cast<::arrow::Int64Array>(batches[0]->GetColumnByName(name));
    EXPECT_EQ(values->Value(0), expected) << name;
  }
}

TEST_P(PartitionsTableTest, GroupsNullPartitionValues) {
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

TEST_P(PartitionsTableTest, GroupsAllNaNsButDistinguishesSignedZeros) {
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

TEST_P(PartitionsTableTest, ScansDecimalPartitions) {
  const int128_t large = (static_cast<int128_t>(1) << 80) + 123;
  const std::vector<int128_t> values{123, -456, large, -large, 0};
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeDecimalPartitionTable(values));
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

TEST_P(PartitionsTableTest, ScanEmpty) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MakeTable({}, kInvalidSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata_table,
                         MetadataTable::Make<PartitionsTable>(table));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, metadata_table->Scan());
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  EXPECT_TRUE(batches.empty());
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, PartitionsTableTest, ::testing::Values(2, 3));

}  // namespace
}  // namespace iceberg
