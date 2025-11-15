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

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_writer.h"
#include "iceberg/metrics.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {

constexpr int64_t kSequenceNumber = 34L;
constexpr int64_t kSnapshotId = 987134631982734L;
constexpr std::string_view kPath =
    "s3://bucket/table/category=cheesy/timestamp_hour=10/id_bucket=3/file.avro";
constexpr FileFormatType kFormat = FileFormatType::kAvro;
constexpr int32_t kSortOrderId = 2;
constexpr int64_t kFirstRowId = 100L;

const std::vector<Literal> kPartition = {Literal::String("cheesy"), Literal::Int(10),
                                         Literal::Int(3)};
const std::vector<int32_t> kEqualityIds = {1};

const auto kMetrics = Metrics{
    .row_count = 1587L,
    .column_sizes = {{1, 15L}, {2, 122L}, {3, 4021L}, {4, 9411L}, {5, 15L}},
    .value_counts = {{1, 100L}, {2, 100L}, {3, 100L}, {4, 100L}, {5, 100L}},
    .null_value_counts = {{1, 0L}, {2, 0L}, {3, 0L}, {4, 0L}, {5, 0L}},
    .nan_value_counts = {{5, 10L}},
    .lower_bounds = {{1, Literal::Int(1)}},
    .upper_bounds = {{1, Literal::Int(1)}},
};

const auto kOffsets = std::vector<int64_t>{4L};

std::unique_ptr<DataFile> CreateDataFile() {
  auto data_file = std::make_unique<DataFile>();
  data_file->file_path = std::string(kPath);
  data_file->file_format = kFormat;
  data_file->partition = kPartition;
  data_file->file_size_in_bytes = 150972L;
  data_file->split_offsets = kOffsets;
  data_file->sort_order_id = kSortOrderId;
  data_file->first_row_id = kFirstRowId;
  data_file->column_sizes = {kMetrics.column_sizes.begin(), kMetrics.column_sizes.end()};
  data_file->value_counts = {kMetrics.value_counts.begin(), kMetrics.value_counts.end()};
  data_file->null_value_counts = {kMetrics.null_value_counts.begin(),
                                  kMetrics.null_value_counts.end()};
  data_file->nan_value_counts = {kMetrics.nan_value_counts.begin(),
                                 kMetrics.nan_value_counts.end()};

  for (const auto& [col_id, bound] : kMetrics.lower_bounds) {
    data_file->lower_bounds[col_id] = bound.Serialize().value();
  }
  for (const auto& [col_id, bound] : kMetrics.upper_bounds) {
    data_file->upper_bounds[col_id] = bound.Serialize().value();
  }

  return data_file;
}

std::unique_ptr<DataFile> CreateDeleteFile() {
  auto delete_file = std::make_unique<DataFile>();
  delete_file->content = DataFile::Content::kEqualityDeletes;
  delete_file->file_path = std::string(kPath);
  delete_file->file_format = kFormat;
  delete_file->partition = kPartition;
  delete_file->file_size_in_bytes = 22905L;
  delete_file->equality_ids = kEqualityIds;

  delete_file->column_sizes = {kMetrics.column_sizes.begin(),
                               kMetrics.column_sizes.end()};
  delete_file->value_counts = {kMetrics.value_counts.begin(),
                               kMetrics.value_counts.end()};
  delete_file->null_value_counts = {kMetrics.null_value_counts.begin(),
                                    kMetrics.null_value_counts.end()};
  delete_file->nan_value_counts = {kMetrics.nan_value_counts.begin(),
                                   kMetrics.nan_value_counts.end()};

  for (const auto& [col_id, bound] : kMetrics.lower_bounds) {
    delete_file->lower_bounds[col_id] = bound.Serialize().value();
  }
  for (const auto& [col_id, bound] : kMetrics.upper_bounds) {
    delete_file->upper_bounds[col_id] = bound.Serialize().value();
  }

  return delete_file;
}

}  // namespace

class ManifestWriterVersionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "id", int64()),
        SchemaField::MakeRequired(2, "timestamp", timestamp_tz()),
        SchemaField::MakeRequired(3, "category", string()),
        SchemaField::MakeRequired(4, "data", string()),
        SchemaField::MakeRequired(5, "double", float64())});
    spec_ = PartitionSpec::Make(
                0, {PartitionField(3, 1000, "category", Transform::Identity()),
                    PartitionField(2, 1001, "timestamp_hour", Transform::Hour()),
                    PartitionField(1, 1002, "id_bucket", Transform::Bucket(16))})
                .value();

    // Initialize a DataFile
    data_file_ = CreateDataFile();

    file_io_ = iceberg::arrow::MakeMockFileIO();
  }

  std::shared_ptr<Schema> schema_{nullptr};
  std::unique_ptr<PartitionSpec> spec_{nullptr};
  std::unique_ptr<DataFile> data_file_{nullptr};
  std::unique_ptr<DataFile> delete_file_{nullptr};

  std::shared_ptr<FileIO> file_io_{nullptr};
};

}  // namespace iceberg
