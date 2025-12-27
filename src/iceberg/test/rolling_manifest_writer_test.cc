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

#include "iceberg/manifest/rolling_manifest_writer.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {

constexpr int64_t kSequenceNumber = 34L;
constexpr int64_t kSnapshotId = 987134631982734L;
constexpr std::string_view kPath =
    "s3://bucket/table/category=cheesy/timestamp_hour=10/id_bucket=3/file.avro";
constexpr FileFormatType kFormat = FileFormatType::kAvro;
constexpr int64_t kFirstRowId = 100L;
constexpr int64_t kFileSizeCheckRowsDivisor = 250;
constexpr int64_t kSmallFileSize = 10L;

const PartitionValues kPartition =
    PartitionValues({Literal::String("cheesy"), Literal::Int(10), Literal::Int(3)});

std::shared_ptr<DataFile> CreateDataFile(int64_t record_count) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = std::format("data_bucket=0/file-{}.parquet", record_count);
  data_file->file_format = FileFormatType::kParquet;
  data_file->partition = kPartition;
  data_file->file_size_in_bytes = 1024;
  data_file->record_count = record_count;
  return data_file;
}

std::shared_ptr<DataFile> CreateDeleteFile(int64_t record_count) {
  auto delete_file = std::make_shared<DataFile>();
  delete_file->content = DataFile::Content::kPositionDeletes;
  delete_file->file_path = std::format("/path/to/delete-{}.parquet", record_count);
  delete_file->file_format = FileFormatType::kParquet;
  delete_file->partition = kPartition;
  delete_file->file_size_in_bytes = 10;
  delete_file->record_count = record_count;
  return delete_file;
}

static std::string CreateManifestPath() {
  return std::format("manifest-{}.avro",
                     std::chrono::system_clock::now().time_since_epoch().count());
}

}  // namespace

class RollingManifestWriterTest : public ::testing::TestWithParam<int32_t> {
 protected:
  void SetUp() override {
    avro::RegisterAll();
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

    file_io_ = iceberg::arrow::MakeMockFileIO();
  }

  RollingManifestWriter::ManifestWriterFactory NewRollingWriteManifestFactory(
      int32_t format_version) {
    return [this, format_version]() -> Result<std::unique_ptr<ManifestWriter>> {
      const std::string manifest_path = CreateManifestPath();
      Result<std::unique_ptr<ManifestWriter>> writer_result =
          NotSupported("Format version: {}", format_version);

      if (format_version == 1) {
        writer_result = ManifestWriter::MakeV1Writer(kSnapshotId, manifest_path, file_io_,
                                                     spec_, schema_);
      } else if (format_version == 2) {
        writer_result = ManifestWriter::MakeV2Writer(
            kSnapshotId, manifest_path, file_io_, spec_, schema_, ManifestContent::kData);
      } else if (format_version == 3) {
        writer_result = ManifestWriter::MakeV3Writer(kSnapshotId, kFirstRowId,
                                                     manifest_path, file_io_, spec_,
                                                     schema_, ManifestContent::kData);
      }

      return writer_result;
    };
  }

  RollingManifestWriter::ManifestWriterFactory NewRollingWriteDeleteManifestFactory(
      int32_t format_version) {
    return [this, format_version]() -> Result<std::unique_ptr<ManifestWriter>> {
      const std::string manifest_path = CreateManifestPath();
      Result<std::unique_ptr<ManifestWriter>> writer_result =
          NotSupported("Format version: {}", format_version);

      if (format_version == 2) {
        writer_result =
            ManifestWriter::MakeV2Writer(kSnapshotId, manifest_path, file_io_, spec_,
                                         schema_, ManifestContent::kDeletes);
      } else if (format_version == 3) {
        writer_result = ManifestWriter::MakeV3Writer(kSnapshotId, kFirstRowId,
                                                     manifest_path, file_io_, spec_,
                                                     schema_, ManifestContent::kDeletes);
      }

      return writer_result;
    };
  }

  void CheckManifests(const std::vector<ManifestFile>& manifests,
                      const std::vector<int32_t>& added_file_counts,
                      const std::vector<int32_t>& existing_file_counts,
                      const std::vector<int32_t>& deleted_file_counts,
                      const std::vector<int64_t>& added_row_counts,
                      const std::vector<int64_t>& existing_row_counts,
                      const std::vector<int64_t>& deleted_row_counts) {
    ASSERT_EQ(manifests.size(), added_file_counts.size());
    for (size_t i = 0; i < manifests.size(); i++) {
      const ManifestFile& manifest = manifests[i];
      EXPECT_TRUE(manifest.has_added_files());
      EXPECT_EQ(manifest.added_files_count.value_or(0), added_file_counts[i]);
      EXPECT_EQ(manifest.added_rows_count.value_or(0), added_row_counts[i]);

      EXPECT_TRUE(manifest.has_existing_files());
      EXPECT_EQ(manifest.existing_files_count.value_or(0), existing_file_counts[i]);
      EXPECT_EQ(manifest.existing_rows_count.value_or(0), existing_row_counts[i]);

      EXPECT_TRUE(manifest.has_deleted_files());
      EXPECT_EQ(manifest.deleted_files_count.value_or(0), deleted_file_counts[i]);
      EXPECT_EQ(manifest.deleted_rows_count.value_or(0), deleted_row_counts[i]);
    }
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<FileIO> file_io_;
};

TEST_P(RollingManifestWriterTest, TestRollingManifestWriterNoRecords) {
  int32_t format_version = GetParam();
  RollingManifestWriter writer(NewRollingWriteManifestFactory(format_version),
                               kSmallFileSize);

  EXPECT_THAT(writer.Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, writer.ToManifestFiles());
  EXPECT_TRUE(manifest_files.empty());

  // Test that calling close again doesn't change the result
  EXPECT_THAT(writer.Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(manifest_files, writer.ToManifestFiles());
  EXPECT_TRUE(manifest_files.empty());
}

TEST_P(RollingManifestWriterTest, TestRollingDeleteManifestWriterNoRecords) {
  int32_t format_version = GetParam();
  if (format_version < 2) {
    GTEST_SKIP() << "Delete manifests only supported in V2+";
  }
  RollingManifestWriter writer(NewRollingWriteDeleteManifestFactory(format_version),
                               kSmallFileSize);

  EXPECT_THAT(writer.Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, writer.ToManifestFiles());
  EXPECT_TRUE(manifest_files.empty());

  // Test that calling close again doesn't change the result
  EXPECT_THAT(writer.Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(manifest_files, writer.ToManifestFiles());
  EXPECT_TRUE(manifest_files.empty());
}

TEST_P(RollingManifestWriterTest, TestRollingManifestWriterSplitFiles) {
  int32_t format_version = GetParam();
  RollingManifestWriter writer(NewRollingWriteManifestFactory(format_version),
                               kSmallFileSize);

  std::vector<int32_t> added_file_counts(3, 0);
  std::vector<int32_t> existing_file_counts(3, 0);
  std::vector<int32_t> deleted_file_counts(3, 0);
  std::vector<int64_t> added_row_counts(3, 0);
  std::vector<int64_t> existing_row_counts(3, 0);
  std::vector<int64_t> deleted_row_counts(3, 0);

  // Write 750 entries (3 * 250) to trigger 3 file splits
  for (int32_t i = 0; i < kFileSizeCheckRowsDivisor * 3; i++) {
    int32_t type = i % 3;
    int32_t file_index = i / kFileSizeCheckRowsDivisor;
    auto data_file = CreateDataFile(i);

    if (type == 0) {
      EXPECT_THAT(writer.WriteAddedEntry(data_file), IsOk());
      added_file_counts[file_index] += 1;
      added_row_counts[file_index] += i;
    } else if (type == 1) {
      EXPECT_THAT(writer.WriteExistingEntry(data_file, 1, 1, std::nullopt), IsOk());
      existing_file_counts[file_index] += 1;
      existing_row_counts[file_index] += i;
    } else {
      EXPECT_THAT(writer.WriteDeletedEntry(data_file, 1, std::nullopt), IsOk());
      deleted_file_counts[file_index] += 1;
      deleted_row_counts[file_index] += i;
    }
  }

  EXPECT_THAT(writer.Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, writer.ToManifestFiles());
  EXPECT_EQ(manifest_files.size(), 3);

  CheckManifests(manifest_files, added_file_counts, existing_file_counts,
                 deleted_file_counts, added_row_counts, existing_row_counts,
                 deleted_row_counts);

  // Test that calling close again doesn't change the result
  EXPECT_THAT(writer.Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(manifest_files, writer.ToManifestFiles());
  EXPECT_EQ(manifest_files.size(), 3);

  CheckManifests(manifest_files, added_file_counts, existing_file_counts,
                 deleted_file_counts, added_row_counts, existing_row_counts,
                 deleted_row_counts);
}

TEST_P(RollingManifestWriterTest, TestRollingDeleteManifestWriterSplitFiles) {
  int32_t format_version = GetParam();
  if (format_version < 2) {
    GTEST_SKIP() << "Delete manifests only supported in V2+";
  }
  RollingManifestWriter writer(NewRollingWriteDeleteManifestFactory(format_version),
                               kSmallFileSize);

  std::vector<int32_t> added_file_counts(3, 0);
  std::vector<int32_t> existing_file_counts(3, 0);
  std::vector<int32_t> deleted_file_counts(3, 0);
  std::vector<int64_t> added_row_counts(3, 0);
  std::vector<int64_t> existing_row_counts(3, 0);
  std::vector<int64_t> deleted_row_counts(3, 0);

  // Write 750 entries (3 * 250) to trigger 3 file splits
  for (int32_t i = 0; i < 3 * kFileSizeCheckRowsDivisor; i++) {
    int32_t type = i % 3;
    int32_t file_index = i / kFileSizeCheckRowsDivisor;
    auto delete_file = CreateDeleteFile(i);

    if (type == 0) {
      EXPECT_THAT(writer.WriteAddedEntry(delete_file), IsOk());
      added_file_counts[file_index] += 1;
      added_row_counts[file_index] += i;
    } else if (type == 1) {
      EXPECT_THAT(writer.WriteExistingEntry(delete_file, 1, 1, std::nullopt), IsOk());
      existing_file_counts[file_index] += 1;
      existing_row_counts[file_index] += i;
    } else {
      EXPECT_THAT(writer.WriteDeletedEntry(delete_file, 1, std::nullopt), IsOk());
      deleted_file_counts[file_index] += 1;
      deleted_row_counts[file_index] += i;
    }
  }

  EXPECT_THAT(writer.Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto manifest_files, writer.ToManifestFiles());
  EXPECT_EQ(manifest_files.size(), 3);

  CheckManifests(manifest_files, added_file_counts, existing_file_counts,
                 deleted_file_counts, added_row_counts, existing_row_counts,
                 deleted_row_counts);

  // Test that calling close again doesn't change the result
  EXPECT_THAT(writer.Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(manifest_files, writer.ToManifestFiles());
  EXPECT_EQ(manifest_files.size(), 3);

  CheckManifests(manifest_files, added_file_counts, existing_file_counts,
                 deleted_file_counts, added_row_counts, existing_row_counts,
                 deleted_row_counts);
}

INSTANTIATE_TEST_SUITE_P(TestRollingManifestWriter, RollingManifestWriterTest,
                         ::testing::Values(1, 2, 3));

}  // namespace iceberg
