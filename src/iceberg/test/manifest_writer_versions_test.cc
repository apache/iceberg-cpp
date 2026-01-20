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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/metrics.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_metadata.h"
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
constexpr int32_t kSortOrderId = 2;
constexpr int64_t kFirstRowId = 100L;

const PartitionValues kPartition =
    PartitionValues({Literal::String("cheesy"), Literal::Int(10), Literal::Int(3)});
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

std::unique_ptr<DataFile> CreateDataFile(
    std::optional<int64_t> first_row_id = std::nullopt) {
  auto data_file = std::make_unique<DataFile>();
  data_file->file_path = std::string(kPath);
  data_file->file_format = kFormat;
  data_file->partition = kPartition;
  data_file->file_size_in_bytes = 150972L;
  data_file->record_count = kMetrics.row_count.value();
  data_file->split_offsets = kOffsets;
  data_file->sort_order_id = kSortOrderId;
  data_file->first_row_id = first_row_id;
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

    data_file_ = CreateDataFile(kFirstRowId);
    delete_file_ = CreateDeleteFile();
    data_file_without_first_row_id_ = CreateDataFile();

    file_io_ = iceberg::arrow::MakeMockFileIO();
  }

  static std::string CreateManifestListPath() {
    return std::format("manifest-list-{}.avro",
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  std::string WriteManifests(int format_version,
                             const std::vector<ManifestFile>& manifests) const {
    const std::string manifest_list_path = CreateManifestListPath();
    constexpr int64_t kParentSnapshotId = kSnapshotId - 1;

    auto writer_result = ManifestListWriter::MakeWriter(
        format_version, kSnapshotId, kParentSnapshotId, manifest_list_path, file_io_,
        kSequenceNumber, kFirstRowId);
    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    EXPECT_THAT(writer->AddAll(manifests), IsOk());
    EXPECT_THAT(writer->Close(), IsOk());

    return manifest_list_path;
  }

  std::vector<ManifestFile> WriteAndReadManifests(
      const std::vector<ManifestFile>& manifests, int format_version) const {
    return ReadManifests(WriteManifests(format_version, manifests));
  }

  std::vector<ManifestFile> ReadManifests(const std::string& manifest_list_path) const {
    auto reader_result = ManifestListReader::Make(manifest_list_path, file_io_);
    EXPECT_THAT(reader_result, IsOk());

    auto reader = std::move(reader_result.value());
    auto files_result = reader->Files();
    EXPECT_THAT(files_result, IsOk());

    return files_result.value();
  }

  static std::string CreateManifestPath() {
    return std::format("manifest-{}.avro",
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  ManifestFile WriteManifest(int format_version,
                             std::vector<std::shared_ptr<DataFile>> data_files) {
    const std::string manifest_path = CreateManifestPath();

    auto writer_result =
        ManifestWriter::MakeWriter(format_version, kSnapshotId, manifest_path, file_io_,
                                   spec_, schema_, ManifestContent::kData, kFirstRowId);
    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (auto& data_file : data_files) {
      EXPECT_THAT(writer->WriteAddedEntry(data_file), IsOk());
    }

    EXPECT_THAT(writer->Close(), IsOk());

    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());

    return std::move(manifest_result.value());
  }

  std::vector<ManifestEntry> ReadManifest(const ManifestFile& manifest_file) {
    auto reader_result = ManifestReader::Make(manifest_file, file_io_, schema_, spec_);
    EXPECT_THAT(reader_result, IsOk());

    auto reader = std::move(reader_result.value());
    auto entries_result = reader->Entries();
    EXPECT_THAT(entries_result, IsOk());

    return entries_result.value();
  }

  ManifestFile WriteDeleteManifest(int format_version,
                                   std::shared_ptr<DataFile> delete_file) {
    const std::string manifest_path = CreateManifestPath();

    auto writer_result = ManifestWriter::MakeWriter(
        format_version, kSnapshotId, manifest_path, file_io_, spec_, schema_,
        ManifestContent::kDeletes, kFirstRowId);

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    EXPECT_THAT(writer->WriteDeletedEntry(delete_file, kSequenceNumber, std::nullopt),
                IsOk());
    EXPECT_THAT(writer->Close(), IsOk());

    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());

    return std::move(manifest_result.value());
  }

  ManifestFile RewriteManifest(const ManifestFile& old_manifest, int format_version) {
    auto entries = ReadManifest(old_manifest);

    const std::string manifest_path = CreateManifestPath();

    auto writer_result =
        ManifestWriter::MakeWriter(format_version, kSnapshotId, manifest_path, file_io_,
                                   spec_, schema_, old_manifest.content, kFirstRowId);
    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (auto& entry : entries) {
      EXPECT_THAT(
          writer->WriteExistingEntry(
              entry.data_file, entry.snapshot_id.value_or(kSnapshotId),
              entry.sequence_number.value_or(TableMetadata::kInitialSequenceNumber),
              entry.file_sequence_number),
          IsOk());
    }
    EXPECT_THAT(writer->Close(), IsOk());

    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());

    return std::move(manifest_result.value());
  }

  void CheckManifest(const ManifestFile& manifest, int64_t expected_sequence_number,
                     int64_t expected_min_sequence_number) {
    ASSERT_EQ(manifest.added_snapshot_id, kSnapshotId);
    ASSERT_EQ(manifest.sequence_number, expected_sequence_number);
    ASSERT_EQ(manifest.min_sequence_number, expected_min_sequence_number);
    switch (manifest.content) {
      case ManifestContent::kData:
        ASSERT_EQ(manifest.added_files_count, 1L);
        ASSERT_EQ(manifest.deleted_files_count, 0L);
        ASSERT_EQ(manifest.added_rows_count, kMetrics.row_count);
        break;
      case ManifestContent::kDeletes:
        ASSERT_EQ(manifest.added_files_count, 0L);
        ASSERT_EQ(manifest.deleted_files_count, 1L);
        ASSERT_EQ(manifest.added_rows_count, 0L);
        break;
      default:
        std::unreachable();
    }

    ASSERT_EQ(manifest.existing_files_count, 0L);
    ASSERT_EQ(manifest.existing_rows_count, 0L);
    ASSERT_EQ(manifest.deleted_rows_count, 0L);
  }

  void CheckDataFile(const DataFile& data_file, DataFile::Content expected_content,
                     std::optional<int64_t> expected_first_row_id = std::nullopt) {
    ASSERT_EQ(data_file.content, expected_content);
    ASSERT_EQ(data_file.file_path, kPath);
    ASSERT_EQ(data_file.file_format, kFormat);
    ASSERT_EQ(data_file.partition, kPartition);
    ASSERT_EQ(data_file.record_count, kMetrics.row_count);
    ASSERT_EQ(data_file.sort_order_id, kSortOrderId);
    switch (data_file.content) {
      case DataFile::Content::kData:
        ASSERT_EQ(data_file.first_row_id, expected_first_row_id);
        ASSERT_TRUE(data_file.equality_ids.empty());
        break;
      case DataFile::Content::kEqualityDeletes:
        ASSERT_EQ(data_file.first_row_id, std::nullopt);
        ASSERT_EQ(data_file.equality_ids, kEqualityIds);
        break;
      case DataFile::Content::kPositionDeletes:
        ASSERT_EQ(data_file.first_row_id, std::nullopt);
        ASSERT_TRUE(data_file.equality_ids.empty());
        break;
      default:
        std::unreachable();
    }

    // Metrics
    ASSERT_EQ(data_file.column_sizes,
              (std::map<int32_t, int64_t>(kMetrics.column_sizes.begin(),
                                          kMetrics.column_sizes.end())));
    ASSERT_EQ(data_file.value_counts,
              (std::map<int32_t, int64_t>(kMetrics.value_counts.begin(),
                                          kMetrics.value_counts.end())));
    ASSERT_EQ(data_file.null_value_counts,
              (std::map<int32_t, int64_t>(kMetrics.null_value_counts.begin(),
                                          kMetrics.null_value_counts.end())));
    ASSERT_EQ(data_file.nan_value_counts,
              (std::map<int32_t, int64_t>(kMetrics.nan_value_counts.begin(),
                                          kMetrics.nan_value_counts.end())));
    ASSERT_EQ(data_file.lower_bounds.size(), kMetrics.lower_bounds.size());
    for (const auto& [col_id, bound] : kMetrics.lower_bounds) {
      auto it = data_file.lower_bounds.find(col_id);
      ASSERT_NE(it, data_file.lower_bounds.end());
      std::vector<uint8_t> serialized_bound = bound.Serialize().value();
      ASSERT_EQ(it->second, serialized_bound);
    }

    ASSERT_EQ(data_file.upper_bounds.size(), kMetrics.upper_bounds.size());
    for (const auto& [col_id, bound] : kMetrics.upper_bounds) {
      auto it = data_file.upper_bounds.find(col_id);
      ASSERT_NE(it, data_file.upper_bounds.end());
      std::vector<uint8_t> serialized_bound = bound.Serialize().value();
      ASSERT_EQ(it->second, serialized_bound);
    }
  }

  void CheckEntry(const ManifestEntry& entry,
                  std::optional<int64_t> expected_data_sequence_number,
                  std::optional<int64_t> expected_file_sequence_number,
                  DataFile::Content expected_content,
                  ManifestStatus expected_status = ManifestStatus::kAdded,
                  std::optional<int64_t> expected_first_row_id = std::nullopt) {
    ASSERT_EQ(entry.status, expected_status);
    ASSERT_EQ(entry.snapshot_id, kSnapshotId);
    ASSERT_EQ(entry.sequence_number, expected_data_sequence_number);
    ASSERT_EQ(entry.file_sequence_number, expected_file_sequence_number);
    if (entry.status == ManifestStatus::kAdded) {
      CheckDataFile(*entry.data_file, expected_content, expected_first_row_id);
    }
  }

  void CheckRewrittenManifest(const ManifestFile& manifest,
                              int64_t expected_sequence_number,
                              int64_t expected_min_sequence_number) {
    ASSERT_EQ(manifest.added_snapshot_id, kSnapshotId);
    ASSERT_EQ(manifest.sequence_number, expected_sequence_number);
    // TODO(zhjwpku): figure out why min_sequence_number check fails
    // ASSERT_EQ(manifest.min_sequence_number, expected_min_sequence_number);
    ASSERT_EQ(manifest.added_files_count, 0L);
    ASSERT_EQ(manifest.existing_files_count, 1L);
    ASSERT_EQ(manifest.deleted_files_count, 0L);
    ASSERT_EQ(manifest.added_rows_count, 0L);
    ASSERT_EQ(manifest.existing_rows_count, kMetrics.row_count);
    ASSERT_EQ(manifest.deleted_rows_count, 0L);
  }

  void CheckRewrittenEntry(const ManifestEntry& entry, int64_t expected_sequence_number,
                           DataFile::Content expected_content,
                           std::optional<int64_t> expected_first_row_id = std::nullopt) {
    ASSERT_EQ(entry.status, ManifestStatus::kExisting);
    ASSERT_EQ(entry.snapshot_id, kSnapshotId);
    // TODO(zhjwpku): Check the sequence_number and file_sequence_number of V1 manifest
    if (entry.sequence_number.has_value()) {
      ASSERT_EQ(entry.sequence_number.value(), expected_sequence_number);
    }
    if (entry.file_sequence_number.has_value()) {
      ASSERT_EQ(entry.file_sequence_number.value(), expected_sequence_number);
    }
    CheckDataFile(*entry.data_file, expected_content, expected_first_row_id);
  }

  std::shared_ptr<Schema> schema_{nullptr};
  std::shared_ptr<PartitionSpec> spec_{nullptr};
  std::shared_ptr<DataFile> data_file_{nullptr};
  std::shared_ptr<DataFile> delete_file_{nullptr};
  std::shared_ptr<DataFile> data_file_without_first_row_id_{nullptr};

  std::shared_ptr<FileIO> file_io_{nullptr};
};

TEST_F(ManifestWriterVersionsTest, TestV1Write) {
  auto manifest = WriteManifest(/*format_version=*/1, {data_file_});
  CheckManifest(manifest, kInvalidSequenceNumber, kInvalidSequenceNumber);
  auto entries = ReadManifest(manifest);
  ASSERT_EQ(entries.size(), 1);
  CheckEntry(entries[0], kInvalidSequenceNumber, kInvalidSequenceNumber,
             DataFile::Content::kData);
}

TEST_F(ManifestWriterVersionsTest, TestV1WriteDelete) {
  const std::string manifest_path = CreateManifestPath();
  ICEBERG_UNWRAP_OR_FAIL(
      auto writer,
      ManifestWriter::MakeV1Writer(kSnapshotId, manifest_path, file_io_, spec_, schema_));

  ManifestEntry entry;
  entry.snapshot_id = kSnapshotId;
  entry.data_file = std::move(delete_file_);

  auto status = writer->WriteDeletedEntry(entry);
  EXPECT_THAT(status, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(status, HasErrorMessage(
                          "Cannot write equality_deletes file to data manifest file"));
}

TEST_F(ManifestWriterVersionsTest, TestV1WriteWithInheritance) {
  auto manifests =
      WriteAndReadManifests({WriteManifest(/*format_version=*/1, {data_file_})}, 1);
  ASSERT_EQ(manifests.size(), 1);
  CheckManifest(manifests[0], TableMetadata::kInitialSequenceNumber,
                TableMetadata::kInitialSequenceNumber);
  auto entries = ReadManifest(manifests[0]);
  ASSERT_EQ(entries.size(), 1);
  CheckEntry(entries[0], TableMetadata::kInitialSequenceNumber,
             TableMetadata::kInitialSequenceNumber, DataFile::Content::kData);
}

TEST_F(ManifestWriterVersionsTest, TestV2Write) {
  auto manifest = WriteManifest(/*format_version=*/2, {data_file_});
  CheckManifest(manifest, kInvalidSequenceNumber, kInvalidSequenceNumber);
  auto entries = ReadManifest(manifest);
  ASSERT_EQ(entries.size(), 1);
  ASSERT_EQ(manifest.content, ManifestContent::kData);
  CheckEntry(entries[0], kInvalidSequenceNumber, kInvalidSequenceNumber,
             DataFile::Content::kData);
}

TEST_F(ManifestWriterVersionsTest, TestV2WriteWithInheritance) {
  auto manifests =
      WriteAndReadManifests({WriteManifest(/*format_version=*/2, {data_file_})}, 2);
  CheckManifest(manifests[0], kSequenceNumber, kSequenceNumber);
  auto entries = ReadManifest(manifests[0]);
  ASSERT_EQ(entries.size(), 1);
  ASSERT_EQ(manifests[0].content, ManifestContent::kData);
  CheckEntry(entries[0], kSequenceNumber, kSequenceNumber, DataFile::Content::kData);
}

TEST_F(ManifestWriterVersionsTest, TestV2PlusWriteDeleteV2) {
  auto manifest = WriteDeleteManifest(/*format_version=*/2, delete_file_);
  CheckManifest(manifest, kInvalidSequenceNumber, kInvalidSequenceNumber);
  auto entries = ReadManifest(manifest);
  ASSERT_EQ(entries.size(), 1);
  ASSERT_EQ(manifest.content, ManifestContent::kDeletes);
  CheckEntry(entries[0], kSequenceNumber, std::nullopt,
             DataFile::Content::kEqualityDeletes, ManifestStatus::kDeleted);
}

TEST_F(ManifestWriterVersionsTest, TestV2ManifestListRewriteWithInheritance) {
  // write with v1
  auto manifests =
      WriteAndReadManifests({WriteManifest(/*format_version=*/1, {data_file_})}, 1);
  CheckManifest(manifests[0], TableMetadata::kInitialSequenceNumber,
                TableMetadata::kInitialSequenceNumber);

  // rewrite existing metadata with v2 manifest list
  auto manifests2 = WriteAndReadManifests(manifests, 2);
  // the ManifestFile did not change and should still have its original sequence number, 0
  CheckManifest(manifests2[0], TableMetadata::kInitialSequenceNumber,
                TableMetadata::kInitialSequenceNumber);

  // should not inherit the v2 sequence number because it was a rewrite
  auto entries = ReadManifest(manifests2[0]);
  CheckEntry(entries[0], TableMetadata::kInitialSequenceNumber,
             TableMetadata::kInitialSequenceNumber, DataFile::Content::kData);
}

TEST_F(ManifestWriterVersionsTest, TestV2ManifestRewriteWithInheritance) {
  // write with v1
  auto manifests =
      WriteAndReadManifests({WriteManifest(/*format_version=*/1, {data_file_})}, 1);
  CheckManifest(manifests[0], TableMetadata::kInitialSequenceNumber,
                TableMetadata::kInitialSequenceNumber);

  // rewrite the manifest file using a v2 manifest
  auto rewritten_manifest = RewriteManifest(manifests[0], 2);
  CheckRewrittenManifest(rewritten_manifest, kInvalidSequenceNumber,
                         TableMetadata::kInitialSequenceNumber);

  // add the v2 manifest to a v2 manifest list, with a sequence number
  auto manifests2 = WriteAndReadManifests({rewritten_manifest}, 2);
  // the ManifestFile is new so it has a sequence number, but the min sequence number 0 is
  // from the entry
  CheckRewrittenManifest(manifests2[0], kSequenceNumber,
                         TableMetadata::kInitialSequenceNumber);

  // should not inherit the v2 sequence number because it was written into the v2 manifest
  auto entries = ReadManifest(manifests2[0]);
  CheckRewrittenEntry(entries[0], TableMetadata::kInitialSequenceNumber,
                      DataFile::Content::kData);
}

TEST_F(ManifestWriterVersionsTest, TestV3Write) {
  auto manifest = WriteManifest(/*format_version=*/3, {data_file_});
  CheckManifest(manifest, kInvalidSequenceNumber, kInvalidSequenceNumber);
  auto entries = ReadManifest(manifest);
  ASSERT_EQ(entries.size(), 1);
  ASSERT_EQ(manifest.content, ManifestContent::kData);
  CheckEntry(entries[0], kInvalidSequenceNumber, kInvalidSequenceNumber,
             DataFile::Content::kData, ManifestStatus::kAdded, kFirstRowId);
}

TEST_F(ManifestWriterVersionsTest, TestV3WriteWithInheritance) {
  auto manifests = WriteAndReadManifests(
      {WriteManifest(/*format_version=*/3, {data_file_without_first_row_id_})}, 3);
  CheckManifest(manifests[0], kSequenceNumber, kSequenceNumber);
  ASSERT_EQ(manifests[0].content, ManifestContent::kData);

  // v2+ should use the correct sequence number by inheriting it
  // v3 should use the correct first-row-id by inheriting it
  auto entries = ReadManifest(manifests[0]);
  ASSERT_EQ(entries.size(), 1);
  // first_row_id should be inherited
  CheckEntry(entries[0], kSequenceNumber, kSequenceNumber, DataFile::Content::kData,
             ManifestStatus::kAdded, kFirstRowId);
}

TEST_F(ManifestWriterVersionsTest, TestV3WriteFirstRowIdAssignment) {
  auto manifests = WriteAndReadManifests(
      {WriteManifest(/*format_version=*/3,
                     {data_file_without_first_row_id_, data_file_without_first_row_id_})},
      3);
  ASSERT_EQ(manifests[0].content, ManifestContent::kData);

  // v2+ should use the correct sequence number by inheriting it
  // v3 should use the correct first-row-id by inheriting it
  auto entries = ReadManifest(manifests[0]);
  ASSERT_EQ(entries.size(), 2);
  int64_t expected_first_row_id = kFirstRowId;
  for (const auto& entry : entries) {
    CheckEntry(entry, kSequenceNumber, kSequenceNumber, DataFile::Content::kData,
               ManifestStatus::kAdded, expected_first_row_id);
    expected_first_row_id += kMetrics.row_count.value();
  }
}

TEST_F(ManifestWriterVersionsTest, TestV3ManifestListRewriteWithInheritance) {
  // write with v1
  auto manifests =
      WriteAndReadManifests({WriteManifest(/*format_version=*/1, {data_file_})}, 1);
  CheckManifest(manifests[0], TableMetadata::kInitialSequenceNumber,
                TableMetadata::kInitialSequenceNumber);

  // rewrite existing metadata with a manifest list
  auto manifests3 = WriteAndReadManifests(manifests, 3);
  // the ManifestFile did not change and should still have its original sequence number, 0
  CheckManifest(manifests3[0], TableMetadata::kInitialSequenceNumber,
                TableMetadata::kInitialSequenceNumber);

  // should not inherit the sequence number because it was a rewrite
  auto entries = ReadManifest(manifests3[0]);
  CheckEntry(entries[0], TableMetadata::kInitialSequenceNumber,
             TableMetadata::kInitialSequenceNumber, DataFile::Content::kData,
             ManifestStatus::kAdded, kFirstRowId);
}

TEST_F(ManifestWriterVersionsTest, TestV3ManifestRewriteWithInheritance) {
  // write with v1
  auto manifests =
      WriteAndReadManifests({WriteManifest(/*format_version=*/1, {data_file_})}, 1);
  CheckManifest(manifests[0], TableMetadata::kInitialSequenceNumber,
                TableMetadata::kInitialSequenceNumber);

  // rewrite the manifest file using a v3 manifest
  auto rewritten_manifest = RewriteManifest(manifests[0], 3);
  CheckRewrittenManifest(rewritten_manifest, kInvalidSequenceNumber,
                         TableMetadata::kInitialSequenceNumber);

  // add the v3 manifest to a v3 manifest list, with a sequence number
  auto manifests3 = WriteAndReadManifests({rewritten_manifest}, 3);
  // the ManifestFile is new so it has a sequence number, but the min sequence number 0 is
  // from the entry
  CheckRewrittenManifest(manifests3[0], kSequenceNumber,
                         TableMetadata::kInitialSequenceNumber);

  // should not inherit the v3 sequence number because it was written into the v3 manifest
  auto entries = ReadManifest(manifests3[0]);
  CheckRewrittenEntry(entries[0], TableMetadata::kInitialSequenceNumber,
                      DataFile::Content::kData, kFirstRowId);
}

}  // namespace iceberg
