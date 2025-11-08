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

#include <optional>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader.h"
#include "iceberg/manifest_writer.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/v1_metadata.h"

namespace iceberg {

constexpr int kRowLineageFormatVersion = 3;
constexpr const char* kPath = "s3://bucket/table/m1.avro";
constexpr int64_t kLength = 1024L;
constexpr int32_t kSpecId = 1;
constexpr int64_t kSeqNum = 34L;
constexpr int64_t kMinSeqNum = 10L;
constexpr int64_t kSnapshotId = 987134631982734L;
constexpr int32_t kAddedFiles = 2;
constexpr int64_t kAddedRows = 5292L;
constexpr int32_t kExistingFiles = 343;
constexpr int64_t kExistingRows = 857273L;
constexpr int32_t kDeletedFiles = 1;
constexpr int64_t kDeletedRows = 22910L;
constexpr int64_t kFirstRowId = 100L;
constexpr int64_t kSnapshotFirstRowId = 130L;

const static auto kTestManifest = ManifestFile{
    .manifest_path = kPath,
    .manifest_length = kLength,
    .partition_spec_id = kSpecId,
    .content = ManifestFile::Content::kData,
    .sequence_number = kSeqNum,
    .min_sequence_number = kMinSeqNum,
    .added_snapshot_id = kSnapshotId,
    .added_files_count = kAddedFiles,
    .existing_files_count = kExistingFiles,
    .deleted_files_count = kDeletedFiles,
    .added_rows_count = kAddedRows,
    .existing_rows_count = kExistingRows,
    .deleted_rows_count = kDeletedRows,
    .partitions = {},
    .key_metadata = {},
    .first_row_id = kFirstRowId,
};

const static auto kDeleteManifest = ManifestFile{
    .manifest_path = kPath,
    .manifest_length = kLength,
    .partition_spec_id = kSpecId,
    .content = ManifestFile::Content::kDeletes,
    .sequence_number = kSeqNum,
    .min_sequence_number = kMinSeqNum,
    .added_snapshot_id = kSnapshotId,
    .added_files_count = kAddedFiles,
    .existing_files_count = kExistingFiles,
    .deleted_files_count = kDeletedFiles,
    .added_rows_count = kAddedRows,
    .existing_rows_count = kExistingRows,
    .deleted_rows_count = kDeletedRows,
    .first_row_id = std::nullopt,
};

class TestManifestListVersions : public ::testing::Test {
 protected:
  void SetUp() override {
    avro::RegisterAll();
    file_io_ = iceberg::arrow::MakeMockFileIO();
  }

  static std::string CreateManifestListPath() {
    return std::format("manifest-list-{}.avro",
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  std::string WriteManifestList(int format_version, int64_t expected_next_row_id,
                                const std::vector<ManifestFile>& manifests) const {
    const std::string manifest_list_path = CreateManifestListPath();
    constexpr int64_t kParentSnapshotId = kSnapshotId - 1;

    Result<std::unique_ptr<ManifestListWriter>> writer_result =
        NotSupported("Format version: {}", format_version);

    if (format_version == 1) {
      writer_result = ManifestListWriter::MakeV1Writer(kSnapshotId, kParentSnapshotId,
                                                       manifest_list_path, file_io_);
    } else if (format_version == 2) {
      writer_result = ManifestListWriter::MakeV2Writer(
          kSnapshotId, kParentSnapshotId, kSeqNum, manifest_list_path, file_io_);
    } else if (format_version == 3) {
      writer_result = ManifestListWriter::MakeV3Writer(kSnapshotId, kParentSnapshotId,
                                                       kSeqNum, kSnapshotFirstRowId,
                                                       manifest_list_path, file_io_);
    }

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    EXPECT_THAT(writer->AddAll(manifests), IsOk());
    EXPECT_THAT(writer->Close(), IsOk());

    if (format_version >= kRowLineageFormatVersion) {
      EXPECT_EQ(writer->next_row_id(), std::make_optional(expected_next_row_id));
    } else {
      EXPECT_FALSE(writer->next_row_id().has_value());
    }

    return manifest_list_path;
  }

  ManifestFile WriteAndReadManifestList(int format_version) const {
    return ReadManifestList(
        WriteManifestList(format_version, kSnapshotFirstRowId, {kTestManifest}));
  }

  ManifestFile ReadManifestList(const std::string& manifest_list_path) const {
    auto reader_result = ManifestListReader::Make(manifest_list_path, file_io_);
    EXPECT_THAT(reader_result, IsOk());

    auto reader = std::move(reader_result.value());
    auto files_result = reader->Files();
    EXPECT_THAT(files_result, IsOk());

    auto manifests = files_result.value();
    EXPECT_EQ(manifests.size(), 1);

    return manifests[0];
  }

  std::vector<ManifestFile> ReadAllManifests(
      const std::string& manifest_list_path) const {
    auto reader_result = ManifestListReader::Make(manifest_list_path, file_io_);
    EXPECT_THAT(reader_result, IsOk());

    auto reader = std::move(reader_result.value());
    auto files_result = reader->Files();
    EXPECT_THAT(files_result, IsOk());

    return files_result.value();
  }

  void ReadAvro(const std::string& path, const std::shared_ptr<Schema>& schema,
                const std::string& expected_json) const {
    auto reader_result = ReaderFactoryRegistry::Open(
        FileFormatType::kAvro, {.path = path, .io = file_io_, .projection = schema});
    EXPECT_THAT(reader_result, IsOk());
    auto reader = std::move(reader_result.value());

    auto arrow_schema_result = reader->Schema();
    EXPECT_THAT(arrow_schema_result, IsOk());
    auto arrow_c_schema = std::move(arrow_schema_result.value());
    auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    auto expected_array =
        ::arrow::json::ArrayFromJSONString(arrow_schema, expected_json).ValueOrDie();

    auto batch_result = reader->Next();
    EXPECT_THAT(batch_result, IsOk());
    EXPECT_TRUE(batch_result.value().has_value());
    auto arrow_c_batch = std::move(batch_result.value().value());

    auto arrow_batch_result =
        ::arrow::ImportArray(&arrow_c_batch, std::move(arrow_schema));
    auto array = arrow_batch_result.ValueOrDie();
    EXPECT_TRUE(array != nullptr);
    EXPECT_TRUE(expected_array->Equals(*array));
  }

  std::shared_ptr<FileIO> file_io_;
};

TEST_F(TestManifestListVersions, TestV1WriteDeleteManifest) {
  const std::string manifest_list_path = CreateManifestListPath();

  auto writer_result = ManifestListWriter::MakeV1Writer(kSnapshotId, kSnapshotId - 1,
                                                        manifest_list_path, file_io_);
  EXPECT_THAT(writer_result, IsOk());

  auto writer = std::move(writer_result.value());
  auto status = writer->Add(kDeleteManifest);

  EXPECT_THAT(status, IsError(ErrorKind::kInvalidManifestList));
  EXPECT_THAT(status, HasErrorMessage("Cannot store delete manifests in a v1 table"));
}

TEST_F(TestManifestListVersions, TestV1Write) {
  auto manifest = WriteAndReadManifestList(/*format_version=*/1);

  // V3 fields are not written and are defaulted
  EXPECT_FALSE(manifest.first_row_id.has_value());

  // V2 fields are not written and are defaulted
  EXPECT_EQ(manifest.sequence_number, 0);
  EXPECT_EQ(manifest.min_sequence_number, 0);

  // V1 fields are read correctly
  EXPECT_EQ(manifest.manifest_path, kPath);
  EXPECT_EQ(manifest.manifest_length, kLength);
  EXPECT_EQ(manifest.partition_spec_id, kSpecId);
  EXPECT_EQ(manifest.content, ManifestFile::Content::kData);
  EXPECT_EQ(manifest.added_snapshot_id, kSnapshotId);
  EXPECT_EQ(manifest.added_files_count, kAddedFiles);
  EXPECT_EQ(manifest.existing_files_count, kExistingFiles);
  EXPECT_EQ(manifest.deleted_files_count, kDeletedFiles);
  EXPECT_EQ(manifest.added_rows_count, kAddedRows);
  EXPECT_EQ(manifest.existing_rows_count, kExistingRows);
  EXPECT_EQ(manifest.deleted_rows_count, kDeletedRows);
}

TEST_F(TestManifestListVersions, TestV2Write) {
  auto manifest = WriteAndReadManifestList(2);

  // V3 fields are not written and are defaulted
  EXPECT_FALSE(manifest.first_row_id.has_value());

  // All V2 fields should be read correctly
  EXPECT_EQ(manifest.manifest_path, kPath);
  EXPECT_EQ(manifest.manifest_length, kLength);
  EXPECT_EQ(manifest.partition_spec_id, kSpecId);
  EXPECT_EQ(manifest.content, ManifestFile::Content::kData);
  EXPECT_EQ(manifest.sequence_number, kSeqNum);
  EXPECT_EQ(manifest.min_sequence_number, kMinSeqNum);
  EXPECT_EQ(manifest.added_snapshot_id, kSnapshotId);
  EXPECT_EQ(manifest.added_files_count, kAddedFiles);
  EXPECT_EQ(manifest.added_rows_count, kAddedRows);
  EXPECT_EQ(manifest.existing_files_count, kExistingFiles);
  EXPECT_EQ(manifest.existing_rows_count, kExistingRows);
  EXPECT_EQ(manifest.deleted_files_count, kDeletedFiles);
  EXPECT_EQ(manifest.deleted_rows_count, kDeletedRows);
}

TEST_F(TestManifestListVersions, TestV3Write) {
  auto manifest = WriteAndReadManifestList(/*format_version=*/3);

  // All V3 fields should be read correctly
  EXPECT_EQ(manifest.manifest_path, kPath);
  EXPECT_EQ(manifest.manifest_length, kLength);
  EXPECT_EQ(manifest.partition_spec_id, kSpecId);
  EXPECT_EQ(manifest.content, ManifestFile::Content::kData);
  EXPECT_EQ(manifest.sequence_number, kSeqNum);
  EXPECT_EQ(manifest.min_sequence_number, kMinSeqNum);
  EXPECT_EQ(manifest.added_snapshot_id, kSnapshotId);
  EXPECT_EQ(manifest.added_files_count, kAddedFiles);
  EXPECT_EQ(manifest.added_rows_count, kAddedRows);
  EXPECT_EQ(manifest.existing_files_count, kExistingFiles);
  EXPECT_EQ(manifest.existing_rows_count, kExistingRows);
  EXPECT_EQ(manifest.deleted_files_count, kDeletedFiles);
  EXPECT_EQ(manifest.deleted_rows_count, kDeletedRows);
  EXPECT_TRUE(manifest.first_row_id.has_value());
  EXPECT_EQ(manifest.first_row_id.value(), kFirstRowId);
}

TEST_F(TestManifestListVersions, TestV3WriteFirstRowIdAssignment) {
  ManifestFile missing_first_row_id = kTestManifest;
  missing_first_row_id.first_row_id = std::nullopt;

  constexpr int64_t kExpectedNextRowId = kSnapshotFirstRowId + kAddedRows + kExistingRows;
  auto manifest_list_path =
      WriteManifestList(/*format_version=*/3, kExpectedNextRowId, {missing_first_row_id});

  auto manifest = ReadManifestList(manifest_list_path);
  EXPECT_EQ(manifest.manifest_path, kPath);
  EXPECT_EQ(manifest.manifest_length, kLength);
  EXPECT_EQ(manifest.partition_spec_id, kSpecId);
  EXPECT_EQ(manifest.content, ManifestFile::Content::kData);
  EXPECT_EQ(manifest.sequence_number, kSeqNum);
  EXPECT_EQ(manifest.min_sequence_number, kMinSeqNum);
  EXPECT_EQ(manifest.added_snapshot_id, kSnapshotId);
  EXPECT_EQ(manifest.added_files_count, kAddedFiles);
  EXPECT_EQ(manifest.added_rows_count, kAddedRows);
  EXPECT_EQ(manifest.existing_files_count, kExistingFiles);
  EXPECT_EQ(manifest.existing_rows_count, kExistingRows);
  EXPECT_EQ(manifest.deleted_files_count, kDeletedFiles);
  EXPECT_EQ(manifest.deleted_rows_count, kDeletedRows);
  EXPECT_EQ(manifest.first_row_id, std::make_optional(kSnapshotFirstRowId));
}

TEST_F(TestManifestListVersions, TestV3WriteMixedRowIdAssignment) {
  ManifestFile missing_first_row_id = kTestManifest;
  missing_first_row_id.first_row_id = std::nullopt;

  constexpr int64_t kExpectedNextRowId =
      kSnapshotFirstRowId + 2 * (kAddedRows + kExistingRows);

  auto manifest_list_path = WriteManifestList(
      3, kExpectedNextRowId, {missing_first_row_id, kTestManifest, missing_first_row_id});

  auto manifests = ReadAllManifests(manifest_list_path);
  EXPECT_EQ(manifests.size(), 3);

  // all v2 fields should be read correctly
  for (const auto& manifest : manifests) {
    EXPECT_EQ(manifest.manifest_path, kPath);
    EXPECT_EQ(manifest.manifest_length, kLength);
    EXPECT_EQ(manifest.partition_spec_id, kSpecId);
    EXPECT_EQ(manifest.content, ManifestFile::Content::kData);
    EXPECT_EQ(manifest.sequence_number, kSeqNum);
    EXPECT_EQ(manifest.min_sequence_number, kMinSeqNum);
    EXPECT_EQ(manifest.added_snapshot_id, kSnapshotId);
    EXPECT_EQ(manifest.added_files_count, kAddedFiles);
    EXPECT_EQ(manifest.added_rows_count, kAddedRows);
    EXPECT_EQ(manifest.existing_files_count, kExistingFiles);
    EXPECT_EQ(manifest.existing_rows_count, kExistingRows);
    EXPECT_EQ(manifest.deleted_files_count, kDeletedFiles);
    EXPECT_EQ(manifest.deleted_rows_count, kDeletedRows);
  }

  EXPECT_EQ(manifests[0].first_row_id, std::make_optional(kSnapshotFirstRowId));
  EXPECT_EQ(manifests[1].first_row_id, kTestManifest.first_row_id);
  EXPECT_EQ(manifests[2].first_row_id,
            std::make_optional(kSnapshotFirstRowId + kAddedRows + kExistingRows));
}

TEST_F(TestManifestListVersions, TestV1ForwardCompatibility) {
  std::string manifest_list_path =
      WriteManifestList(/*format_version=*/1, kSnapshotFirstRowId, {kTestManifest});
  std::string expected_array_json = R"([
    ["s3://bucket/table/m1.avro", 1024, 1, 987134631982734, 2, 343, 1, [], 5292, 857273, 22910, null]
  ])";
  ReadAvro(manifest_list_path, ManifestFileAdapterV1::kManifestListSchema,
           expected_array_json);
}

TEST_F(TestManifestListVersions, TestV2ForwardCompatibility) {
  // V2 manifest list files can be read by V1 readers, but the sequence numbers and
  // content will be ignored.
  std::string manifest_list_path =
      WriteManifestList(/*format_version=*/2, kSnapshotFirstRowId, {kTestManifest});
  std::string expected_array_json = R"([
    ["s3://bucket/table/m1.avro", 1024, 1, 987134631982734, 2, 343, 1, [], 5292, 857273, 22910, null]
  ])";
  ReadAvro(manifest_list_path, ManifestFileAdapterV1::kManifestListSchema,
           expected_array_json);
}

TEST_F(TestManifestListVersions, TestManifestsWithoutRowStats) {
  // Create a schema without row stats columns to simulate an old manifest list file
  auto schema_without_stats = std::make_shared<Schema>(std::vector<SchemaField>{
      ManifestFile::kManifestPath,
      ManifestFile::kManifestLength,
      ManifestFile::kPartitionSpecId,
      ManifestFile::kAddedSnapshotId,
      ManifestFile::kAddedFilesCount,
      ManifestFile::kExistingFilesCount,
      ManifestFile::kDeletedFilesCount,
      ManifestFile::kPartitions,
  });

  ArrowSchema arrow_c_schema;
  EXPECT_THAT(ToArrowSchema(*schema_without_stats, &arrow_c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

  std::string json_data = R"([["path/to/manifest.avro", 1024, 1, 100, 2, 3, 4, null]])";
  auto array = ::arrow::json::ArrayFromJSONString(arrow_schema, json_data).ValueOrDie();
  ArrowArray arrow_array;
  EXPECT_TRUE(::arrow::ExportArray(*array, &arrow_array).ok());

  std::string manifest_list_path = CreateManifestListPath();
  auto writer_result = WriterFactoryRegistry::Open(
      FileFormatType::kAvro,
      {.path = manifest_list_path, .schema = schema_without_stats, .io = file_io_});
  EXPECT_THAT(writer_result, IsOk());
  auto writer = std::move(writer_result.value());
  EXPECT_THAT(writer->Write(&arrow_array), IsOk());
  EXPECT_THAT(writer->Close(), IsOk());

  // Read back and verify
  auto manifest = ReadManifestList(manifest_list_path);

  EXPECT_EQ(manifest.manifest_path, "path/to/manifest.avro");
  EXPECT_EQ(manifest.manifest_length, 1024L);
  EXPECT_EQ(manifest.partition_spec_id, 1);
  EXPECT_EQ(manifest.added_snapshot_id, 100L);

  EXPECT_TRUE(manifest.has_added_files());
  EXPECT_EQ(manifest.added_files_count, 2);
  EXPECT_FALSE(manifest.added_rows_count.has_value());

  EXPECT_TRUE(manifest.has_existing_files());
  EXPECT_EQ(manifest.existing_files_count, 3);
  EXPECT_FALSE(manifest.existing_rows_count.has_value());

  EXPECT_TRUE(manifest.has_deleted_files());
  EXPECT_EQ(manifest.deleted_files_count, 4);
  EXPECT_FALSE(manifest.deleted_rows_count.has_value());

  EXPECT_FALSE(manifest.first_row_id.has_value());
}

TEST_F(TestManifestListVersions, TestManifestsPartitionSummary) {
  auto first_summary_lower_bound = Literal::Int(10).Serialize().value();
  auto first_summary_upper_bound = Literal::Int(100).Serialize().value();
  auto second_summary_lower_bound = Literal::Int(20).Serialize().value();
  auto second_summary_upper_bound = Literal::Int(200).Serialize().value();

  std::vector<PartitionFieldSummary> partition_summaries{
      PartitionFieldSummary{
          .contains_null = false,
          .contains_nan = std::nullopt,
          .lower_bound = first_summary_lower_bound,
          .upper_bound = first_summary_upper_bound,
      },
      PartitionFieldSummary{
          .contains_null = true,
          .contains_nan = false,
          .lower_bound = second_summary_lower_bound,
          .upper_bound = second_summary_upper_bound,
      },
  };

  ManifestFile manifest{
      .manifest_path = kPath,
      .manifest_length = kLength,
      .partition_spec_id = kSpecId,
      .content = ManifestFile::Content::kData,
      .sequence_number = kSeqNum,
      .min_sequence_number = kMinSeqNum,
      .added_snapshot_id = kSnapshotId,
      .added_files_count = kAddedFiles,
      .existing_files_count = kExistingFiles,
      .deleted_files_count = kDeletedFiles,
      .added_rows_count = kAddedRows,
      .existing_rows_count = kExistingRows,
      .deleted_rows_count = kDeletedRows,
      .partitions = partition_summaries,
      .key_metadata = {},
      .first_row_id = std::nullopt,
  };

  // Test for all format versions
  for (int format_version = 1; format_version <= 3; ++format_version) {
    int64_t expected_next_row_id = kSnapshotFirstRowId +
                                   manifest.added_rows_count.value() +
                                   manifest.existing_rows_count.value();

    auto manifest_list_path =
        WriteManifestList(format_version, expected_next_row_id, {manifest});

    auto returned_manifest = ReadManifestList(manifest_list_path);
    EXPECT_EQ(returned_manifest.partitions.size(), 2);

    const auto& first = returned_manifest.partitions[0];
    EXPECT_FALSE(first.contains_null);
    EXPECT_FALSE(first.contains_nan.has_value());
    EXPECT_EQ(first.lower_bound, first_summary_lower_bound);
    EXPECT_EQ(first.upper_bound, first_summary_upper_bound);

    const auto& second = returned_manifest.partitions[1];
    EXPECT_TRUE(second.contains_null);
    EXPECT_TRUE(second.contains_nan.has_value());
    EXPECT_FALSE(second.contains_nan.value());
    EXPECT_EQ(second.lower_bound, second_summary_lower_bound);
    EXPECT_EQ(second.upper_bound, second_summary_upper_bound);
  }
}

}  // namespace iceberg
