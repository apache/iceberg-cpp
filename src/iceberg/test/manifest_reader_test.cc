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

#include "iceberg/manifest/manifest_reader.h"

#include <chrono>
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

class TestManifestReader : public testing::TestWithParam<int> {
 protected:
  void SetUp() override {
    avro::RegisterAll();

    file_io_ = arrow::MakeMockFileIO();

    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(/*field_id=*/3, "id", int32()),
        SchemaField::MakeRequired(/*field_id=*/4, "data", string())});

    ICEBERG_UNWRAP_OR_FAIL(
        spec_,
        PartitionSpec::Make(
            /*spec_id=*/0, {PartitionField(/*source_id=*/4, /*field_id=*/1000,
                                           "data_bucket", Transform::Bucket(16))}));
  }

  std::string MakeManifestPath() {
    return std::format("manifest-{}.avro",
                       std::chrono::system_clock::now().time_since_epoch().count());
  }

  std::unique_ptr<DataFile> MakeDataFile(const std::string& path,
                                         const PartitionValues& partition,
                                         int64_t record_count = 1) {
    return std::make_unique<DataFile>(DataFile{
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = record_count,
        .file_size_in_bytes = 10,
        .sort_order_id = 0,
    });
  }

  ManifestFile WriteManifest(int format_version, std::optional<int64_t> snapshot_id,
                             const std::vector<ManifestEntry>& entries) {
    const std::string manifest_path = MakeManifestPath();

    Result<std::unique_ptr<ManifestWriter>> writer_result =
        NotSupported("Format version: {}", format_version);

    switch (format_version) {
      case 1:
        writer_result = ManifestWriter::MakeV1Writer(snapshot_id, manifest_path, file_io_,
                                                     spec_, schema_);
        break;
      case 2:
        writer_result = ManifestWriter::MakeV2Writer(
            snapshot_id, manifest_path, file_io_, spec_, schema_, ManifestContent::kData);
        break;
      case 3:
        writer_result = ManifestWriter::MakeV3Writer(snapshot_id, /*first_row_id=*/0L,
                                                     manifest_path, file_io_, spec_,
                                                     schema_, ManifestContent::kData);
        break;
    }

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (const auto& entry : entries) {
      EXPECT_THAT(writer->WriteEntry(entry), IsOk());
    }

    EXPECT_THAT(writer->Close(), IsOk());
    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());
    return std::move(manifest_result.value());
  }

  ManifestEntry MakeEntry(ManifestStatus status, int64_t snapshot_id,
                          std::unique_ptr<DataFile> file) {
    return ManifestEntry{
        .status = status,
        .snapshot_id = snapshot_id,
        .sequence_number =
            (status == ManifestStatus::kAdded) ? std::nullopt : std::make_optional(0),
        .file_sequence_number = std::nullopt,
        .data_file = std::move(file),
    };
  }

  std::unique_ptr<DataFile> MakeDeleteFile(
      const std::string& path, const PartitionValues& partition,
      DataFile::Content content,
      std::optional<std::string> referenced_file = std::nullopt,
      std::optional<int64_t> content_offset = std::nullopt,
      std::optional<int64_t> content_size = std::nullopt) {
    return std::make_unique<DataFile>(DataFile{
        .content = content,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = 1,
        .file_size_in_bytes = 10,
        .equality_ids = (content == DataFile::Content::kEqualityDeletes)
                            ? std::vector<int>{3}
                            : std::vector<int>{},
        .referenced_data_file = referenced_file,
        .content_offset = content_offset,
        .content_size_in_bytes = content_size,
    });
  }

  ManifestFile WriteDeleteManifest(int format_version, int64_t snapshot_id,
                                   std::vector<ManifestEntry> entries) {
    const std::string manifest_path = MakeManifestPath();

    Result<std::unique_ptr<ManifestWriter>> writer_result =
        NotSupported("Format version: {}", format_version);

    if (format_version == 2) {
      writer_result =
          ManifestWriter::MakeV2Writer(snapshot_id, manifest_path, file_io_, spec_,
                                       schema_, ManifestContent::kDeletes);
    } else if (format_version == 3) {
      writer_result = ManifestWriter::MakeV3Writer(
          snapshot_id, /*first_row_id=*/std::nullopt, manifest_path, file_io_, spec_,
          schema_, ManifestContent::kDeletes);
    }

    EXPECT_THAT(writer_result, IsOk());
    auto writer = std::move(writer_result.value());

    for (const auto& entry : entries) {
      EXPECT_THAT(writer->WriteEntry(entry), IsOk());
    }

    EXPECT_THAT(writer->Close(), IsOk());
    auto manifest_result = writer->ToManifestFile();
    EXPECT_THAT(manifest_result, IsOk());
    return std::move(manifest_result.value());
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
};

TEST_P(TestManifestReader, TestManifestReaderWithEmptyInheritableMetadata) {
  int version = GetParam();
  auto file_a =
      MakeDataFile("/path/to/data-a.parquet", PartitionValues({Literal::Int(0)}));

  auto entry =
      MakeEntry(ManifestStatus::kExisting, /*snapshot_id=*/1000L, std::move(file_a));
  entry.sequence_number = 0;
  entry.file_sequence_number = 0;

  std::vector<ManifestEntry> entries;
  entries.push_back(std::move(entry));
  auto manifest = WriteManifest(version, /*snapshot_id=*/1000L, std::move(entries));

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto read_entries = entries_result.value();

  ASSERT_EQ(read_entries.size(), 1);
  const auto& read_entry = read_entries[0];
  EXPECT_EQ(read_entry.status, ManifestStatus::kExisting);
  EXPECT_EQ(read_entry.data_file->file_path, "/path/to/data-a.parquet");
  EXPECT_EQ(read_entry.snapshot_id, 1000L);
}

TEST_P(TestManifestReader, TestReaderWithFilterWithoutSelect) {
  int version = GetParam();
  auto file_a =
      MakeDataFile("/path/to/data-a.parquet", PartitionValues({Literal::Int(0)}));
  auto file_b =
      MakeDataFile("/path/to/data-b.parquet", PartitionValues({Literal::Int(1)}));
  auto file_c =
      MakeDataFile("/path/to/data-c.parquet", PartitionValues({Literal::Int(2)}));

  std::vector<ManifestEntry> entries;
  entries.push_back(MakeEntry(ManifestStatus::kAdded, 1000L, std::move(file_a)));
  entries.push_back(MakeEntry(ManifestStatus::kAdded, 1000L, std::move(file_b)));
  entries.push_back(MakeEntry(ManifestStatus::kAdded, 1000L, std::move(file_c)));

  auto manifest = WriteManifest(version, 1000L, std::move(entries));

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  reader->FilterRows(Expressions::Equal("id", Literal::Int(0)));

  auto result_entries = reader->Entries();
  ASSERT_THAT(result_entries, IsOk());
  auto read_entries = result_entries.value();

  // note that all files are returned because the reader returns data files that may
  // match, and the partition is bucketing by data, which doesn't help filter files
  ASSERT_EQ(read_entries.size(), 3);
  EXPECT_EQ(read_entries[0].data_file->file_path, "/path/to/data-a.parquet");
  EXPECT_EQ(read_entries[1].data_file->file_path, "/path/to/data-b.parquet");
  EXPECT_EQ(read_entries[2].data_file->file_path, "/path/to/data-c.parquet");
}

TEST_P(TestManifestReader, TestManifestReaderWithPartitionMetadata) {
  int version = GetParam();
  auto file_a =
      MakeDataFile("/path/to/data-a.parquet", PartitionValues({Literal::Int(0)}));
  auto entry =
      MakeEntry(ManifestStatus::kExisting, /*snapshot_id=*/123L, std::move(file_a));
  entry.sequence_number = 0;
  entry.file_sequence_number = 0;

  std::vector<ManifestEntry> entries;
  entries.push_back(std::move(entry));
  auto manifest = WriteManifest(version, /*snapshot_id=*/1000L, std::move(entries));

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto read_entries = entries_result.value();

  ASSERT_EQ(read_entries.size(), 1);
  const auto& read_entry = read_entries[0];
  EXPECT_EQ(read_entry.snapshot_id, 123L);

  ASSERT_EQ(read_entry.data_file->partition.num_fields(), 1);
  EXPECT_EQ(read_entry.data_file->partition.values()[0], Literal::Int(0));
}

TEST_P(TestManifestReader, TestDeleteFilesWithReferences) {
  int version = GetParam();
  if (version < 2) {
    GTEST_SKIP() << "Delete files only supported in V2+";
  }

  auto delete_file_1 = MakeDeleteFile(
      "/path/to/data-a-deletes.parquet", PartitionValues({Literal::Int(0)}),
      DataFile::Content::kPositionDeletes, "/path/to/data-a.parquet");
  auto delete_file_2 = MakeDeleteFile(
      "/path/to/data-b-deletes.parquet", PartitionValues({Literal::Int(1)}),
      DataFile::Content::kPositionDeletes, "/path/to/data-b.parquet");

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeEntry(ManifestStatus::kAdded, /*snapshot_id=*/1000L, std::move(delete_file_1)));
  entries.push_back(
      MakeEntry(ManifestStatus::kAdded, /*snapshot_id=*/1000L, std::move(delete_file_2)));

  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries));

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto read_entries = entries_result.value();
  ASSERT_EQ(read_entries.size(), 2);

  for (const auto& entry : read_entries) {
    if (entry.data_file->file_path == "/path/to/data-a-deletes.parquet") {
      EXPECT_EQ(entry.data_file->referenced_data_file, "/path/to/data-a.parquet");
    } else {
      EXPECT_EQ(entry.data_file->referenced_data_file, "/path/to/data-b.parquet");
    }
  }
}

TEST_P(TestManifestReader, TestDVs) {
  int version = GetParam();
  if (version < 3) {
    GTEST_SKIP() << "DVs only supported in V3+";
  }

  auto dv1 =
      MakeDeleteFile("/path/to/data-a-deletes.puffin", PartitionValues({Literal::Int(0)}),
                     DataFile::Content::kPositionDeletes, "/path/to/data-a.parquet",
                     /*content_offset=*/4L, /*content_size_in_bytes=*/6L);
  auto dv2 =
      MakeDeleteFile("/path/to/data-b-deletes.puffin", PartitionValues({Literal::Int(1)}),
                     DataFile::Content::kPositionDeletes, "/path/to/data-b.parquet",
                     /*content_offset=*/4L, /*content_size_in_bytes=*/6L);

  std::vector<ManifestEntry> entries;
  entries.push_back(
      MakeEntry(ManifestStatus::kAdded, /*snapshot_id=*/1000L, std::move(dv1)));
  entries.push_back(
      MakeEntry(ManifestStatus::kAdded, /*snapshot_id=*/1000L, std::move(dv2)));
  auto manifest = WriteDeleteManifest(version, /*snapshot_id=*/1000L, std::move(entries));

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());

  auto entries_result = reader->Entries();
  ASSERT_THAT(entries_result, IsOk());
  auto read_entries = entries_result.value();
  ASSERT_EQ(read_entries.size(), 2);

  for (const auto& entry : read_entries) {
    if (entry.data_file->file_path == "/path/to/data-a-deletes.puffin") {
      EXPECT_EQ(entry.data_file->referenced_data_file, "/path/to/data-a.parquet");
      EXPECT_EQ(entry.data_file->content_offset, 4L);
      EXPECT_EQ(entry.data_file->content_size_in_bytes, 6L);
    } else {
      EXPECT_EQ(entry.data_file->referenced_data_file, "/path/to/data-b.parquet");
      EXPECT_EQ(entry.data_file->content_offset, 4L);
      EXPECT_EQ(entry.data_file->content_size_in_bytes, 6L);
    }
  }
}

TEST_P(TestManifestReader, TestInvalidUsage) {
  int version = GetParam();
  auto file_a =
      MakeDataFile("/path/to/data-a.parquet", PartitionValues({Literal::Int(0)}));
  auto entry =
      MakeEntry(ManifestStatus::kExisting, /*snapshot_id=*/1000L, std::move(file_a));
  entry.sequence_number = 0;
  entry.file_sequence_number = 0;

  auto manifest =
      WriteManifest(version, /*snapshot_id=*/std::nullopt, {std::move(entry)});

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  EXPECT_THAT(reader_result, IsError(ErrorKind::kInvalidManifest));
  EXPECT_THAT(reader_result, HasErrorMessage("has no snapshot ID"));
}

TEST_P(TestManifestReader, TestDropStats) {
  int version = GetParam();

  // Create a data file with full metrics
  auto file_with_stats = std::make_unique<DataFile>(DataFile{
      .file_path = "/path/to/data-with-stats.parquet",
      .file_format = FileFormatType::kParquet,
      .partition = PartitionValues({Literal::Int(0)}),
      .record_count = 100,
      .file_size_in_bytes = 1024,
      // Add stats for multiple columns
      .column_sizes = {{1, 100}, {2, 200}, {3, 300}},
      .value_counts = {{1, 10}, {2, 20}, {3, 30}},
      .null_value_counts = {{1, 1}, {2, 2}, {3, 3}},
      .nan_value_counts = {{1, 0}, {2, 0}, {3, 0}},
      .lower_bounds = {{1, {0x01}}, {2, {0x02}}, {3, {0x03}}},
      .upper_bounds = {{1, {0xFF}}, {2, {0xFE}}, {3, {0xFD}}},
      .sort_order_id = 0,
  });

  auto entry = MakeEntry(ManifestStatus::kAdded, /*snapshot_id=*/1000L,
                         std::move(file_with_stats));

  std::vector<ManifestEntry> entries;
  entries.push_back(std::move(entry));

  auto manifest = WriteManifest(version, /*snapshot_id=*/1000L, std::move(entries));

  auto reader_result = ManifestReader::Make(manifest, file_io_, schema_, spec_);
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(reader_result.value());
  reader->Select({"record_count"}).TryDropStats();

  ICEBERG_UNWRAP_OR_FAIL(auto read_entries, reader->Entries());
  ASSERT_EQ(read_entries.size(), 1);
  const auto& read_entry = read_entries[0];

  // record_count should be preserved
  EXPECT_EQ(read_entry.data_file->record_count, 100);

  // Stats maps should be cleared
  EXPECT_TRUE(read_entry.data_file->column_sizes.empty());
  EXPECT_TRUE(read_entry.data_file->value_counts.empty());
  EXPECT_TRUE(read_entry.data_file->null_value_counts.empty());
  EXPECT_TRUE(read_entry.data_file->nan_value_counts.empty());
  EXPECT_TRUE(read_entry.data_file->lower_bounds.empty());
  EXPECT_TRUE(read_entry.data_file->upper_bounds.empty());
}

TEST(ManifestReaderStaticTest, TestShouldDropStats) {
  EXPECT_FALSE(ManifestReader::ShouldDropStats({}));
  EXPECT_FALSE(ManifestReader::ShouldDropStats({std::string(Schema::kAllColumns)}));
  EXPECT_TRUE(ManifestReader::ShouldDropStats({"file_path", "file_format", "partition"}));
  EXPECT_TRUE(
      ManifestReader::ShouldDropStats({"file_path", "file_format", "record_count"}));
  EXPECT_FALSE(
      ManifestReader::ShouldDropStats({"file_path", "file_format", "value_counts"}));
  EXPECT_FALSE(
      ManifestReader::ShouldDropStats({"file_path", "file_format", "lower_bounds"}));
  EXPECT_FALSE(ManifestReader::ShouldDropStats(
      {"file_path", "value_counts", "null_value_counts", "lower_bounds"}));
  EXPECT_FALSE(
      ManifestReader::ShouldDropStats({"file_path", "record_count", "value_counts"}));
}

INSTANTIATE_TEST_SUITE_P(ManifestReaderVersions, TestManifestReader,
                         testing::Values(1, 2, 3));

}  // namespace iceberg
