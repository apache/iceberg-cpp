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
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/deletes/dv_util_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_reader.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/std_io.h"
#include "iceberg/test/test_resource.h"

namespace iceberg::puffin {

namespace {

constexpr std::string_view kReferencedDataFileProperty = "referenced-data-file";
constexpr std::string_view kCardinalityProperty = "cardinality";

int64_t Position(int64_t bucket, int64_t container, int64_t value) {
  return (bucket << 32) + (container << 16) + value;
}

std::vector<int64_t> AllContainerPositions() {
  std::vector<int64_t> positions = {
      Position(0, 0, 5),
      Position(0, 0, 7),
      Position(1, 0, 10),
      Position(1, 0, 20),
  };
  positions.reserve(10004);
  for (int64_t bucket = 0; bucket < 2; ++bucket) {
    for (int64_t value = 0; value < 10000; value += 2) {
      positions.push_back(Position(bucket, 2, value));
    }
  }
  return positions;
}

struct PositionRange {
  int64_t begin;
  int64_t end;
};

struct ExpectedBlob {
  std::string referenced_data_file;
  std::vector<int32_t> input_fields;
  int64_t offset;
  int64_t length;
  int64_t cardinality;
  std::vector<int64_t> positions;
  std::vector<PositionRange> ranges;
};

std::shared_ptr<DataFile> MakeDeleteFile(const ExpectedBlob& expected,
                                         const std::string& fixture_path,
                                         int64_t file_size) {
  return std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = fixture_path,
      .file_format = FileFormatType::kPuffin,
      .record_count = expected.cardinality,
      .file_size_in_bytes = file_size,
      .referenced_data_file = expected.referenced_data_file,
      .content_offset = expected.offset,
      .content_size_in_bytes = expected.length,
  });
}

void AssertPositions(const ExpectedBlob& expected,
                     const std::shared_ptr<DataFile>& delete_file,
                     const std::shared_ptr<FileIO>& io) {
  ASSERT_TRUE(delete_file->content_offset.has_value());
  ASSERT_TRUE(delete_file->content_size_in_bytes.has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto positions, DVUtil::ReadDV(delete_file, io));
  EXPECT_EQ(positions.Cardinality(), expected.cardinality);
  int64_t expected_cardinality = static_cast<int64_t>(expected.positions.size());
  for (const auto& range : expected.ranges) {
    expected_cardinality += range.end - range.begin;
  }
  ASSERT_EQ(expected_cardinality, expected.cardinality);
  for (int64_t position : expected.positions) {
    EXPECT_TRUE(positions.IsDeleted(position)) << "Missing position " << position;
  }
  for (const auto& range : expected.ranges) {
    for (int64_t position = range.begin; position < range.end; ++position) {
      ASSERT_TRUE(positions.IsDeleted(position)) << "Missing position " << position;
    }
  }
}

void AssertFixture(const std::string& resource_name,
                   const std::vector<ExpectedBlob>& expected_blobs) {
  const std::string fixture_path = GetResourcePath(resource_name);
  const auto file_size = static_cast<int64_t>(std::filesystem::file_size(fixture_path));
  auto io = std::make_shared<test::StdFileIO>();
  ICEBERG_UNWRAP_OR_FAIL(auto input_file, io->NewInputFile(fixture_path));
  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(std::move(input_file)));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, reader->ReadFileMetadata());

  ASSERT_EQ(metadata.blobs.size(), expected_blobs.size());
  std::unordered_set<std::string> seen;
  for (const auto& blob : metadata.blobs) {
    const auto& referenced_data_file =
        blob.properties.at(std::string(kReferencedDataFileProperty));
    auto expected = std::ranges::find_if(expected_blobs, [&](const auto& candidate) {
      return candidate.referenced_data_file == referenced_data_file;
    });
    ASSERT_NE(expected, expected_blobs.end())
        << "Unexpected referenced data file " << referenced_data_file;
    ASSERT_TRUE(seen.insert(referenced_data_file).second)
        << "Duplicate referenced data file " << referenced_data_file;

    EXPECT_EQ(blob.type, StandardBlobTypes::kDeletionVectorV1);
    EXPECT_EQ(blob.input_fields, expected->input_fields);
    EXPECT_EQ(blob.snapshot_id, -1);
    EXPECT_EQ(blob.sequence_number, -1);
    EXPECT_EQ(blob.offset, expected->offset);
    EXPECT_EQ(blob.length, expected->length);
    EXPECT_TRUE(blob.compression_codec.empty());
    EXPECT_EQ(blob.properties.at(std::string(kCardinalityProperty)),
              std::to_string(expected->cardinality));

    AssertPositions(*expected, MakeDeleteFile(*expected, fixture_path, file_size), io);
  }
  EXPECT_EQ(seen.size(), expected_blobs.size());
}

}  // namespace

TEST(PuffinDVInteropTest, ReadsJavaSingleBlobFixture) {
  AssertFixture(
      "deletion_vectors/java/single-blob-dv.puffin",
      {{
          .referenced_data_file = "s3://warehouse/db/table/data/00000-0-abc.parquet",
          .input_fields = {MetadataColumns::kFilePositionColumnId},
          .offset = 4,
          .length = 50,
          .cardinality = 5,
          .positions = {1, 3, 5, 7, 9},
      }});
}

TEST(PuffinDVInteropTest, ReadsJavaMultiBlobFixture) {
  AssertFixture(
      "deletion_vectors/java/multi-blob-dv.puffin",
      {
          {
              .referenced_data_file = "s3://warehouse/db/table/data/file-001.parquet",
              .input_fields = {MetadataColumns::kFilePositionColumnId},
              .offset = 4,
              .length = 46,
              .cardinality = 3,
              .positions = {0, 100, 200},
          },
          {
              .referenced_data_file = "s3://warehouse/db/table/data/file-002.parquet",
              .input_fields = {MetadataColumns::kFilePositionColumnId},
              .offset = 50,
              .length = 44,
              .cardinality = 2,
              .positions = {50, 150},
          },
      });
}

TEST(PuffinDVInteropTest, ReadsGoSingleBlobFixture) {
  AssertFixture("deletion_vectors/go/single-blob-dv.puffin",
                {{
                    .referenced_data_file = "data/test.parquet",
                    .input_fields = {},
                    .offset = 4,
                    .length = 50,
                    .cardinality = 5,
                    .positions = {1, 3, 5, 7, 9},
                }});
}

TEST(PuffinDVInteropTest, ReadsGoMultiBlobFixture) {
  AssertFixture(
      "deletion_vectors/go/multi-blob-dv.puffin",
      {
          {
              .referenced_data_file = "s3://warehouse/db/table/data/go-file-001.parquet",
              .input_fields = {},
              .offset = 4,
              .length = 68,
              .cardinality = 4,
              .positions = {0, 100, 200, (int64_t{1} << 32) + 7},
          },
          {
              .referenced_data_file = "s3://warehouse/db/table/data/go-file-002.parquet",
              .input_fields = {},
              .offset = 72,
              .length = 66,
              .cardinality = 3,
              .positions = {50, 150, (int64_t{2} << 32) + 9},
          },
      });
}

TEST(PuffinDVInteropTest, ReadsGoAllContainerTypesFixture) {
  AssertFixture(
      "deletion_vectors/go/all-container-types-dv.puffin",
      {{
          .referenced_data_file = "s3://warehouse/db/table/data/all-containers.parquet",
          .input_fields = {},
          .offset = 4,
          .length = 16466,
          .cardinality = 11493,
          .positions = AllContainerPositions(),
          .ranges =
              {
                  {Position(0, 1, 1), Position(0, 1, 1000)},
                  {Position(1, 1, 10), Position(1, 1, 500)},
              },
      }});
}

}  // namespace iceberg::puffin
