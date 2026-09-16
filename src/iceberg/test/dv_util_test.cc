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

#include <zconf.h>
#include <zlib.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/deletes/dv_util_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/json_serde_internal.h"
#include "iceberg/puffin/puffin_format.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

constexpr std::string_view kReferencedDataFile = "data.parquet";
constexpr std::string_view kReferencedDataFileProperty = "referenced-data-file";
constexpr std::string_view kCardinalityProperty = "cardinality";

struct MetadataTestCase {
  std::string name;
  std::function<void(std::vector<uint8_t>&)> mutate_data;
  std::function<void(puffin::Blob&)> mutate_blob;
  std::function<void(DataFile&)> mutate_manifest;
  std::string expected_error;
};

void AddTrailingBitmapData(std::vector<uint8_t>& data) {
  constexpr size_t kLengthPrefixBytes = 4;
  constexpr size_t kCrcBytes = 4;

  data.insert(data.end() - kCrcBytes, 0);
  const auto length = static_cast<int32_t>(data.size() - kLengthPrefixBytes - kCrcBytes);
  WriteBigEndian(length, data.data());

  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, reinterpret_cast<const Bytef*>(data.data() + kLengthPrefixBytes),
              static_cast<uInt>(length));
  WriteBigEndian(static_cast<uint32_t>(crc), data.data() + data.size() - kCrcBytes);
}

Result<std::shared_ptr<DataFile>> WriteDVFixture(const std::shared_ptr<MockFileIO>& io,
                                                 const MetadataTestCase& test_case) {
  PositionDeleteIndex positions;
  positions.Delete(1);
  positions.Delete(3);
  positions.Delete(5);
  ICEBERG_ASSIGN_OR_RAISE(auto data, positions.Serialize());
  if (test_case.mutate_data) {
    test_case.mutate_data(data);
  }

  puffin::Blob blob{
      .type = std::string(puffin::StandardBlobTypes::kDeletionVectorV1),
      .input_fields = {MetadataColumns::kFilePositionColumnId},
      .snapshot_id = -1,
      .sequence_number = -1,
      .data = std::move(data),
      .requested_compression = puffin::PuffinCompressionCodec::kNone,
      .properties =
          {
              {std::string(kReferencedDataFileProperty),
               std::string(kReferencedDataFile)},
              {std::string(kCardinalityProperty), "3"},
          },
  };
  if (test_case.mutate_blob) {
    test_case.mutate_blob(blob);
  }

  const std::string path = "memory://" + test_case.name + ".puffin";
  const auto codec =
      blob.requested_compression.value_or(puffin::PuffinCompressionCodec::kNone);
  puffin::BlobMetadata blob_metadata{
      .type = blob.type,
      .input_fields = blob.input_fields,
      .snapshot_id = blob.snapshot_id,
      .sequence_number = blob.sequence_number,
      .offset = puffin::PuffinFormat::kMagicLength,
      .length = static_cast<int64_t>(blob.data.size()),
      .compression_codec = std::string(puffin::CodecName(codec)),
      .properties = blob.properties,
  };
  const std::string footer =
      puffin::ToJsonString(puffin::FileMetadata{.blobs = {blob_metadata}});

  std::vector<std::byte> file_data;
  auto append = [&file_data](const void* data, size_t size) {
    auto bytes = std::span(reinterpret_cast<const std::byte*>(data), size);
    file_data.insert(file_data.end(), bytes.begin(), bytes.end());
  };
  append(puffin::PuffinFormat::kMagicV1.data(), puffin::PuffinFormat::kMagicV1.size());
  append(blob.data.data(), blob.data.size());
  append(puffin::PuffinFormat::kMagicV1.data(), puffin::PuffinFormat::kMagicV1.size());
  append(footer.data(), footer.size());

  std::array<std::byte, puffin::PuffinFormat::kFooterStructLength> footer_struct{};
  WriteLittleEndian(static_cast<int32_t>(footer.size()), footer_struct.data());
  std::memcpy(footer_struct.data() + puffin::PuffinFormat::kFooterStructMagicOffset,
              puffin::PuffinFormat::kMagicV1.data(),
              puffin::PuffinFormat::kMagicV1.size());
  append(footer_struct.data(), footer_struct.size());
  io->AddFile(path, file_data);

  auto delete_file = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = path,
      .file_format = FileFormatType::kPuffin,
      .record_count = 3,
      .file_size_in_bytes = static_cast<int64_t>(file_data.size()),
      .referenced_data_file = std::string(kReferencedDataFile),
      .content_offset = blob_metadata.offset,
      .content_size_in_bytes = blob_metadata.length,
  });
  if (test_case.mutate_manifest) {
    test_case.mutate_manifest(*delete_file);
  }
  return delete_file;
}

}  // namespace

TEST(DVUtilTest, RejectsMalformedPuffinMetadata) {
  const std::vector<MetadataTestCase> cases = {
      {
          .name = "wrong-offset",
          .mutate_manifest = [](DataFile& file) { ++*file.content_offset; },
          .expected_error = "No Puffin blob starts",
      },
      {
          .name = "wrong-length",
          .mutate_manifest = [](DataFile& file) { --*file.content_size_in_bytes; },
          .expected_error = "manifest content_size_in_bytes",
      },
      {
          .name = "missing-manifest-reference",
          .mutate_manifest = [](DataFile& file) { file.referenced_data_file.reset(); },
          .expected_error = "requires referenced_data_file",
      },
      {
          .name = "empty-manifest-reference",
          .mutate_manifest = [](DataFile& file) { file.referenced_data_file = ""; },
          .expected_error = "requires referenced_data_file",
      },
      {
          .name = "wrong-type",
          .mutate_blob = [](puffin::Blob& blob) { blob.type = "test-blob"; },
          .expected_error = "Invalid deletion vector blob type",
      },
      {
          .name = "wrong-snapshot",
          .mutate_blob = [](puffin::Blob& blob) { blob.snapshot_id = 0; },
          .expected_error = "snapshot-id -1",
      },
      {
          .name = "wrong-sequence",
          .mutate_blob = [](puffin::Blob& blob) { blob.sequence_number = 0; },
          .expected_error = "sequence-number -1",
      },
      {
          .name = "compressed",
          .mutate_blob =
              [](puffin::Blob& blob) {
                blob.requested_compression = puffin::PuffinCompressionCodec::kZstd;
              },
          .expected_error = "must not be compressed",
      },
      {
          .name = "missing-reference",
          .mutate_blob =
              [](puffin::Blob& blob) {
                blob.properties.erase(std::string(kReferencedDataFileProperty));
              },
          .expected_error = "requires non-empty 'referenced-data-file'",
      },
      {
          .name = "empty-reference",
          .mutate_blob =
              [](puffin::Blob& blob) {
                blob.properties[std::string(kReferencedDataFileProperty)] = "";
              },
          .expected_error = "requires non-empty 'referenced-data-file'",
      },
      {
          .name = "mismatched-reference",
          .mutate_blob =
              [](puffin::Blob& blob) {
                blob.properties[std::string(kReferencedDataFileProperty)] =
                    "other.parquet";
              },
          .expected_error = "does not match Puffin",
      },
      {
          .name = "missing-cardinality",
          .mutate_blob =
              [](puffin::Blob& blob) {
                blob.properties.erase(std::string(kCardinalityProperty));
              },
          .expected_error = "requires 'cardinality'",
      },
      {
          .name = "invalid-cardinality",
          .mutate_blob =
              [](puffin::Blob& blob) {
                blob.properties[std::string(kCardinalityProperty)] = "three";
              },
          .expected_error = "Failed to parse",
      },
      {
          .name = "negative-cardinality",
          .mutate_blob =
              [](puffin::Blob& blob) {
                blob.properties[std::string(kCardinalityProperty)] = "-1";
              },
          .expected_error = "must be non-negative",
      },
      {
          .name = "mismatched-cardinality",
          .mutate_blob =
              [](puffin::Blob& blob) {
                blob.properties[std::string(kCardinalityProperty)] = "4";
              },
          .expected_error = "Manifest record_count 3",
      },
      {
          .name = "trailing-bitmap-data",
          .mutate_data = AddTrailingBitmapData,
          .expected_error = "Trailing data after bitmaps",
      },
  };

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    auto io = std::make_shared<MockFileIO>();
    ICEBERG_UNWRAP_OR_FAIL(auto delete_file, WriteDVFixture(io, test_case));
    EXPECT_THAT(DVUtil::ReadDV(delete_file, io),
                HasErrorMessage(test_case.expected_error));
  }
}

TEST(DVUtilTest, ReadsValidPuffinMetadata) {
  auto io = std::make_shared<MockFileIO>();
  MetadataTestCase test_case{.name = "valid"};
  ICEBERG_UNWRAP_OR_FAIL(auto delete_file, WriteDVFixture(io, test_case));
  ICEBERG_UNWRAP_OR_FAIL(auto positions, DVUtil::ReadDV(delete_file, io));

  EXPECT_EQ(positions.Cardinality(), 3);
  EXPECT_TRUE(positions.IsDeleted(1));
  EXPECT_TRUE(positions.IsDeleted(3));
  EXPECT_TRUE(positions.IsDeleted(5));
}

TEST(DVUtilTest, ReadsUsingActualFileSize) {
  auto io = std::make_shared<MockFileIO>();
  MetadataTestCase test_case{
      .name = "stale-manifest-file-size",
      .mutate_manifest = [](DataFile& file) { file.file_size_in_bytes = 0; },
  };
  ICEBERG_UNWRAP_OR_FAIL(auto delete_file, WriteDVFixture(io, test_case));
  ICEBERG_UNWRAP_OR_FAIL(auto positions, DVUtil::ReadDV(delete_file, io));

  EXPECT_EQ(positions.Cardinality(), 3);
}

}  // namespace iceberg
