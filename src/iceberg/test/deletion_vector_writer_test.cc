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

#include "iceberg/data/deletion_vector_writer.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/data/delete_loader.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/partition_spec.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_reader.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/util/macros.h"
#include "iceberg/version.h"

namespace iceberg {

namespace {

std::shared_ptr<DataFile> FindByReferencedFile(
    const std::vector<std::shared_ptr<DataFile>>& files, const std::string& ref) {
  for (const auto& file : files) {
    if (file->referenced_data_file == ref) {
      return file;
    }
  }
  return nullptr;
}

std::shared_ptr<PartitionSpec> UnpartitionedSpec() {
  return PartitionSpec::Unpartitioned();
}

const puffin::BlobMetadata* FindBlobByReferencedFile(
    const std::vector<puffin::BlobMetadata>& blobs, const std::string& ref) {
  for (const auto& blob : blobs) {
    auto it = blob.properties.find("referenced-data-file");
    if (it != blob.properties.end() && it->second == ref) {
      return &blob;
    }
  }
  return nullptr;
}

// A load_previous_deletes callback for data files that have no existing deletes.
Result<std::optional<PositionDeleteIndex>> NoPreviousDeletes(std::string_view) {
  return std::nullopt;
}

DeletionVectorWriterOptions MakeDVWriterOptions(
    std::shared_ptr<MockFileIO> io, std::string path,
    std::function<Result<std::optional<PositionDeleteIndex>>(std::string_view)>
        load_previous_deletes = NoPreviousDeletes) {
  return DeletionVectorWriterOptions{
      .path = std::move(path),
      .io = std::move(io),
      .load_previous_deletes = std::move(load_previous_deletes),
  };
}

}  // namespace

TEST(DeletionVectorWriterTest, WriteThenLoadEndToEnd) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();

  std::vector<std::shared_ptr<DataFile>> delete_files;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer, DeletionVectorWriter::Make(MakeDVWriterOptions(
                                            io, "memory://deletes.puffin")));

    ASSERT_THAT(writer->Delete("data-a.parquet", 0, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Delete("data-a.parquet", 5, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Delete("data-a.parquet", 10, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Delete("data-b.parquet", 1, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Delete("data-b.parquet", 2, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Close(), IsOk());

    ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
    delete_files = result.data_files;
    // Each referenced data file is reported once.
    EXPECT_EQ(result.referenced_data_files.size(), 2u);
    // No previous deletes were loaded, so nothing was rewritten.
    EXPECT_TRUE(result.rewritten_delete_files.empty());
  }

  // One DataFile per referenced data file.
  ASSERT_EQ(delete_files.size(), 2u);

  auto dv_a = FindByReferencedFile(delete_files, "data-a.parquet");
  auto dv_b = FindByReferencedFile(delete_files, "data-b.parquet");
  ASSERT_NE(dv_a, nullptr);
  ASSERT_NE(dv_b, nullptr);

  EXPECT_EQ(dv_a->content, DataFile::Content::kPositionDeletes);
  EXPECT_EQ(dv_a->file_format, FileFormatType::kPuffin);
  EXPECT_TRUE(dv_a->IsDeletionVector());
  EXPECT_EQ(dv_a->file_path, "memory://deletes.puffin");
  EXPECT_EQ(dv_a->record_count, 3);
  EXPECT_TRUE(dv_a->content_offset.has_value());
  EXPECT_TRUE(dv_a->content_size_in_bytes.has_value());
  EXPECT_GT(dv_a->file_size_in_bytes, 0);
  EXPECT_EQ(dv_a->partition_spec_id, spec->spec_id());
  EXPECT_EQ(dv_b->record_count, 2);

  // Both blobs live in the same Puffin file but at different offsets.
  EXPECT_EQ(dv_a->file_path, dv_b->file_path);
  EXPECT_NE(dv_a->content_offset.value(), dv_b->content_offset.value());

  ICEBERG_UNWRAP_OR_FAIL(auto input_file, io->NewInputFile("memory://deletes.puffin"));
  ICEBERG_UNWRAP_OR_FAIL(auto puffin_reader,
                         puffin::PuffinReader::Make(std::move(input_file)));
  ICEBERG_UNWRAP_OR_FAIL(auto puffin_metadata, puffin_reader->ReadFileMetadata());
  ASSERT_EQ(puffin_metadata.blobs.size(), 2u);
  EXPECT_EQ(puffin_metadata.properties.at("created-by"), ICEBERG_FULL_VERSION_STRING);

  auto assert_blob = [](const puffin::BlobMetadata& blob, const DataFile& file,
                        const std::string& ref, const std::string& cardinality) {
    EXPECT_EQ(blob.type, puffin::StandardBlobTypes::kDeletionVectorV1);
    EXPECT_EQ(blob.input_fields,
              std::vector<int32_t>{MetadataColumns::kFilePositionColumnId});
    EXPECT_EQ(blob.snapshot_id, -1);
    EXPECT_EQ(blob.sequence_number, -1);
    EXPECT_TRUE(blob.compression_codec.empty());
    EXPECT_EQ(blob.offset, file.content_offset.value());
    EXPECT_EQ(blob.length, file.content_size_in_bytes.value());
    EXPECT_EQ(blob.properties.at("referenced-data-file"), ref);
    EXPECT_EQ(blob.properties.at("cardinality"), cardinality);
  };
  ASSERT_NE(FindBlobByReferencedFile(puffin_metadata.blobs, "data-a.parquet"), nullptr);
  ASSERT_NE(FindBlobByReferencedFile(puffin_metadata.blobs, "data-b.parquet"), nullptr);
  assert_blob(*FindBlobByReferencedFile(puffin_metadata.blobs, "data-a.parquet"), *dv_a,
              "data-a.parquet", "3");
  assert_blob(*FindBlobByReferencedFile(puffin_metadata.blobs, "data-b.parquet"), *dv_b,
              "data-b.parquet", "2");

  // Load each data file's DV separately.
  DeleteLoader loader(io);
  {
    auto result = loader.LoadPositionDeletes({&dv_a, 1}, "data-a.parquet");
    ASSERT_THAT(result, IsOk());
    auto& index = result.value();
    EXPECT_EQ(index.Cardinality(), 3);
    EXPECT_TRUE(index.IsDeleted(0));
    EXPECT_TRUE(index.IsDeleted(5));
    EXPECT_TRUE(index.IsDeleted(10));
    EXPECT_FALSE(index.IsDeleted(1));
  }

  {
    auto result = loader.LoadPositionDeletes({&dv_b, 1}, "data-b.parquet");
    ASSERT_THAT(result, IsOk());
    auto& index = result.value();
    EXPECT_EQ(index.Cardinality(), 2);
    EXPECT_TRUE(index.IsDeleted(1));
    EXPECT_TRUE(index.IsDeleted(2));
    EXPECT_FALSE(index.IsDeleted(0));
  }
}

// The PositionDeleteIndex overload bulk-adds positions for a data file.
TEST(DeletionVectorWriterTest, DeleteFromIndex) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();

  PositionDeleteIndex positions;
  positions.Delete(0);
  positions.Delete(3, 6);  // [3, 6) -> 3, 4, 5

  ICEBERG_UNWRAP_OR_FAIL(auto writer, DeletionVectorWriter::Make(MakeDVWriterOptions(
                                          io, "memory://from-index.puffin")));
  ASSERT_THAT(writer->Delete("data.parquet", positions, spec, PartitionValues{}), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  ASSERT_EQ(result.data_files.size(), 1u);
  EXPECT_EQ(result.data_files[0]->record_count, 4);

  DeleteLoader loader(io);
  auto loaded = loader.LoadPositionDeletes(result.data_files, "data.parquet");
  ASSERT_THAT(loaded, IsOk());
  EXPECT_EQ(loaded.value().Cardinality(), 4);
  EXPECT_TRUE(loaded.value().IsDeleted(0));
  EXPECT_TRUE(loaded.value().IsDeleted(5));
  EXPECT_FALSE(loaded.value().IsDeleted(6));
}

// Previously written deletes are merged into the new vector, and the file-scoped
// delete files they came from are reported as rewritten.
TEST(DeletionVectorWriterTest, LoadPreviousDeletesMergesAndReportsRewritten) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();

  // Build a previous DV index carrying its source (file-scoped) delete file,
  // the same way DeleteLoader would produce it.
  PositionDeleteIndex previous_positions;
  previous_positions.Delete(100);
  previous_positions.Delete(200);
  ICEBERG_UNWRAP_OR_FAIL(auto previous_blob, previous_positions.Serialize());
  auto previous_dv = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "memory://old.puffin",
      .file_format = FileFormatType::kPuffin,
      .record_count = 2,
      .referenced_data_file = "data.parquet",
      .content_offset = 0,
      .content_size_in_bytes = static_cast<int64_t>(previous_blob.size()),
  });

  ICEBERG_UNWRAP_OR_FAIL(
      auto writer,
      DeletionVectorWriter::Make(MakeDVWriterOptions(
          io, "memory://merged.puffin",
          [&](std::string_view path) -> Result<std::optional<PositionDeleteIndex>> {
            if (path != "data.parquet") {
              return std::nullopt;
            }
            ICEBERG_ASSIGN_OR_RAISE(
                auto index, PositionDeleteIndex::Deserialize(previous_blob, previous_dv));
            return std::optional<PositionDeleteIndex>(std::move(index));
          })));

  ASSERT_THAT(writer->Delete("data.parquet", 0, spec, PartitionValues{}), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  ASSERT_EQ(result.data_files.size(), 1u);
  // New position plus the two previous positions.
  EXPECT_EQ(result.data_files[0]->record_count, 3);
  // The previous DV is file-scoped, so it is reported for removal.
  ASSERT_EQ(result.rewritten_delete_files.size(), 1u);
  EXPECT_EQ(result.rewritten_delete_files[0]->file_path, "memory://old.puffin");

  DeleteLoader loader(io);
  auto loaded = loader.LoadPositionDeletes(result.data_files, "data.parquet");
  ASSERT_THAT(loaded, IsOk());
  EXPECT_EQ(loaded.value().Cardinality(), 3);
  EXPECT_TRUE(loaded.value().IsDeleted(0));
  EXPECT_TRUE(loaded.value().IsDeleted(100));
  EXPECT_TRUE(loaded.value().IsDeleted(200));
}

TEST(DeletionVectorWriterTest, PreviousDeletesWithoutSourceFilesAreNotRewritten) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();

  ICEBERG_UNWRAP_OR_FAIL(
      auto writer,
      DeletionVectorWriter::Make(MakeDVWriterOptions(
          io, "memory://merged-partition.puffin",
          [&](std::string_view path) -> Result<std::optional<PositionDeleteIndex>> {
            PositionDeleteIndex index;
            index.Delete(50);
            return std::optional<PositionDeleteIndex>(std::move(index));
          })));

  ASSERT_THAT(writer->Delete("data.parquet", 0, spec, PartitionValues{}), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  ASSERT_EQ(result.data_files.size(), 1u);
  // The previous position was merged in.
  EXPECT_EQ(result.data_files[0]->record_count, 2);
  // The previous delete is partition-scoped, so it is not rewritten.
  EXPECT_TRUE(result.rewritten_delete_files.empty());

  DeleteLoader loader(io);
  auto loaded = loader.LoadPositionDeletes(result.data_files, "data.parquet");
  ASSERT_THAT(loaded, IsOk());
  EXPECT_EQ(loaded.value().Cardinality(), 2);
  EXPECT_TRUE(loaded.value().IsDeleted(0));
  EXPECT_TRUE(loaded.value().IsDeleted(50));
}

TEST(DeletionVectorWriterTest, EmptyWriterProducesNoDataFiles) {
  auto io = std::make_shared<MockFileIO>();
  ICEBERG_UNWRAP_OR_FAIL(auto writer,
                         DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                             .path = "memory://empty.puffin",
                             .io = io,
                             .load_previous_deletes = NoPreviousDeletes}));
  ASSERT_THAT(writer->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  EXPECT_TRUE(result.data_files.empty());
  EXPECT_THAT(io->NewInputFile("memory://empty.puffin"), IsError(ErrorKind::kNotFound));
}

TEST(DeletionVectorWriterTest, DeleteRejectsEmptyReferencedFile) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();
  ICEBERG_UNWRAP_OR_FAIL(auto writer, DeletionVectorWriter::Make(MakeDVWriterOptions(
                                          io, "memory://invalid.puffin")));
  EXPECT_THAT(writer->Delete("", 0, spec, PartitionValues{}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(writer->Delete("data.parquet", 0, nullptr, PartitionValues{}),
              IsError(ErrorKind::kInvalidArgument));
  PositionDeleteIndex positions;
  positions.Delete(0);
  EXPECT_THAT(writer->Delete("data.parquet", positions, nullptr, PartitionValues{}),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorWriterTest, DeleteRejectsOutOfRangePosition) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();
  ICEBERG_UNWRAP_OR_FAIL(auto writer, DeletionVectorWriter::Make(MakeDVWriterOptions(
                                          io, "memory://invalid.puffin")));
  // Negative and out-of-range positions are rejected rather than silently
  // dropped by the underlying bitmap.
  EXPECT_THAT(writer->Delete("data.parquet", -1, spec, PartitionValues{}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(writer->Delete("data.parquet", RoaringPositionBitmap::kMaxPosition + 1,
                             spec, PartitionValues{}),
              IsError(ErrorKind::kInvalidArgument));
}

// Close propagates a load_previous_deletes failure and returns no metadata.
TEST(DeletionVectorWriterTest, ClosePropagatesLoadPreviousDeletesError) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();
  ICEBERG_UNWRAP_OR_FAIL(
      auto writer,
      DeletionVectorWriter::Make(MakeDVWriterOptions(
          io, "memory://err.puffin",
          [](std::string_view) -> Result<std::optional<PositionDeleteIndex>> {
            return IOError("boom");
          })));

  ASSERT_THAT(writer->Delete("data.parquet", 0, spec, PartitionValues{}), IsOk());
  EXPECT_THAT(writer->Close(), IsError(ErrorKind::kIOError));
  // No partial delete metadata is exposed after a failed Close.
  EXPECT_THAT(writer->Metadata(), IsError(ErrorKind::kValidationFailed));
  // No orphan Puffin file was created.
  EXPECT_THAT(io->NewInputFile("memory://err.puffin"), IsError(ErrorKind::kNotFound));
}

TEST(DeletionVectorWriterTest, MakeRejectsMissingOutputPath) {
  EXPECT_THAT(DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                  .io = std::make_shared<MockFileIO>(),
                  .load_previous_deletes = NoPreviousDeletes}),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorWriterTest, MakeRejectsMissingLoadPreviousDeletes) {
  auto io = std::make_shared<MockFileIO>();
  EXPECT_THAT(DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                  .path = "x.puffin",
                  .io = io,
              }),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorWriterTest, DeleteAfterCloseFails) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();
  ICEBERG_UNWRAP_OR_FAIL(auto writer, DeletionVectorWriter::Make(MakeDVWriterOptions(
                                          io, "memory://closed.puffin")));
  ASSERT_THAT(writer->Close(), IsOk());
  EXPECT_THAT(writer->Delete("data-a.parquet", 0, spec, PartitionValues{}),
              IsError(ErrorKind::kValidationFailed));
}

}  // namespace iceberg
