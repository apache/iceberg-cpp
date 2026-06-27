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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/data/delete_loader.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"

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

}  // namespace

// Full write -> read round trip: write deletion vectors with the writer, then
// load them back through DeleteLoader using the produced DataFile metadata.
TEST(DeletionVectorWriterTest, WriteThenLoadEndToEnd) {
  auto io = std::make_shared<MockFileIO>();

  std::vector<std::shared_ptr<DataFile>> data_files;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer,
                           DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                               .path = "memory://deletes.puffin",
                               .io = io,
                               .properties = {{"created-by", "iceberg-cpp-test"}},
                           }));

    ASSERT_THAT(writer->Delete("data-a.parquet", 0), IsOk());
    ASSERT_THAT(writer->Delete("data-a.parquet", 5), IsOk());
    ASSERT_THAT(writer->Delete("data-a.parquet", 10), IsOk());
    ASSERT_THAT(writer->Delete("data-b.parquet", 1), IsOk());
    ASSERT_THAT(writer->Delete("data-b.parquet", 2), IsOk());
    ASSERT_THAT(writer->Close(), IsOk());

    ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
    data_files = result.data_files;
  }

  // One DataFile per referenced data file.
  ASSERT_EQ(data_files.size(), 2u);

  auto dv_a = FindByReferencedFile(data_files, "data-a.parquet");
  auto dv_b = FindByReferencedFile(data_files, "data-b.parquet");
  ASSERT_NE(dv_a, nullptr);
  ASSERT_NE(dv_b, nullptr);

  // Metadata is spec-compliant for a deletion vector.
  EXPECT_EQ(dv_a->content, DataFile::Content::kPositionDeletes);
  EXPECT_EQ(dv_a->file_format, FileFormatType::kPuffin);
  EXPECT_TRUE(dv_a->IsDeletionVector());
  EXPECT_EQ(dv_a->file_path, "memory://deletes.puffin");
  EXPECT_EQ(dv_a->record_count, 3);
  EXPECT_TRUE(dv_a->content_offset.has_value());
  EXPECT_TRUE(dv_a->content_size_in_bytes.has_value());
  EXPECT_GT(dv_a->file_size_in_bytes, 0);
  EXPECT_EQ(dv_b->record_count, 2);

  // Both blobs live in the same Puffin file but at different offsets.
  EXPECT_EQ(dv_a->file_path, dv_b->file_path);
  EXPECT_NE(dv_a->content_offset.value(), dv_b->content_offset.value());

  // Read back through DeleteLoader for data-a.parquet.
  DeleteLoader loader(io);
  {
    auto result = loader.LoadPositionDeletes(data_files, "data-a.parquet");
    ASSERT_THAT(result, IsOk());
    auto& index = result.value();
    EXPECT_EQ(index.Cardinality(), 3);
    EXPECT_TRUE(index.IsDeleted(0));
    EXPECT_TRUE(index.IsDeleted(5));
    EXPECT_TRUE(index.IsDeleted(10));
    EXPECT_FALSE(index.IsDeleted(1));
  }

  // And for data-b.parquet (the loader filters by referenced_data_file).
  {
    auto result = loader.LoadPositionDeletes(data_files, "data-b.parquet");
    ASSERT_THAT(result, IsOk());
    auto& index = result.value();
    EXPECT_EQ(index.Cardinality(), 2);
    EXPECT_TRUE(index.IsDeleted(1));
    EXPECT_TRUE(index.IsDeleted(2));
    EXPECT_FALSE(index.IsDeleted(0));
  }
}

TEST(DeletionVectorWriterTest, EmptyWriterProducesNoDataFiles) {
  auto io = std::make_shared<MockFileIO>();
  ICEBERG_UNWRAP_OR_FAIL(auto writer,
                         DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                             .path = "memory://empty.puffin", .io = io}));
  ASSERT_THAT(writer->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  EXPECT_TRUE(result.data_files.empty());

  // No blobs were written, so no (orphan) Puffin file should have been created.
  EXPECT_THAT(io->NewInputFile("memory://empty.puffin"), IsError(ErrorKind::kNotFound));
}

TEST(DeletionVectorWriterTest, DeleteRejectsInvalidPosition) {
  auto io = std::make_shared<MockFileIO>();
  ICEBERG_UNWRAP_OR_FAIL(auto writer,
                         DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                             .path = "memory://invalid.puffin", .io = io}));
  EXPECT_THAT(writer->Delete("data-a.parquet", -1), IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(writer->Delete("data-a.parquet", RoaringPositionBitmap::kMaxPosition + 1),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorWriterTest, MakeRejectsNullIo) {
  EXPECT_THAT(DeletionVectorWriter::Make(DeletionVectorWriterOptions{.path = "x.puffin"}),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorWriterTest, MakeRejectsEmptyPath) {
  auto io = std::make_shared<MockFileIO>();
  EXPECT_THAT(DeletionVectorWriter::Make(DeletionVectorWriterOptions{.io = io}),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorWriterTest, DeleteAfterCloseFails) {
  auto io = std::make_shared<MockFileIO>();
  ICEBERG_UNWRAP_OR_FAIL(auto writer,
                         DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                             .path = "memory://closed.puffin", .io = io}));
  ASSERT_THAT(writer->Close(), IsOk());
  EXPECT_THAT(writer->Delete("data-a.parquet", 0), IsError(ErrorKind::kValidationFailed));
}

}  // namespace iceberg
