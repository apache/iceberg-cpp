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
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
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

std::shared_ptr<PartitionSpec> UnpartitionedSpec() {
  return PartitionSpec::Unpartitioned();
}

}  // namespace

// Full write -> read round trip: write deletion vectors with the writer, then
// load them back through DeleteLoader using the produced DataFile metadata.
TEST(DeletionVectorWriterTest, WriteThenLoadEndToEnd) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();

  std::vector<std::shared_ptr<DataFile>> delete_files;
  {
    ICEBERG_UNWRAP_OR_FAIL(auto writer,
                           DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                               .path = "memory://deletes.puffin",
                               .io = io,
                               .properties = {{"created-by", "iceberg-cpp-test"}},
                           }));

    ASSERT_THAT(writer->Delete("data-a.parquet", 0, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Delete("data-a.parquet", 5, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Delete("data-a.parquet", 10, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Delete("data-b.parquet", 1, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Delete("data-b.parquet", 2, spec, PartitionValues{}), IsOk());
    ASSERT_THAT(writer->Close(), IsOk());

    ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
    delete_files = result.delete_files;
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

  // Metadata is spec-compliant for a deletion vector.
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

  // Read back through DeleteLoader for data-a.parquet.
  DeleteLoader loader(io);
  {
    auto result = loader.LoadPositionDeletes(delete_files, "data-a.parquet");
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
    auto result = loader.LoadPositionDeletes(delete_files, "data-b.parquet");
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

  ICEBERG_UNWRAP_OR_FAIL(auto writer,
                         DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                             .path = "memory://from-index.puffin", .io = io}));
  ASSERT_THAT(writer->Delete("data.parquet", positions, spec, PartitionValues{}), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  ASSERT_EQ(result.delete_files.size(), 1u);
  EXPECT_EQ(result.delete_files[0]->record_count, 4);

  DeleteLoader loader(io);
  auto loaded = loader.LoadPositionDeletes(result.delete_files, "data.parquet");
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

  auto previous_dv = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "memory://old.puffin",
      .file_format = FileFormatType::kPuffin,
      .referenced_data_file = "data.parquet",
  });

  ICEBERG_UNWRAP_OR_FAIL(
      auto writer,
      DeletionVectorWriter::Make(DeletionVectorWriterOptions{
          .path = "memory://merged.puffin",
          .io = io,
          .load_previous_deletes = [&](std::string_view path) -> Result<PreviousDeletes> {
            if (path != "data.parquet") {
              return PreviousDeletes{};
            }
            auto index = std::make_shared<PositionDeleteIndex>();
            index->Delete(100);
            index->Delete(200);
            return PreviousDeletes{.index = index, .delete_files = {previous_dv}};
          },
      }));

  ASSERT_THAT(writer->Delete("data.parquet", 0, spec, PartitionValues{}), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  ASSERT_EQ(result.delete_files.size(), 1u);
  // New position plus the two previous positions.
  EXPECT_EQ(result.delete_files[0]->record_count, 3);
  // The previous DV is file-scoped, so it is reported for removal.
  ASSERT_EQ(result.rewritten_delete_files.size(), 1u);
  EXPECT_EQ(result.rewritten_delete_files[0]->file_path, "memory://old.puffin");

  DeleteLoader loader(io);
  auto loaded = loader.LoadPositionDeletes(result.delete_files, "data.parquet");
  ASSERT_THAT(loaded, IsOk());
  EXPECT_EQ(loaded.value().Cardinality(), 3);
  EXPECT_TRUE(loaded.value().IsDeleted(0));
  EXPECT_TRUE(loaded.value().IsDeleted(100));
  EXPECT_TRUE(loaded.value().IsDeleted(200));
}

// A previous delete that is not file-scoped (e.g. a partition-scoped position
// delete) is merged into the new vector but is NOT reported as rewritten.
TEST(DeletionVectorWriterTest, PartitionScopedPreviousDeleteMergesButNotRewritten) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();

  // No referenced_data_file and no equal file_path bounds -> not file-scoped.
  auto previous_position_delete = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "memory://partition-deletes.parquet",
      .file_format = FileFormatType::kParquet,
  });

  ICEBERG_UNWRAP_OR_FAIL(
      auto writer,
      DeletionVectorWriter::Make(DeletionVectorWriterOptions{
          .path = "memory://merged-partition.puffin",
          .io = io,
          .load_previous_deletes = [&](std::string_view path) -> Result<PreviousDeletes> {
            auto index = std::make_shared<PositionDeleteIndex>();
            index->Delete(50);
            return PreviousDeletes{.index = index,
                                   .delete_files = {previous_position_delete}};
          },
      }));

  ASSERT_THAT(writer->Delete("data.parquet", 0, spec, PartitionValues{}), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  ASSERT_EQ(result.delete_files.size(), 1u);
  // The previous position was merged in.
  EXPECT_EQ(result.delete_files[0]->record_count, 2);
  // The previous delete is partition-scoped, so it is not rewritten.
  EXPECT_TRUE(result.rewritten_delete_files.empty());

  DeleteLoader loader(io);
  auto loaded = loader.LoadPositionDeletes(result.delete_files, "data.parquet");
  ASSERT_THAT(loaded, IsOk());
  EXPECT_EQ(loaded.value().Cardinality(), 2);
  EXPECT_TRUE(loaded.value().IsDeleted(0));
  EXPECT_TRUE(loaded.value().IsDeleted(50));
}

TEST(DeletionVectorWriterTest, EmptyWriterProducesNoDataFiles) {
  auto io = std::make_shared<MockFileIO>();
  ICEBERG_UNWRAP_OR_FAIL(auto writer,
                         DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                             .path = "memory://empty.puffin", .io = io}));
  ASSERT_THAT(writer->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto result, writer->Metadata());
  EXPECT_TRUE(result.delete_files.empty());

  // No blobs were written, so no (orphan) Puffin file should have been created.
  EXPECT_THAT(io->NewInputFile("memory://empty.puffin"), IsError(ErrorKind::kNotFound));
}

TEST(DeletionVectorWriterTest, DeleteRejectsInvalidPosition) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = UnpartitionedSpec();
  ICEBERG_UNWRAP_OR_FAIL(auto writer,
                         DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                             .path = "memory://invalid.puffin", .io = io}));
  EXPECT_THAT(writer->Delete("", 0, spec, PartitionValues{}),
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
  auto spec = UnpartitionedSpec();
  ICEBERG_UNWRAP_OR_FAIL(auto writer,
                         DeletionVectorWriter::Make(DeletionVectorWriterOptions{
                             .path = "memory://closed.puffin", .io = io}));
  ASSERT_THAT(writer->Close(), IsOk());
  EXPECT_THAT(writer->Delete("data-a.parquet", 0, spec, PartitionValues{}),
              IsError(ErrorKind::kValidationFailed));
}

}  // namespace iceberg
