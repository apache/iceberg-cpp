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

#include "iceberg/data/delete_loader.h"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/puffin/deletion_vector.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/struct_like_set.h"

namespace iceberg {

class DeleteLoaderTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }

  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "name", string())});
    partition_spec_ = PartitionSpec::Unpartitioned();
    loader_ = std::make_unique<DeleteLoader>(file_io_);
  }

  /// Write position deletes using WriteDelete API and return the DataFile metadata.
  std::shared_ptr<DataFile> WritePositionDeletes(
      const std::string& path,
      const std::vector<std::pair<std::string, int64_t>>& deletes) {
    PositionDeleteWriterOptions options{
        .path = path,
        .schema = schema_,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .flush_threshold = 10000,
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };

    auto writer = PositionDeleteWriter::Make(options).value();
    for (const auto& [file_path, pos] : deletes) {
      ICEBERG_THROW_NOT_OK(writer->WriteDelete(file_path, pos));
    }
    ICEBERG_THROW_NOT_OK(writer->Close());
    return writer->Metadata().value().data_files[0];
  }

  /// Write equality deletes and return the DataFile metadata.
  std::shared_ptr<DataFile> WriteEqualityDeletes(
      const std::string& path, const std::string& json_data,
      std::vector<int32_t> equality_field_ids = {1, 2}) {
    EqualityDeleteWriterOptions options{
        .path = path,
        .schema = schema_,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .equality_field_ids = std::move(equality_field_ids),
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };

    auto writer = EqualityDeleteWriter::Make(options).value();

    ArrowSchema arrow_c_schema;
    ICEBERG_THROW_NOT_OK(ToArrowSchema(*schema_, &arrow_c_schema));
    auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
    auto test_data = ::arrow::json::ArrayFromJSONString(
                         ::arrow::struct_(arrow_type->fields()), json_data)
                         .ValueOrDie();
    ArrowArray arrow_array;
    auto export_status = ::arrow::ExportArray(*test_data, &arrow_array);
    if (!export_status.ok()) throw std::runtime_error(export_status.ToString());
    ICEBERG_THROW_NOT_OK(writer->Write(&arrow_array));
    ICEBERG_THROW_NOT_OK(writer->Close());
    return writer->Metadata().value().data_files[0];
  }

  /// Write a deletion vector to a Puffin file and return the DataFile metadata.
  std::shared_ptr<DataFile> WriteDeletionVector(const std::string& path,
                                                const std::string& referenced_data_file,
                                                const std::vector<int64_t>& positions) {
    RoaringPositionBitmap bitmap;
    for (int64_t pos : positions) {
      bitmap.Add(pos);
    }

    auto output_file = file_io_->NewOutputFile(path).value();
    auto writer = puffin::PuffinWriter::Make(std::move(output_file)).value();
    auto blob = puffin::MakeDeletionVectorBlob(bitmap, referenced_data_file).value();
    auto blob_metadata = writer->Write(blob).value();
    ICEBERG_THROW_NOT_OK(writer->Finish());

    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = path,
        .file_format = FileFormatType::kPuffin,
        .record_count = static_cast<int64_t>(bitmap.Cardinality()),
        .referenced_data_file = referenced_data_file,
        .content_offset = blob_metadata.offset,
        .content_size_in_bytes = blob_metadata.length,
    });
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> partition_spec_;
  std::unique_ptr<DeleteLoader> loader_;
};

// --- Position Delete Tests ---

TEST_F(DeleteLoaderTest, LoadPositionDeletesEmpty) {
  std::vector<std::shared_ptr<DataFile>> empty;
  auto result = loader_->LoadPositionDeletes(empty, "data.parquet");
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result->IsEmpty());
}

TEST_F(DeleteLoaderTest, LoadPositionDeletesSingleFile) {
  auto delete_file = WritePositionDeletes(
      "pos_deletes_1.parquet",
      {{"data.parquet", 0}, {"data.parquet", 5}, {"data.parquet", 10}});

  std::vector<std::shared_ptr<DataFile>> files = {delete_file};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsOk());

  auto& index = result.value();
  ASSERT_FALSE(index.IsEmpty());
  ASSERT_EQ(index.Cardinality(), 3);
  ASSERT_TRUE(index.IsDeleted(0));
  ASSERT_TRUE(index.IsDeleted(5));
  ASSERT_TRUE(index.IsDeleted(10));
  ASSERT_FALSE(index.IsDeleted(1));
  ASSERT_FALSE(index.IsDeleted(6));
}

TEST_F(DeleteLoaderTest, LoadPositionDeletesMultipleFiles) {
  auto file1 = WritePositionDeletes("pos_deletes_a.parquet",
                                    {{"data.parquet", 1}, {"data.parquet", 3}});
  auto file2 = WritePositionDeletes("pos_deletes_b.parquet",
                                    {{"data.parquet", 5}, {"data.parquet", 7}});

  std::vector<std::shared_ptr<DataFile>> files = {file1, file2};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsOk());

  auto& index = result.value();
  ASSERT_EQ(index.Cardinality(), 4);
  ASSERT_TRUE(index.IsDeleted(1));
  ASSERT_TRUE(index.IsDeleted(3));
  ASSERT_TRUE(index.IsDeleted(5));
  ASSERT_TRUE(index.IsDeleted(7));
}

TEST_F(DeleteLoaderTest, LoadPositionDeletesFiltersByDataFilePath) {
  auto delete_file =
      WritePositionDeletes("pos_deletes_mixed.parquet", {{"data_a.parquet", 0},
                                                         {"data_a.parquet", 2},
                                                         {"data_b.parquet", 10},
                                                         {"data_b.parquet", 20}});

  // Mixed paths -> writer must NOT set the hint, forcing the loader's
  // per-row filter path. Locks the routing in case the writer behavior changes.
  ASSERT_FALSE(delete_file->referenced_data_file.has_value());

  std::vector<std::shared_ptr<DataFile>> files = {delete_file};

  // Load only positions for data_a.parquet
  auto result_a = loader_->LoadPositionDeletes(files, "data_a.parquet");
  ASSERT_THAT(result_a, IsOk());
  ASSERT_EQ(result_a.value().Cardinality(), 2);
  ASSERT_TRUE(result_a.value().IsDeleted(0));
  ASSERT_TRUE(result_a.value().IsDeleted(2));
  ASSERT_FALSE(result_a.value().IsDeleted(10));

  // Load only positions for data_b.parquet
  auto result_b = loader_->LoadPositionDeletes(files, "data_b.parquet");
  ASSERT_THAT(result_b, IsOk());
  ASSERT_EQ(result_b.value().Cardinality(), 2);
  ASSERT_TRUE(result_b.value().IsDeleted(10));
  ASSERT_TRUE(result_b.value().IsDeleted(20));
  ASSERT_FALSE(result_b.value().IsDeleted(0));

  // Load for non-existent file
  auto result_none = loader_->LoadPositionDeletes(files, "nonexistent.parquet");
  ASSERT_THAT(result_none, IsOk());
  ASSERT_TRUE(result_none.value().IsEmpty());
}

TEST_F(DeleteLoaderTest, LoadPositionDeletesSkipsMismatchedReferencedDataFile) {
  auto delete_file = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "missing-pos-delete.parquet",
      .file_format = FileFormatType::kParquet,
      .referenced_data_file = "other-data.parquet",
  });

  std::vector<std::shared_ptr<DataFile>> files = {delete_file};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result.value().IsEmpty());
}

TEST_F(DeleteLoaderTest, LoadPositionDeletesFastPathHonorsReferencedDataFile) {
  // Single-file writes -> writer sets referenced_data_file -> loader takes
  // the zero-copy fast path. Sized above the consumer's 64-element sniff
  // threshold so the dispatcher's real coalesce/bulk logic runs end-to-end,
  // not just the small-input shortcut covered by LoadPositionDeletesSingleFile.
  constexpr int64_t kRowCount = 128;
  std::vector<std::pair<std::string, int64_t>> deletes;
  deletes.reserve(kRowCount);
  for (int64_t i = 0; i < kRowCount; ++i) {
    deletes.emplace_back("data.parquet", i);
  }
  auto delete_file = WritePositionDeletes("pos_deletes_fast_path.parquet", deletes);

  ASSERT_TRUE(delete_file->referenced_data_file.has_value());
  ASSERT_EQ(delete_file->referenced_data_file.value(), "data.parquet");

  std::vector<std::shared_ptr<DataFile>> files = {delete_file};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsOk());

  auto& index = result.value();
  ASSERT_EQ(index.Cardinality(), kRowCount);
  ASSERT_TRUE(index.IsDeleted(0));
  ASSERT_TRUE(index.IsDeleted(kRowCount / 2));
  ASSERT_TRUE(index.IsDeleted(kRowCount - 1));
  ASSERT_FALSE(index.IsDeleted(kRowCount));
}

TEST_F(DeleteLoaderTest, LoadDeletionVector) {
  auto dv_file =
      WriteDeletionVector("dv-a.puffin", "data.parquet", {0, 5, 10, 4'000'000'000LL});

  std::vector<std::shared_ptr<DataFile>> files = {dv_file};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsOk());

  auto& index = result.value();
  ASSERT_EQ(index.Cardinality(), 4);
  ASSERT_TRUE(index.IsDeleted(0));
  ASSERT_TRUE(index.IsDeleted(5));
  ASSERT_TRUE(index.IsDeleted(10));
  ASSERT_TRUE(index.IsDeleted(4'000'000'000LL));
  ASSERT_FALSE(index.IsDeleted(1));
}

TEST_F(DeleteLoaderTest, LoadDeletionVectorSkipsMismatchedReferencedDataFile) {
  auto dv_file = WriteDeletionVector("dv-b.puffin", "other-data.parquet", {1, 2, 3});

  std::vector<std::shared_ptr<DataFile>> files = {dv_file};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result.value().IsEmpty());
}

TEST_F(DeleteLoaderTest, LoadDeletionVectorRequiresContentOffsetAndSize) {
  auto dv_file = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "dv.puffin",
      .file_format = FileFormatType::kPuffin,
      .referenced_data_file = "data.parquet",
  });
  std::vector<std::shared_ptr<DataFile>> files = {dv_file};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

TEST_F(DeleteLoaderTest, LoadDeletionVectorRejectsCardinalityMismatch) {
  auto dv_file = WriteDeletionVector("dv-card.puffin", "data.parquet", {0, 1, 2});
  // Corrupt the recorded cardinality so it no longer matches the bitmap.
  dv_file->record_count = 99;

  std::vector<std::shared_ptr<DataFile>> files = {dv_file};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

// Iceberg uses either a deletion vector or position deletes for a data file, not
// both. This exercises the loader's robustness: a mixed list is merged into one
// index, with each source filtered by the target data file path.
TEST_F(DeleteLoaderTest, LoadMixedDeletionVectorAndPositionDeletes) {
  auto dv = WriteDeletionVector("dv-mixed.puffin", "data.parquet", {0, 5});
  auto pos = WritePositionDeletes("pos-mixed.parquet",
                                  {{"data.parquet", 10}, {"data.parquet", 20}});

  std::vector<std::shared_ptr<DataFile>> files = {dv, pos};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsOk());

  auto& index = result.value();
  EXPECT_EQ(index.Cardinality(), 4);
  EXPECT_TRUE(index.IsDeleted(0));
  EXPECT_TRUE(index.IsDeleted(5));
  EXPECT_TRUE(index.IsDeleted(10));
  EXPECT_TRUE(index.IsDeleted(20));
  EXPECT_FALSE(index.IsDeleted(1));
}

TEST_F(DeleteLoaderTest, LoadPositionDeletesRejectsWrongContent) {
  auto data_file = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kData,
      .file_path = "data.parquet",
      .file_format = FileFormatType::kParquet,
  });
  std::vector<std::shared_ptr<DataFile>> files = {data_file};
  auto result = loader_->LoadPositionDeletes(files, "data.parquet");
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

// --- Equality Delete Tests ---

TEST_F(DeleteLoaderTest, LoadEqualityDeletesEmpty) {
  std::vector<std::shared_ptr<DataFile>> empty;
  StructType eq_type(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
  });

  auto result = loader_->LoadEqualityDeletes(empty, eq_type);
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result.value()->IsEmpty());
  ASSERT_EQ(result.value()->Size(), 0);
}

TEST_F(DeleteLoaderTest, LoadEqualityDeletesSingleFile) {
  auto delete_file =
      WriteEqualityDeletes("eq_deletes_1.parquet", R"([[1, "Alice"], [2, "Bob"]])");

  StructType eq_type(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
  });

  std::vector<std::shared_ptr<DataFile>> files = {delete_file};
  auto result = loader_->LoadEqualityDeletes(files, eq_type);
  ASSERT_THAT(result, IsOk());

  ASSERT_FALSE(result.value()->IsEmpty());
  ASSERT_EQ(result.value()->Size(), 2);
}

TEST_F(DeleteLoaderTest, LoadEqualityDeletesMultipleFiles) {
  auto file1 = WriteEqualityDeletes("eq_deletes_a.parquet", R"([[1, "Alice"]])");
  auto file2 =
      WriteEqualityDeletes("eq_deletes_b.parquet", R"([[2, "Bob"], [3, "Charlie"]])");

  StructType eq_type(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(2, "name", string()),
  });

  std::vector<std::shared_ptr<DataFile>> files = {file1, file2};
  auto result = loader_->LoadEqualityDeletes(files, eq_type);
  ASSERT_THAT(result, IsOk());
  ASSERT_EQ(result.value()->Size(), 3);
}

TEST_F(DeleteLoaderTest, LoadEqualityDeletesRejectsWrongContent) {
  auto data_file = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kData,
      .file_path = "data.parquet",
      .file_format = FileFormatType::kParquet,
  });
  StructType eq_type(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
  });

  std::vector<std::shared_ptr<DataFile>> files = {data_file};
  auto result = loader_->LoadEqualityDeletes(files, eq_type);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

}  // namespace iceberg
