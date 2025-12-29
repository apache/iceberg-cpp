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

#include "iceberg/data/data_writer.h"

#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow_c_data.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/file_writer_factory.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/data/writer.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"

namespace iceberg {

// Mock implementation of FileWriter for testing
class MockFileWriter : public FileWriter {
 public:
  MockFileWriter() = default;

  Status Write(ArrowArray* data) override {
    if (is_closed_) {
      return Invalid("Writer is closed");
    }
    if (data == nullptr) {
      return Invalid("Null data provided");
    }
    write_count_++;
    // Simulate writing some bytes
    bytes_written_ += 1024;
    return {};
  }

  Result<int64_t> Length() const override { return bytes_written_; }

  Status Close() override {
    if (is_closed_) {
      return Invalid("Writer already closed");
    }
    is_closed_ = true;
    return {};
  }

  Result<WriteResult> Metadata() override {
    if (!is_closed_) {
      return Invalid("Writer must be closed before getting metadata");
    }

    WriteResult result;
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "/test/data/file.parquet";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = write_count_ * 100;
    data_file->file_size_in_bytes = bytes_written_;
    result.data_files.push_back(data_file);

    return result;
  }

  bool is_closed() const { return is_closed_; }
  int32_t write_count() const { return write_count_; }

 private:
  int64_t bytes_written_ = 0;
  bool is_closed_ = false;
  int32_t write_count_ = 0;
};

TEST(FileWriterTest, BasicWriteOperation) {
  MockFileWriter writer;

  // Create a dummy ArrowArray (normally this would contain actual data)
  ArrowArray dummy_array = {};

  ASSERT_THAT(writer.Write(&dummy_array), IsOk());
  ASSERT_EQ(writer.write_count(), 1);

  auto length_result = writer.Length();
  ASSERT_THAT(length_result, IsOk());
  ASSERT_EQ(*length_result, 1024);
}

TEST(FileWriterTest, MultipleWrites) {
  MockFileWriter writer;
  ArrowArray dummy_array = {};

  // Write multiple times
  for (int i = 0; i < 5; i++) {
    ASSERT_THAT(writer.Write(&dummy_array), IsOk());
  }

  ASSERT_EQ(writer.write_count(), 5);

  auto length_result = writer.Length();
  ASSERT_THAT(length_result, IsOk());
  ASSERT_EQ(*length_result, 5120);  // 5 * 1024
}

TEST(FileWriterTest, WriteNullData) {
  MockFileWriter writer;

  auto status = writer.Write(nullptr);
  ASSERT_THAT(status, HasErrorMessage("Null data provided"));
}

TEST(FileWriterTest, CloseWriter) {
  MockFileWriter writer;
  ArrowArray dummy_array = {};

  ASSERT_THAT(writer.Write(&dummy_array), IsOk());
  ASSERT_FALSE(writer.is_closed());

  ASSERT_THAT(writer.Close(), IsOk());
  ASSERT_TRUE(writer.is_closed());
}

TEST(FileWriterTest, DoubleClose) {
  MockFileWriter writer;

  ASSERT_THAT(writer.Close(), IsOk());
  auto status = writer.Close();
  ASSERT_THAT(status, HasErrorMessage("Writer already closed"));
}

TEST(FileWriterTest, WriteAfterClose) {
  MockFileWriter writer;
  ArrowArray dummy_array = {};

  ASSERT_THAT(writer.Close(), IsOk());

  auto status = writer.Write(&dummy_array);
  ASSERT_THAT(status, HasErrorMessage("Writer is closed"));
}

TEST(FileWriterTest, MetadataBeforeClose) {
  MockFileWriter writer;
  ArrowArray dummy_array = {};

  ASSERT_THAT(writer.Write(&dummy_array), IsOk());

  auto metadata_result = writer.Metadata();
  ASSERT_THAT(metadata_result,
              HasErrorMessage("Writer must be closed before getting metadata"));
}

TEST(FileWriterTest, MetadataAfterClose) {
  MockFileWriter writer;
  ArrowArray dummy_array = {};

  // Write some data
  ASSERT_THAT(writer.Write(&dummy_array), IsOk());
  ASSERT_THAT(writer.Write(&dummy_array), IsOk());
  ASSERT_THAT(writer.Write(&dummy_array), IsOk());

  // Close the writer
  ASSERT_THAT(writer.Close(), IsOk());

  // Get metadata
  auto metadata_result = writer.Metadata();
  ASSERT_THAT(metadata_result, IsOk());

  const auto& result = *metadata_result;
  ASSERT_EQ(result.data_files.size(), 1);

  const auto& data_file = result.data_files[0];
  ASSERT_EQ(data_file->file_path, "/test/data/file.parquet");
  ASSERT_EQ(data_file->file_format, FileFormatType::kParquet);
  ASSERT_EQ(data_file->record_count, 300);         // 3 writes * 100 records
  ASSERT_EQ(data_file->file_size_in_bytes, 3072);  // 3 * 1024
}

TEST(FileWriterTest, WriteResultStructure) {
  FileWriter::WriteResult result;

  // Test that WriteResult can hold multiple data files
  auto data_file1 = std::make_shared<DataFile>();
  data_file1->file_path = "/test/data/file1.parquet";
  data_file1->record_count = 100;

  auto data_file2 = std::make_shared<DataFile>();
  data_file2->file_path = "/test/data/file2.parquet";
  data_file2->record_count = 200;

  result.data_files.push_back(data_file1);
  result.data_files.push_back(data_file2);

  ASSERT_EQ(result.data_files.size(), 2);
  ASSERT_EQ(result.data_files[0]->file_path, "/test/data/file1.parquet");
  ASSERT_EQ(result.data_files[0]->record_count, 100);
  ASSERT_EQ(result.data_files[1]->file_path, "/test/data/file2.parquet");
  ASSERT_EQ(result.data_files[1]->record_count, 200);
}

TEST(FileWriterTest, EmptyWriteResult) {
  FileWriter::WriteResult result;
  ASSERT_EQ(result.data_files.size(), 0);
  ASSERT_TRUE(result.data_files.empty());
}

// Tests for stub implementations (methods return NotImplemented)
class WriterStubTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeRequired(2, "data", string())});

    ICEBERG_UNWRAP_OR_FAIL(spec_, PartitionSpec::Make(0, {}));
    io_ = nullptr;  // Not needed for stub tests
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<FileIO> io_;
};

TEST_F(WriterStubTest, DataWriterMethodsReturnNotImplemented) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewDataWriter("/test/data/file.parquet",
                                             FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());

  auto& writer = *writer_result;
  ArrowArray dummy_array = {};

  // All methods should return NotImplemented
  ASSERT_THAT(writer->Write(&dummy_array),
              HasErrorMessage("DataWriter not yet implemented"));
  ASSERT_THAT(writer->Length(), HasErrorMessage("DataWriter not yet implemented"));
  ASSERT_THAT(writer->Close(), HasErrorMessage("DataWriter not yet implemented"));
  ASSERT_THAT(writer->Metadata(), HasErrorMessage("DataWriter not yet implemented"));
}

TEST_F(WriterStubTest, PositionDeleteWriterMethodsReturnNotImplemented) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewPositionDeleteWriter(
      "/test/deletes/pos_deletes.parquet", FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());

  auto& writer = *writer_result;
  ArrowArray dummy_array = {};

  // All methods should return NotImplemented
  ASSERT_THAT(writer->Write(&dummy_array),
              HasErrorMessage("PositionDeleteWriter not yet implemented"));
  ASSERT_THAT(writer->WriteDelete("/test/file.parquet", 0),
              HasErrorMessage("PositionDeleteWriter not yet implemented"));
  ASSERT_THAT(writer->Length(),
              HasErrorMessage("PositionDeleteWriter not yet implemented"));
  ASSERT_THAT(writer->Close(),
              HasErrorMessage("PositionDeleteWriter not yet implemented"));
  ASSERT_THAT(writer->Metadata(),
              HasErrorMessage("PositionDeleteWriter not yet implemented"));
}

TEST_F(WriterStubTest, EqualityDeleteWriterMethodsReturnNotImplemented) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});

  PartitionValues partition;
  auto writer_result = factory.NewEqualityDeleteWriter(
      "/test/deletes/eq_deletes.parquet", FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());

  auto& writer = *writer_result;
  ArrowArray dummy_array = {};

  // All methods should return NotImplemented
  ASSERT_THAT(writer->Write(&dummy_array),
              HasErrorMessage("EqualityDeleteWriter not yet implemented"));
  ASSERT_THAT(writer->Length(),
              HasErrorMessage("EqualityDeleteWriter not yet implemented"));
  ASSERT_THAT(writer->Close(),
              HasErrorMessage("EqualityDeleteWriter not yet implemented"));
  ASSERT_THAT(writer->Metadata(),
              HasErrorMessage("EqualityDeleteWriter not yet implemented"));

  // equality_field_ids should return the configured value
  ASSERT_EQ(writer->equality_field_ids(), std::vector<int32_t>({1, 2}));
}

// Tests for FileWriterFactory
class FileWriterFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeRequired(2, "data", string()),
                                 SchemaField::MakeRequired(3, "category", string())});

    ICEBERG_UNWRAP_OR_FAIL(
        spec_, PartitionSpec::Make(
                   0, {PartitionField(3, 1000, "category", Transform::Identity())}));
    io_ = nullptr;  // FileIO not needed for stub tests
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<FileIO> io_;
};

TEST_F(FileWriterFactoryTest, ConstructorWithNullProperties) {
  FileWriterFactory factory(schema_, spec_, io_, nullptr);
  // Should construct successfully without properties
}

TEST_F(FileWriterFactoryTest, NewDataWriterCreatesWriter) {
  FileWriterFactory factory(schema_, spec_, io_);

  PartitionValues partition;
  auto result = factory.NewDataWriter("/test/data/file.parquet", FileFormatType::kParquet,
                                      partition);

  // Factory should successfully create a writer
  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryTest, NewDataWriterWithSortOrder) {
  FileWriterFactory factory(schema_, spec_, io_);

  PartitionValues partition;
  auto result = factory.NewDataWriter("/test/data/file.parquet", FileFormatType::kParquet,
                                      partition,
                                      /*sort_order_id=*/1);

  // Factory should successfully create a writer
  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryTest, NewPositionDeleteWriterCreatesWriter) {
  FileWriterFactory factory(schema_, spec_, io_);

  PartitionValues partition;
  auto result = factory.NewPositionDeleteWriter("/test/deletes/pos_deletes.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryTest, NewPositionDeleteWriterWithRowSchema) {
  FileWriterFactory factory(schema_, spec_, io_);

  auto row_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
  factory.SetPositionDeleteRowSchema(row_schema);

  PartitionValues partition;
  auto result = factory.NewPositionDeleteWriter("/test/deletes/pos_deletes.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryTest, NewEqualityDeleteWriterCreatesWriter) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});  // Must set config first

  PartitionValues partition;
  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq_deletes.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryTest, NewEqualityDeleteWriterWithConfig) {
  FileWriterFactory factory(schema_, spec_, io_);

  auto eq_delete_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeRequired(2, "data", string())});
  std::vector<int32_t> equality_field_ids = {1, 2};
  factory.SetEqualityDeleteConfig(eq_delete_schema, equality_field_ids);

  PartitionValues partition;
  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq_deletes.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryTest, NewEqualityDeleteWriterWithSortOrder) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});  // Must set config first

  PartitionValues partition;
  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq_deletes.parquet",
                                                FileFormatType::kParquet, partition,
                                                /*sort_order_id=*/1);

  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryTest, NewEqualityDeleteWriterUsesDefaultSchema) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(nullptr, {1, 2});  // Set field IDs but no custom schema

  // Don't set custom equality delete schema - should use default schema
  PartitionValues partition;
  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq_deletes.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryTest, NewEqualityDeleteWriterUsesCustomSchema) {
  FileWriterFactory factory(schema_, spec_, io_);

  auto custom_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
  std::vector<int32_t> equality_field_ids = {1};
  factory.SetEqualityDeleteConfig(custom_schema, equality_field_ids);

  PartitionValues partition;
  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq_deletes.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, IsOk());
}

// Tests for input validation
class FileWriterFactoryInputValidationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeRequired(2, "data", string())});
    ICEBERG_UNWRAP_OR_FAIL(spec_, PartitionSpec::Make(0, {}));
    io_ = nullptr;
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<FileIO> io_;
};

TEST_F(FileWriterFactoryInputValidationTest, NewDataWriterRejectsEmptyPath) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto result = factory.NewDataWriter("", FileFormatType::kParquet, partition);

  ASSERT_THAT(result, HasErrorMessage("Path cannot be empty"));
}

TEST_F(FileWriterFactoryInputValidationTest, NewDataWriterRejectsNullSchema) {
  FileWriterFactory factory(nullptr, spec_, io_);
  PartitionValues partition;

  auto result = factory.NewDataWriter("/test/data/file.parquet", FileFormatType::kParquet,
                                      partition);

  ASSERT_THAT(result, HasErrorMessage("Schema cannot be null"));
}

TEST_F(FileWriterFactoryInputValidationTest, NewDataWriterRejectsNullSpec) {
  FileWriterFactory factory(schema_, nullptr, io_);
  PartitionValues partition;

  auto result = factory.NewDataWriter("/test/data/file.parquet", FileFormatType::kParquet,
                                      partition);

  ASSERT_THAT(result, HasErrorMessage("PartitionSpec cannot be null"));
}

TEST_F(FileWriterFactoryInputValidationTest, NewPositionDeleteWriterRejectsEmptyPath) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto result = factory.NewPositionDeleteWriter("", FileFormatType::kParquet, partition);

  ASSERT_THAT(result, HasErrorMessage("Path cannot be empty"));
}

TEST_F(FileWriterFactoryInputValidationTest, NewPositionDeleteWriterRejectsNullSchema) {
  FileWriterFactory factory(nullptr, spec_, io_);
  PartitionValues partition;

  auto result = factory.NewPositionDeleteWriter("/test/deletes/pos.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, HasErrorMessage("Schema cannot be null"));
}

TEST_F(FileWriterFactoryInputValidationTest, NewPositionDeleteWriterRejectsNullSpec) {
  FileWriterFactory factory(schema_, nullptr, io_);
  PartitionValues partition;

  auto result = factory.NewPositionDeleteWriter("/test/deletes/pos.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, HasErrorMessage("PartitionSpec cannot be null"));
}

TEST_F(FileWriterFactoryInputValidationTest, NewEqualityDeleteWriterRejectsEmptyPath) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});
  PartitionValues partition;

  auto result = factory.NewEqualityDeleteWriter("", FileFormatType::kParquet, partition);

  ASSERT_THAT(result, HasErrorMessage("Path cannot be empty"));
}

TEST_F(FileWriterFactoryInputValidationTest, NewEqualityDeleteWriterRejectsNullSchema) {
  FileWriterFactory factory(nullptr, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});
  PartitionValues partition;

  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, HasErrorMessage("Schema cannot be null"));
}

TEST_F(FileWriterFactoryInputValidationTest, NewEqualityDeleteWriterRejectsNullSpec) {
  FileWriterFactory factory(schema_, nullptr, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});
  PartitionValues partition;

  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, HasErrorMessage("PartitionSpec cannot be null"));
}

TEST_F(FileWriterFactoryInputValidationTest,
       NewEqualityDeleteWriterRejectsEmptyFieldIds) {
  FileWriterFactory factory(schema_, spec_, io_);
  // Don't set equality config, so field IDs will be empty
  PartitionValues partition;

  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq.parquet",
                                                FileFormatType::kParquet, partition);

  ASSERT_THAT(result, HasErrorMessage("Equality field IDs cannot be empty"));
}

// Tests for state management in stub writers
class WriterStubStateManagementTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeRequired(2, "data", string())});
    ICEBERG_UNWRAP_OR_FAIL(spec_, PartitionSpec::Make(0, {}));
    io_ = nullptr;
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<FileIO> io_;
};

TEST_F(WriterStubStateManagementTest, DataWriterRejectsWriteAfterClose) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewDataWriter("/test/data/file.parquet",
                                             FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  // Close the writer
  ASSERT_THAT(writer->Close(), HasErrorMessage("DataWriter not yet implemented"));

  // Now try to write - should fail because writer is closed
  ArrowArray dummy_array = {};
  auto status = writer->Write(&dummy_array);
  ASSERT_THAT(status, HasErrorMessage("Writer is already closed"));
}

TEST_F(WriterStubStateManagementTest, DataWriterCloseIsIdempotent) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewDataWriter("/test/data/file.parquet",
                                             FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  // Close multiple times - second close should succeed (idempotent)
  ASSERT_THAT(writer->Close(), HasErrorMessage("DataWriter not yet implemented"));
  ASSERT_THAT(writer->Close(), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
}

TEST_F(WriterStubStateManagementTest, DataWriterRejectsNullData) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewDataWriter("/test/data/file.parquet",
                                             FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  auto status = writer->Write(nullptr);
  ASSERT_THAT(status, HasErrorMessage("Cannot write null data"));
}

TEST_F(WriterStubStateManagementTest, PositionDeleteWriterRejectsWriteAfterClose) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewPositionDeleteWriter(
      "/test/deletes/pos.parquet", FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  // Close the writer
  ASSERT_THAT(writer->Close(),
              HasErrorMessage("PositionDeleteWriter not yet implemented"));

  // Try to write - should fail
  ArrowArray dummy_array = {};
  auto status = writer->Write(&dummy_array);
  ASSERT_THAT(status, HasErrorMessage("Writer is already closed"));
}

TEST_F(WriterStubStateManagementTest, PositionDeleteWriterRejectsWriteDeleteAfterClose) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewPositionDeleteWriter(
      "/test/deletes/pos.parquet", FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  // Close the writer
  ASSERT_THAT(writer->Close(),
              HasErrorMessage("PositionDeleteWriter not yet implemented"));

  // Try WriteDelete - should fail
  auto status = writer->WriteDelete("/test/file.parquet", 100);
  ASSERT_THAT(status, HasErrorMessage("Writer is already closed"));
}

TEST_F(WriterStubStateManagementTest, PositionDeleteWriterRejectsNullData) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewPositionDeleteWriter(
      "/test/deletes/pos.parquet", FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  auto status = writer->Write(nullptr);
  ASSERT_THAT(status, HasErrorMessage("Cannot write null data"));
}

TEST_F(WriterStubStateManagementTest, PositionDeleteWriterRejectsEmptyFilePath) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto writer_result = factory.NewPositionDeleteWriter(
      "/test/deletes/pos.parquet", FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  auto status = writer->WriteDelete("", 100);
  ASSERT_THAT(status, HasErrorMessage("File path cannot be empty"));
}

TEST_F(WriterStubStateManagementTest, EqualityDeleteWriterRejectsWriteAfterClose) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});
  PartitionValues partition;

  auto writer_result = factory.NewEqualityDeleteWriter(
      "/test/deletes/eq.parquet", FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  // Close the writer
  ASSERT_THAT(writer->Close(),
              HasErrorMessage("EqualityDeleteWriter not yet implemented"));

  // Try to write - should fail
  ArrowArray dummy_array = {};
  auto status = writer->Write(&dummy_array);
  ASSERT_THAT(status, HasErrorMessage("Writer is already closed"));
}

TEST_F(WriterStubStateManagementTest, EqualityDeleteWriterRejectsNullData) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});
  PartitionValues partition;

  auto writer_result = factory.NewEqualityDeleteWriter(
      "/test/deletes/eq.parquet", FileFormatType::kParquet, partition);
  ASSERT_THAT(writer_result, IsOk());
  auto& writer = *writer_result;

  auto status = writer->Write(nullptr);
  ASSERT_THAT(status, HasErrorMessage("Cannot write null data"));
}

// Tests for different file formats
class FileWriterFactoryFileFormatTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
    ICEBERG_UNWRAP_OR_FAIL(spec_, PartitionSpec::Make(0, {}));
    io_ = nullptr;
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<FileIO> io_;
};

TEST_F(FileWriterFactoryFileFormatTest, DataWriterSupportsParquet) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto result = factory.NewDataWriter("/test/data/file.parquet", FileFormatType::kParquet,
                                      partition);
  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryFileFormatTest, DataWriterSupportsAvro) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto result =
      factory.NewDataWriter("/test/data/file.avro", FileFormatType::kAvro, partition);
  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryFileFormatTest, PositionDeleteWriterSupportsParquet) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto result = factory.NewPositionDeleteWriter("/test/deletes/pos.parquet",
                                                FileFormatType::kParquet, partition);
  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryFileFormatTest, PositionDeleteWriterSupportsAvro) {
  FileWriterFactory factory(schema_, spec_, io_);
  PartitionValues partition;

  auto result = factory.NewPositionDeleteWriter("/test/deletes/pos.avro",
                                                FileFormatType::kAvro, partition);
  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryFileFormatTest, EqualityDeleteWriterSupportsParquet) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1});
  PartitionValues partition;

  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq.parquet",
                                                FileFormatType::kParquet, partition);
  ASSERT_THAT(result, IsOk());
}

TEST_F(FileWriterFactoryFileFormatTest, EqualityDeleteWriterSupportsAvro) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1});
  PartitionValues partition;

  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq.avro",
                                                FileFormatType::kAvro, partition);
  ASSERT_THAT(result, IsOk());
}

// Edge case tests
class WriterEdgeCaseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeRequired(2, "data", string())});
    ICEBERG_UNWRAP_OR_FAIL(spec_, PartitionSpec::Make(0, {}));
    io_ = nullptr;
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<FileIO> io_;
};

TEST_F(WriterEdgeCaseTest, EqualityDeleteWriterWithSingleFieldId) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1});  // Single field ID

  PartitionValues partition;
  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq.parquet",
                                                FileFormatType::kParquet, partition);
  ASSERT_THAT(result, IsOk());

  auto& writer = *result;
  ASSERT_EQ(writer->equality_field_ids(), std::vector<int32_t>({1}));
}

TEST_F(WriterEdgeCaseTest, EqualityDeleteWriterWithMultipleFieldIds) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});  // Multiple field IDs

  PartitionValues partition;
  auto result = factory.NewEqualityDeleteWriter("/test/deletes/eq.parquet",
                                                FileFormatType::kParquet, partition);
  ASSERT_THAT(result, IsOk());

  auto& writer = *result;
  ASSERT_EQ(writer->equality_field_ids(), std::vector<int32_t>({1, 2}));
}

TEST_F(WriterEdgeCaseTest, MultipleWritersFromSameFactory) {
  FileWriterFactory factory(schema_, spec_, io_);
  factory.SetEqualityDeleteConfig(schema_, {1, 2});

  PartitionValues partition;

  // Create multiple writers from the same factory
  auto data_writer1 =
      factory.NewDataWriter("/test/data1.parquet", FileFormatType::kParquet, partition);
  auto data_writer2 =
      factory.NewDataWriter("/test/data2.parquet", FileFormatType::kParquet, partition);
  auto pos_writer = factory.NewPositionDeleteWriter("/test/pos.parquet",
                                                    FileFormatType::kParquet, partition);
  auto eq_writer = factory.NewEqualityDeleteWriter("/test/eq.parquet",
                                                   FileFormatType::kParquet, partition);

  ASSERT_THAT(data_writer1, IsOk());
  ASSERT_THAT(data_writer2, IsOk());
  ASSERT_THAT(pos_writer, IsOk());
  ASSERT_THAT(eq_writer, IsOk());
}

TEST_F(WriterEdgeCaseTest, FactoryWithPartitionedSpec) {
  auto partitioned_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeRequired(2, "category", string()),
                               SchemaField::MakeRequired(3, "data", string())});

  auto partitioned_spec_result = PartitionSpec::Make(
      0, {PartitionField(2, 1000, "category", Transform::Identity())});
  ASSERT_THAT(partitioned_spec_result, IsOk());
  auto partitioned_spec =
      std::shared_ptr<PartitionSpec>(std::move(*partitioned_spec_result));

  FileWriterFactory factory(partitioned_schema, partitioned_spec, io_);

  PartitionValues partition;
  partition.AddValue(Literal::String("electronics"));

  auto result = factory.NewDataWriter("/test/data/category=electronics/file.parquet",
                                      FileFormatType::kParquet, partition);
  ASSERT_THAT(result, IsOk());
}

TEST_F(WriterEdgeCaseTest, ReconfigureEqualityDeleteConfig) {
  FileWriterFactory factory(schema_, spec_, io_);

  // Set initial config
  factory.SetEqualityDeleteConfig(schema_, {1});

  PartitionValues partition;
  auto writer1 = factory.NewEqualityDeleteWriter("/test/eq1.parquet",
                                                 FileFormatType::kParquet, partition);
  ASSERT_THAT(writer1, IsOk());
  ASSERT_EQ((*writer1)->equality_field_ids(), std::vector<int32_t>({1}));

  // Reconfigure with different field IDs
  factory.SetEqualityDeleteConfig(schema_, {1, 2});

  auto writer2 = factory.NewEqualityDeleteWriter("/test/eq2.parquet",
                                                 FileFormatType::kParquet, partition);
  ASSERT_THAT(writer2, IsOk());
  ASSERT_EQ((*writer2)->equality_field_ids(), std::vector<int32_t>({1, 2}));
}

TEST_F(WriterEdgeCaseTest, ReconfigurePositionDeleteRowSchema) {
  FileWriterFactory factory(schema_, spec_, io_);

  auto row_schema1 = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
  factory.SetPositionDeleteRowSchema(row_schema1);

  PartitionValues partition;
  auto writer1 = factory.NewPositionDeleteWriter("/test/pos1.parquet",
                                                 FileFormatType::kParquet, partition);
  ASSERT_THAT(writer1, IsOk());

  // Reconfigure with different row schema
  auto row_schema2 = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeRequired(2, "data", string())});
  factory.SetPositionDeleteRowSchema(row_schema2);

  auto writer2 = factory.NewPositionDeleteWriter("/test/pos2.parquet",
                                                 FileFormatType::kParquet, partition);
  ASSERT_THAT(writer2, IsOk());
}

}  // namespace iceberg
