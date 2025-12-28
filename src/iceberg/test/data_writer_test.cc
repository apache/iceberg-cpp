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

#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow_c_data.h"
#include "iceberg/data/writer.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/test/matchers.h"

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

}  // namespace iceberg
