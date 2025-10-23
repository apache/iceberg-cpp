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

#include <arrow/filesystem/localfs.h>
#include <arrow/result.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_stream_internal.h"
#include "iceberg/test/temp_file_test_base.h"

namespace iceberg::avro {

class AVROStreamTest : public TempFileTestBase {
 public:
  void SetUp() override {
    TempFileTestBase::SetUp();
    temp_filepath_ = CreateNewTempFilePath();
    local_fs_ = std::make_shared<::arrow::fs::LocalFileSystem>();
  }

  std::shared_ptr<AvroOutputStream> CreateOutputStream(const std::string& path,
                                                       int64_t buffer_size) {
    auto arrow_out_ret = local_fs_->OpenOutputStream(path);
    if (!arrow_out_ret.ok()) {
      throw std::runtime_error("Failed to open output stream: " +
                               arrow_out_ret.status().message());
    }
    return std::make_shared<AvroOutputStream>(std::move(arrow_out_ret.ValueUnsafe()),
                                              buffer_size);
  }

  std::shared_ptr<AvroInputStream> CreateInputStream(const std::string& path,
                                                     int64_t buffer_size) {
    auto arrow_in_ret = local_fs_->OpenInputFile(path);
    if (!arrow_in_ret.ok()) {
      throw std::runtime_error("Failed to open input stream: " +
                               arrow_in_ret.status().message());
    }
    return std::make_shared<AvroInputStream>(std::move(arrow_in_ret.ValueUnsafe()),
                                             buffer_size);
  }

  void WriteDataToStream(const std::shared_ptr<AvroOutputStream>& avro_output_stream,
                         const std::string& data) {
    uint8_t* buf;
    size_t buf_size;
    ASSERT_TRUE(avro_output_stream->next(&buf, &buf_size));
    std::memcpy(buf, data.data(), data.size());
    avro_output_stream->backup(1024 - data.size());
    avro_output_stream->flush();
  }

  void ReadDataFromStream(const std::shared_ptr<AvroInputStream>& avro_input_stream,
                          std::string& data) {
    const uint8_t* buf{};
    size_t len{};
    ASSERT_TRUE(avro_input_stream->next(&buf, &len));
    data = std::string(reinterpret_cast<const char*>(buf), len);
  }

  void CheckStreamEof(const std::shared_ptr<AvroInputStream>& avro_input_stream) {
    const uint8_t* buf{};
    size_t len{};
    ASSERT_FALSE(avro_input_stream->next(&buf, &len));
  }

  int64_t buffer_size_ = 1024;
  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs_;
  std::string temp_filepath_;
};

TEST_F(AVROStreamTest, TestAvroBasicStream) {
  // Write test data
  const std::string test_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  auto avro_output_stream = CreateOutputStream(temp_filepath_, buffer_size_);
  WriteDataToStream(avro_output_stream, test_data);

  auto avro_input_stream = CreateInputStream(temp_filepath_, buffer_size_);

  const uint8_t* data{};
  size_t len{};
  ASSERT_TRUE(avro_input_stream->next(&data, &len));
  EXPECT_EQ(len, test_data.size());

  EXPECT_EQ(avro_input_stream->byteCount(), test_data.size());
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(data), len), test_data);
  ASSERT_FALSE(avro_input_stream->next(&data, &len));
}

TEST_F(AVROStreamTest, InputStreamBackup) {
  // Write test data
  const std::string test_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  auto avro_output_stream = CreateOutputStream(temp_filepath_, buffer_size_);
  WriteDataToStream(avro_output_stream, test_data);

  // Create a test input stream
  auto avro_input_stream = CreateInputStream(temp_filepath_, buffer_size_);

  // Read data
  const uint8_t* data{};
  size_t len{};
  ASSERT_TRUE(avro_input_stream->next(&data, &len));
  EXPECT_EQ(len, test_data.size());

  // Backup 10 bytes
  const size_t backupSize = 10;
  avro_input_stream->backup(backupSize);

  // Check byteCount after backup
  EXPECT_EQ(avro_input_stream->byteCount(), test_data.size() - backupSize);

  // Read the backed-up data again
  ASSERT_TRUE(avro_input_stream->next(&data, &len));
  EXPECT_EQ(len, backupSize);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(data), len),
            test_data.substr(test_data.size() - backupSize));

  // Check that we've reached the end of the stream
  ASSERT_FALSE(avro_input_stream->next(&data, &len));
}

TEST_F(AVROStreamTest, InputStreamSkip) {
  // Write test data
  const std::string test_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  auto avro_output_stream = CreateOutputStream(temp_filepath_, buffer_size_);
  WriteDataToStream(avro_output_stream, test_data);

  // Create a test input stream
  auto avro_input_stream = CreateInputStream(temp_filepath_, buffer_size_);

  // Skip the first 10 bytes
  const size_t skipSize = 10;
  avro_input_stream->skip(skipSize);

  // Check byteCount after skip
  EXPECT_EQ(avro_input_stream->byteCount(), skipSize);

  // Read the remaining data
  const uint8_t* data{};
  size_t len{};
  ASSERT_TRUE(avro_input_stream->next(&data, &len));
  EXPECT_EQ(len, test_data.size() - skipSize);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(data), len),
            test_data.substr(skipSize));

  // Check that we've reached the end of the stream
  ASSERT_FALSE(avro_input_stream->next(&data, &len));
}

TEST_F(AVROStreamTest, InputStreamSeek) {
  // Write test data
  const std::string test_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  auto avro_output_stream = CreateOutputStream(temp_filepath_, buffer_size_);
  WriteDataToStream(avro_output_stream, test_data);

  // Create a test input stream
  auto avro_input_stream = CreateInputStream(temp_filepath_, buffer_size_);

  // Seek to position 15
  const int64_t seekPos = 15;
  avro_input_stream->seek(seekPos);

  // Check byteCount after seek
  EXPECT_EQ(avro_input_stream->byteCount(), static_cast<size_t>(seekPos));

  // Read the remaining data
  const uint8_t* data{};
  size_t len{};
  ASSERT_TRUE(avro_input_stream->next(&data, &len));
  EXPECT_EQ(len, test_data.size() - seekPos);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(data), len),
            test_data.substr(seekPos));

  // Check that we've reached the end of the stream
  ASSERT_FALSE(avro_input_stream->next(&data, &len));
}

TEST_F(AVROStreamTest, OutputStreamBasic) {
  auto avro_output_stream = CreateOutputStream(temp_filepath_, buffer_size_);

  // Test next()
  uint8_t* data{};
  size_t len{};
  ASSERT_TRUE(avro_output_stream->next(&data, &len));
  EXPECT_EQ(len, static_cast<size_t>(buffer_size_));

  // Write some data
  const std::string test_data = "Hello, Avro!";
  std::memcpy(data, test_data.data(), test_data.size());

  // Backup unused bytes
  avro_output_stream->backup(len - test_data.size());

  // Check byteCount
  EXPECT_EQ(avro_output_stream->byteCount(), test_data.size());

  // Flush the data
  avro_output_stream->flush();

  // Verify the written data
  auto avro_input_stream = CreateInputStream(temp_filepath_, buffer_size_);
  std::string read_data;
  ReadDataFromStream(avro_input_stream, read_data);
  EXPECT_EQ(read_data, test_data);
  CheckStreamEof(avro_input_stream);
}

TEST_F(AVROStreamTest, OutputStreamMultipleWrites) {
  auto avro_output_stream = CreateOutputStream(temp_filepath_, buffer_size_);

  // Write first chunk
  const std::string chunk1 = "First chunk of data. ";
  uint8_t* data1{};
  size_t len1{};
  ASSERT_TRUE(avro_output_stream->next(&data1, &len1));
  std::memcpy(data1, chunk1.data(), chunk1.size());
  avro_output_stream->backup(len1 - chunk1.size());

  // Write second chunk
  const std::string chunk2 = "Second chunk of data.";
  uint8_t* data2{};
  size_t len2{};
  ASSERT_TRUE(avro_output_stream->next(&data2, &len2));
  std::memcpy(data2, chunk2.data(), chunk2.size());
  avro_output_stream->backup(len2 - chunk2.size());

  // Check byteCount
  EXPECT_EQ(avro_output_stream->byteCount(), chunk1.size() + chunk2.size());

  // Flush and close
  avro_output_stream->flush();

  // Verify the written data
  std::string expectedData = chunk1 + chunk2;
  auto avro_input_stream = CreateInputStream(temp_filepath_, buffer_size_);
  std::string read_data;
  ReadDataFromStream(avro_input_stream, read_data);
  EXPECT_EQ(read_data, expectedData);
  CheckStreamEof(avro_input_stream);
}

TEST_F(AVROStreamTest, InputStreamLargeData) {
  // Create a large test data set (larger than the buffer size)
  const size_t dataSize = buffer_size_ * 2.5;
  std::vector<char> test_data(dataSize);
  for (size_t i = 0; i < dataSize; ++i) {
    test_data[i] = static_cast<char>(i % 256);
  }

  auto arrow_out_ret = local_fs_->OpenOutputStream(temp_filepath_);
  if (!arrow_out_ret.ok()) {
    throw std::runtime_error("Failed to open output stream: " +
                             arrow_out_ret.status().message());
  }
  auto arrow_out = arrow_out_ret.ValueUnsafe();
  // Write the test data
  auto status = arrow_out->Write(test_data.data(), test_data.size());
  ASSERT_TRUE(status.ok());
  status = arrow_out->Flush();
  ASSERT_TRUE(status.ok());
  status = arrow_out->Close();
  ASSERT_TRUE(status.ok());

  // Create an AvroInputStream with a smaller buffer
  const int64_t smallBufferSize = buffer_size_ / 2;
  auto avro_input_stream = CreateInputStream(temp_filepath_, smallBufferSize);

  // Read all the data in chunks
  std::vector<char> readData;
  const uint8_t* data{};
  size_t len{};
  while (avro_input_stream->next(&data, &len)) {
    readData.insert(readData.end(), data, data + len);
  }

  // Verify all data was read correctly
  EXPECT_EQ(readData.size(), test_data.size());
  EXPECT_EQ(std::memcmp(readData.data(), test_data.data(), test_data.size()), 0);
  EXPECT_EQ(avro_input_stream->byteCount(), test_data.size());
}

TEST_F(AVROStreamTest, OutputStreamLargeData) {
  // Create an AvroOutputStream with a small buffer
  const int64_t smallBufferSize = 256;
  auto avro_output_stream = CreateOutputStream(temp_filepath_, smallBufferSize);

  // Create a large test data set (larger than the buffer size)
  const size_t dataSize = smallBufferSize * 3.5;
  std::vector<char> test_data(dataSize);
  for (size_t i = 0; i < dataSize; ++i) {
    test_data[i] = static_cast<char>(i % 256);
  }

  // Write the data in chunks
  size_t bytesWritten = 0;
  while (bytesWritten < dataSize) {
    uint8_t* buffer{};
    size_t bufferSize{};
    ASSERT_TRUE(avro_output_stream->next(&buffer, &bufferSize));

    size_t bytesToWrite = std::min(bufferSize, dataSize - bytesWritten);
    std::memcpy(buffer, test_data.data() + bytesWritten, bytesToWrite);

    if (bytesToWrite < bufferSize) {
      avro_output_stream->backup(bufferSize - bytesToWrite);
    }

    bytesWritten += bytesToWrite;
  }

  // Flush and close
  avro_output_stream->flush();

  // Verify the written data
  auto avro_input_stream = CreateInputStream(temp_filepath_, buffer_size_);
  std::vector<char> readData;
  const uint8_t* data{};
  size_t len{};
  while (avro_input_stream->next(&data, &len)) {
    readData.insert(readData.end(), data, data + len);
  }
  // Verify all data was read correctly
  EXPECT_EQ(readData.size(), test_data.size());
  EXPECT_EQ(std::memcmp(readData.data(), test_data.data(), test_data.size()), 0);
  EXPECT_EQ(avro_input_stream->byteCount(), test_data.size());
}

}  // namespace iceberg::avro
