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

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <gtest/gtest.h>
#include <iceberg/arrow/arrow_fs_file_io.h>
#include <iceberg/avro/avro_stream.h>
#include <iceberg/avro/demo_avro.h>
#include <iceberg/file_reader.h>

#include "matchers.h"
#include "temp_file_test_base.h"

namespace iceberg::avro {

class AVROTest : public TempFileTestBase {
 public:
  void SetUp() override {
    TempFileTestBase::SetUp();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    temp_filepath_ = CreateNewTempFilePath();
  }

  std::shared_ptr<iceberg::FileIO> file_io_;
  std::string temp_filepath_;
};

TEST_F(AVROTest, TestDemoAvro) {
  std::string expected =
      "{\n\
    \"type\": \"record\",\n\
    \"name\": \"testrecord\",\n\
    \"fields\": [\n\
        {\n\
            \"name\": \"testbytes\",\n\
            \"type\": \"bytes\",\n\
            \"default\": \"\"\n\
        }\n\
    ]\n\
}\n\
";

  auto avro = iceberg::avro::DemoAvro();
  EXPECT_EQ(avro.print(), expected);
}

TEST_F(AVROTest, TestAvroBasicStream) {
  auto fs = std::make_shared<::arrow::fs::LocalFileSystem>();
  std::cout << temp_filepath_ << std::endl;
  auto arrow_out_ret = fs->OpenOutputStream(temp_filepath_);
  ASSERT_TRUE(arrow_out_ret.ok());
  auto avro_output_stream =
      std::make_shared<AvroOutputStream>(std::move(arrow_out_ret.ValueUnsafe()), 1024);
  std::string test_data = "test data";
  {
    uint8_t* buf;
    size_t buf_size;
    ASSERT_TRUE(avro_output_stream->next(&buf, &buf_size));
    std::memcpy(buf, test_data.data(), test_data.size());
    avro_output_stream->backup(1024 - test_data.size());
    avro_output_stream->flush();
  }

  auto arrow_in_ret = fs->OpenInputFile(temp_filepath_);
  ASSERT_TRUE(arrow_in_ret.ok());
  auto avro_input_stream =
      std::make_shared<AvroInputStream>(std::move(arrow_in_ret.ValueUnsafe()), 1024);
  {
    const uint8_t* data{};
    size_t len{};
    ASSERT_TRUE(avro_input_stream->next(&data, &len));
    EXPECT_EQ(len, test_data.size());

    EXPECT_EQ(avro_input_stream->byteCount(), test_data.size());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data), len), test_data);
    std::cout << std::string(reinterpret_cast<const char*>(data), len) << std::endl;
    ASSERT_FALSE(avro_input_stream->next(&data, &len));
  }
}
TEST_F(AVROTest, TestDemoAvroReader) {
  auto result = ReaderFactoryRegistry::Create(FileFormatType::kAvro, {});
  ASSERT_THAT(result, IsOk());

  auto reader = std::move(result.value());
  ASSERT_EQ(reader->data_layout(), Reader::DataLayout::kStructLike);

  auto data = reader->Next();
  ASSERT_THAT(data, IsOk());
  ASSERT_TRUE(std::holds_alternative<std::monostate>(data.value()));
}

}  // namespace iceberg::avro
