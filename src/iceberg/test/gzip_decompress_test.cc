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
#include <arrow/io/compressed.h>
#include <arrow/io/file.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/file_io.h"
#include "iceberg/util/gzip_internal.h"
#include "matchers.h"
#include "temp_file_test_base.h"

namespace iceberg {

class GZipTest : public TempFileTestBase {
 protected:
  void SetUp() override {
    TempFileTestBase::SetUp();
    io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    temp_filepath_ = CreateNewTempFilePathWithSuffix("test.gz");
  }

  std::shared_ptr<iceberg::FileIO> io_;
  std::string temp_filepath_;
};

TEST_F(GZipTest, GZipDecompressedString) {
  auto test_string = GenerateRandomString(1024);

#define ARROW_ASSIGN_OR_THROW(var, expr)   \
  {                                        \
    auto result = expr;                    \
    ASSERT_TRUE(result.ok());              \
    var = std::move(result.ValueUnsafe()); \
  }
  std::shared_ptr<::arrow::io::FileOutputStream> out_stream;
  ARROW_ASSIGN_OR_THROW(out_stream, ::arrow::io::FileOutputStream::Open(temp_filepath_));

  std::shared_ptr<::arrow::util::Codec> gzip_codec;
  ARROW_ASSIGN_OR_THROW(gzip_codec,
                        ::arrow::util::Codec::Create(::arrow::Compression::GZIP));

  std::shared_ptr<::arrow::io::OutputStream> compressed_stream;
  ARROW_ASSIGN_OR_THROW(compressed_stream, ::arrow::io::CompressedOutputStream::Make(
                                               gzip_codec.get(), out_stream));
#undef ARROW_ASSIGN_OR_THROW

  ASSERT_TRUE(compressed_stream->Write(test_string).ok());
  ASSERT_TRUE(compressed_stream->Flush().ok());
  ASSERT_TRUE(compressed_stream->Close().ok());

  auto result = io_->ReadFile(temp_filepath_, test_string.size());
  EXPECT_THAT(result, IsOk());

  auto gzip_decompressor = std::make_unique<GZipDecompressor>();
  EXPECT_THAT(gzip_decompressor->Init(), IsOk());
  EXPECT_THAT(result, IsOk());
  auto read_data = gzip_decompressor->Decompress(result.value());
  EXPECT_THAT(read_data, IsOk());
  ASSERT_EQ(read_data.value(), test_string);
}

}  // namespace iceberg
