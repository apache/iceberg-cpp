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

#include "iceberg/arrow/io/local_file_io.h"

#include <filesystem>

#include <gtest/gtest.h>

class LocalFileIOTest : public testing::Test {
 protected:
  void SetUp() override {
    local_fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
    file_io_ = std::make_shared<iceberg::arrow::io::LocalFileIO>(local_fs_);
  }

  std::shared_ptr<arrow::fs::LocalFileSystem> local_fs_;
  std::shared_ptr<iceberg::FileIO> file_io_;
  std::filesystem::path tmpfile = std::filesystem::temp_directory_path() / "123.txt";
};

TEST_F(LocalFileIOTest, readWriteFile) {
  auto read_res = file_io_->ReadFile(tmpfile.string(), 1024);
  EXPECT_EQ(read_res.error().kind, iceberg::ErrorKind::kInvalidArgument);

  read_res = file_io_->ReadFile(tmpfile.string(), std::nullopt);
  EXPECT_EQ(read_res.error().kind, iceberg::ErrorKind::kIOError);

  auto write_res = file_io_->WriteFile(tmpfile.string(), "hello world", false);
  EXPECT_TRUE(write_res.has_value());

  read_res = file_io_->ReadFile(tmpfile.string(), std::nullopt);
  EXPECT_EQ(read_res.value(), "hello world");
}

TEST_F(LocalFileIOTest, deleteFile) {
  auto del_res = file_io_->DeleteFile(tmpfile.string());
  EXPECT_TRUE(del_res.has_value());

  del_res = file_io_->DeleteFile(tmpfile.string());
  EXPECT_EQ(del_res.error().kind, iceberg::ErrorKind::kNoSuchFile);
}
