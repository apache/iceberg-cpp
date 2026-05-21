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

#include "iceberg/file_io.h"

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg {
namespace {

class RecordingFileIO : public FileIO {
 public:
  explicit RecordingFileIO(std::string failure_path = "")
      : failure_path_(std::move(failure_path)) {}

  Status DeleteFile(const std::string& file_location) override {
    deleted_paths.push_back(file_location);
    if (file_location == failure_path_) {
      return IOError("failed to delete {}", file_location);
    }
    return {};
  }

  std::vector<std::string> deleted_paths;

 private:
  std::string failure_path_;
};

}  // namespace

TEST(FileIOTest, DeleteFilesFallsBackToDeleteFileForEachPath) {
  RecordingFileIO file_io;
  std::vector<std::string> paths = {"file-a.avro", "file-b.avro"};

  EXPECT_THAT(file_io.DeleteFiles(paths), IsOk());
  EXPECT_THAT(file_io.deleted_paths,
              ::testing::ElementsAre("file-a.avro", "file-b.avro"));
}

TEST(FileIOTest, DeleteFilesReturnsFirstDeleteFileError) {
  RecordingFileIO file_io("file-b.avro");
  std::vector<std::string> paths = {"file-a.avro", "file-b.avro", "file-c.avro"};

  auto status = file_io.DeleteFiles(paths);

  EXPECT_THAT(status, IsError(ErrorKind::kIOError));
  EXPECT_THAT(status, HasErrorMessage("failed to delete file-b.avro"));
  EXPECT_THAT(file_io.deleted_paths,
              ::testing::ElementsAre("file-a.avro", "file-b.avro"));
}

}  // namespace iceberg
