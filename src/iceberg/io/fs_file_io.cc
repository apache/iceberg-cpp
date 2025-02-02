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

#include "fs_file_io.h"

#include <fcntl.h>

#include <cassert>
#include <filesystem>

namespace iceberg {
namespace io {

bool FsInputFile::exists() const { return std::filesystem::exists(path()); }

void FsOutputFile::create() {
  int fd = open(path().c_str(), O_CREAT | O_WRONLY, 0666);
  assert(fd != -1);
  // TODO: how to handle this scenario
}

std::shared_ptr<InputFile> FsFileIO::newInputFile(const std::string& path) {
  return std::make_shared<FsInputFile>(path);
}

std::shared_ptr<OutputFile> FsFileIO::newOutputFile(const std::string& path) {
  return std::make_shared<FsOutputFile>(path);
}

void FsFileIO::DeleteFile(const std::string& path) {
  std::error_code ec;
  bool ret = std::filesystem::remove(path, ec);
  assert(ret);
  // TODO: how to handle this scenario
}

}  // namespace io
}  // namespace iceberg
