// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "iceberg/arrow/file_io_register.h"

#include <mutex>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/util/macros.h"

namespace iceberg::arrow {

void RegisterLocalFileIO() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    FileIORegistry::Register(
        std::string(FileIORegistry::kArrowLocalFileIO),
        [](const std::unordered_map<std::string, std::string>& /*properties*/)
            -> Result<std::unique_ptr<FileIO>> { return MakeLocalFileIO(); });
  });
}

void RegisterS3FileIO() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    FileIORegistry::Register(
        std::string(FileIORegistry::kArrowS3FileIO),
        [](const std::unordered_map<std::string, std::string>& properties)
            -> Result<std::unique_ptr<FileIO>> { return MakeS3FileIO(properties); });
  });
}

}  // namespace iceberg::arrow
