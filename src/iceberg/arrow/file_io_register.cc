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

void RegisterFileIO() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    // Register Arrow local filesystem FileIO
    FileIORegistry::Register(
        FileIORegistry::kArrowLocalFileIO,
        [](const std::string& /*warehouse*/,
           const std::unordered_map<std::string, std::string>& /*properties*/)
            -> Result<std::shared_ptr<FileIO>> {
          return std::shared_ptr<FileIO>(MakeLocalFileIO());
        });

    // Register Arrow S3 FileIO
    FileIORegistry::Register(
        FileIORegistry::kArrowS3FileIO,
        [](const std::string& warehouse,
           const std::unordered_map<std::string, std::string>& properties)
            -> Result<std::shared_ptr<FileIO>> {
          ICEBERG_ASSIGN_OR_RAISE(auto file_io, MakeS3FileIO(warehouse, properties));
          return std::shared_ptr<FileIO>(std::move(file_io));
        });
  });
}

}  // namespace iceberg::arrow
