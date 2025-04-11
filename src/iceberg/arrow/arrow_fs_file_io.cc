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

#include "iceberg/arrow/arrow_fs_file_io.h"

#include <arrow/filesystem/localfs.h>

#include "iceberg/arrow/arrow_error_transform_internal.h"

namespace iceberg::arrow {

/// \brief Read the content of the file at the given location.
Result<std::string> ArrowFileSystemFileIO::ReadFile(const std::string& file_location,
                                                    std::optional<size_t> length) {
  ::arrow::fs::FileInfo file_info(file_location);
  if (length.has_value()) {
    file_info.set_size(length.value());
  }
  std::string content;
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file, arrow_fs_->OpenInputFile(file_info));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file_size, file->GetSize());

  content.resize(file_size);
  size_t remain = file_size;
  size_t offset = 0;
  while (remain > 0) {
    size_t read_length = std::min(remain, static_cast<size_t>(1024 * 1024));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(
        auto read_bytes,
        file->Read(read_length, reinterpret_cast<uint8_t*>(&content[offset])));
    remain -= read_bytes;
    offset += read_bytes;
  }

  return content;
}

/// \brief Write the given content to the file at the given location.
Status ArrowFileSystemFileIO::WriteFile(const std::string& file_location,
                                        std::string_view content) {
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file, arrow_fs_->OpenOutputStream(file_location));
  ICEBERG_ARROW_RETURN_NOT_OK(file->Write(content.data(), content.size()));
  ICEBERG_ARROW_RETURN_NOT_OK(file->Flush());
  ICEBERG_ARROW_RETURN_NOT_OK(file->Close());
  return {};
}

/// \brief Delete a file at the given location.
Status ArrowFileSystemFileIO::DeleteFile(const std::string& file_location) {
  ICEBERG_ARROW_RETURN_NOT_OK(arrow_fs_->DeleteFile(file_location));
  return {};
}

}  // namespace iceberg::arrow
