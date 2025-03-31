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

#include "iceberg/arrow/io/arrow_fs_file_io.h"

#include <filesystem>

#include <arrow/filesystem/localfs.h>

#include "iceberg/arrow/arrow_error_transform_internal.h"

namespace iceberg::arrow::io {

/// \brief Read the content of the file at the given location.
expected<std::string, Error> ArrowFileSystemFileIO::ReadFile(
    const std::string& file_location, std::optional<size_t> length) {
  // We don't support reading a file with a specific length.
  if (length.has_value()) {
    return unexpected(Error(ErrorKind::kInvalidArgument, "Length is not supported"));
  }
  std::string content;
  ICEBERG_INTERNAL_ASSIGN_OR_RETURN(auto file, arrow_fs_->OpenInputFile(file_location));
  ICEBERG_INTERNAL_ASSIGN_OR_RETURN(auto file_size, file->GetSize());

  content.resize(file_size);
  ICEBERG_INTERNAL_ASSIGN_OR_RETURN(
      auto read_length,
      file->ReadAt(0, file_size, reinterpret_cast<uint8_t*>(&content[0])));

  return content;
}

/// \brief Write the given content to the file at the given location.
expected<void, Error> ArrowFileSystemFileIO::WriteFile(const std::string& file_location,
                                                       std::string_view content,
                                                       bool overwrite) {
  // auto file_info = arrow_fs_->GetFileInfo(file_location);
  // if (file_info.status().ok() && !overwrite) {
  //   return unexpected(
  //       Error(ErrorKind::kAlreadyExists, std::format("File {} exists",
  //       file_location)));
  // }
  ICEBERG_INTERNAL_ASSIGN_OR_RETURN(auto file,
                                    arrow_fs_->OpenOutputStream(file_location));
  ICEBERG_INTERNAL_RETURN_NOT_OK(file->Write(content.data(), content.size()));
  ICEBERG_INTERNAL_RETURN_NOT_OK(file->Flush());
  ICEBERG_INTERNAL_RETURN_NOT_OK(file->Close());
  return {};
}

/// \brief Delete a file at the given location.
expected<void, Error> ArrowFileSystemFileIO::DeleteFile(
    const std::string& file_location) {
  ICEBERG_INTERNAL_RETURN_NOT_OK(arrow_fs_->DeleteFile(file_location));
  return {};
}

}  // namespace iceberg::arrow::io
