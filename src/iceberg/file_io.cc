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

#include <limits>
#include <utility>

#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

Status FinishWithCloseStatus(Status operation_status, Status close_status) {
  if (!operation_status.has_value()) {
    auto error = operation_status.error();
    if (!close_status.has_value()) {
      error.message += "; additionally failed to close stream: ";
      error.message += close_status.error().message;
    }
    return std::unexpected<Error>(std::move(error));
  }
  return close_status;
}

}  // namespace

Result<std::unique_ptr<InputFile>> FileIO::NewInputFile(std::string file_location) {
  return NotImplemented("NewInputFile not implemented for {}", file_location);
}

Result<std::unique_ptr<InputFile>> FileIO::NewInputFile(std::string file_location,
                                                        size_t /*length*/) {
  return NewInputFile(std::move(file_location));
}

Result<std::unique_ptr<OutputFile>> FileIO::NewOutputFile(std::string file_location) {
  return NotImplemented("NewOutputFile not implemented for {}", file_location);
}

Result<std::string> FileIO::ReadFile(const std::string& file_location,
                                     std::optional<size_t> length) {
  int64_t read_size;
  std::unique_ptr<InputFile> input_file;
  if (length.has_value()) {
    if (*length > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
      return InvalidArgument("Requested read length {} exceeds int64_t max", *length);
    }
    ICEBERG_ASSIGN_OR_RAISE(input_file, NewInputFile(file_location, *length));
    read_size = static_cast<int64_t>(*length);
  } else {
    ICEBERG_ASSIGN_OR_RAISE(input_file, NewInputFile(file_location));
    ICEBERG_ASSIGN_OR_RAISE(read_size, input_file->Size());
  }
  if (read_size < 0) {
    return Invalid("Invalid negative file size {} for {}", read_size, file_location);
  }

  auto size = static_cast<size_t>(read_size);
  std::string content(size, '\0');
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file->Open());
  Status read_status = {};
  if (size > 0) {
    auto bytes = std::as_writable_bytes(std::span(content.data(), content.size()));
    read_status = stream->ReadFully(/*position=*/0, bytes);
  }
  ICEBERG_RETURN_UNEXPECTED(
      FinishWithCloseStatus(std::move(read_status), stream->Close()));
  return content;
}

Status FileIO::WriteFile(const std::string& file_location, std::string_view content) {
  ICEBERG_ASSIGN_OR_RAISE(auto output_file, NewOutputFile(file_location));
  ICEBERG_ASSIGN_OR_RAISE(auto stream, output_file->CreateOrOverwrite());
  Status status = {};
  if (!content.empty()) {
    auto bytes = std::as_bytes(std::span(content.data(), content.size()));
    status = stream->Write(bytes);
  }
  if (status.has_value()) {
    status = stream->Flush();
  }
  return FinishWithCloseStatus(std::move(status), stream->Close());
}

}  // namespace iceberg
