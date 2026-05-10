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

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <optional>

#include <arrow/buffer.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/mockfs.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::arrow {

namespace {

Result<int64_t> ToInt64Length(size_t length) {
  if (length > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
    return InvalidArgument("File length {} exceeds int64_t max", length);
  }
  return static_cast<int64_t>(length);
}

::arrow::Status ToArrowStatus(const Error& error) {
  switch (error.kind) {
    case ErrorKind::kInvalid:
    case ErrorKind::kInvalidArgument:
      return ::arrow::Status::Invalid(error.message);
    case ErrorKind::kNotImplemented:
    case ErrorKind::kNotSupported:
      return ::arrow::Status::NotImplemented(error.message);
    default:
      return ::arrow::Status::IOError(error.message);
  }
}

::arrow::Result<int64_t> BytesToReadAt(int64_t position, int64_t nbytes, int64_t size) {
  if (position < 0 || nbytes < 0) {
    return ::arrow::Status::Invalid("ReadAt position and length must be non-negative");
  }
  if (position > size) {
    return ::arrow::Status::IOError("Read out of bounds (offset = ", position,
                                    ", size = ", nbytes, ") in file of size ", size);
  }
  return std::min(nbytes, size - position);
}

/// Adapts the generic Iceberg input stream API to Arrow's RandomAccessFile API.
///
/// Avro and Parquet readers in the bundle layer consume Arrow IO streams. This
/// fallback keeps those readers usable with non-Arrow FileIO implementations without
/// exposing Arrow filesystem details through the generic FileIO interface.
class InputStreamAdapter : public ::arrow::io::RandomAccessFile {
 public:
  InputStreamAdapter(std::unique_ptr<SeekableInputStream> input, int64_t size)
      : input_(std::move(input)), size_(size) {
    RandomAccessFile::set_mode(::arrow::io::FileMode::READ);
  }

  ::arrow::Status Close() override {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return ::arrow::Status::OK();
    }
    auto status = input_->Close();
    if (!status.has_value()) {
      return ToArrowStatus(status.error());
    }
    closed_ = true;
    return ::arrow::Status::OK();
  }

  ::arrow::Result<int64_t> Tell() const override {
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(CheckOpenLocked());
    auto position = input_->Position();
    if (!position.has_value()) {
      return ToArrowStatus(position.error());
    }
    if (position.value() < 0) {
      return ::arrow::Status::IOError("FileIO input stream returned negative position");
    }
    return position.value();
  }

  bool closed() const override {
    std::lock_guard lock(mutex_);
    return closed_;
  }

  ::arrow::Status Seek(int64_t position) override {
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(CheckOpenLocked());
    auto status = input_->Seek(position);
    if (!status.has_value()) {
      return ToArrowStatus(status.error());
    }
    return ::arrow::Status::OK();
  }

  ::arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    if (nbytes < 0) {
      return ::arrow::Status::Invalid("Cannot read a negative number of bytes");
    }
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(CheckOpenLocked());
    if (nbytes == 0) {
      return 0;
    }
    auto data = reinterpret_cast<std::byte*>(out);
    auto result = input_->Read(std::span(data, static_cast<size_t>(nbytes)));
    if (!result.has_value()) {
      return ToArrowStatus(result.error());
    }
    if (result.value() < 0 || result.value() > nbytes) {
      return ::arrow::Status::IOError("FileIO input stream returned invalid byte count");
    }
    return result.value();
  }

  ::arrow::Result<std::shared_ptr<::arrow::Buffer>> Read(int64_t nbytes) override {
    if (nbytes < 0) {
      return ::arrow::Status::Invalid("Cannot read a negative number of bytes");
    }
    ARROW_ASSIGN_OR_RAISE(auto buffer, ::arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(auto bytes_read, Read(nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, /*shrink_to_fit=*/false));
    return std::shared_ptr<::arrow::Buffer>(std::move(buffer));
  }

  ::arrow::Result<int64_t> GetSize() override { return size_; }

  ::arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(CheckOpenLocked());
    ARROW_ASSIGN_OR_RAISE(auto bytes_to_read, BytesToReadAt(position, nbytes, size_));
    if (bytes_to_read == 0) {
      return 0;
    }
    auto data = reinterpret_cast<std::byte*>(out);
    auto status =
        input_->ReadFully(position, std::span(data, static_cast<size_t>(bytes_to_read)));
    if (!status.has_value()) {
      return ToArrowStatus(status.error());
    }
    return bytes_to_read;
  }

  ::arrow::Result<std::shared_ptr<::arrow::Buffer>> ReadAt(int64_t position,
                                                           int64_t nbytes) override {
    {
      std::lock_guard lock(mutex_);
      ARROW_RETURN_NOT_OK(CheckOpenLocked());
    }
    ARROW_ASSIGN_OR_RAISE(auto bytes_to_read, BytesToReadAt(position, nbytes, size_));
    ARROW_ASSIGN_OR_RAISE(auto buffer, ::arrow::AllocateResizableBuffer(bytes_to_read));
    if (bytes_to_read == 0) {
      return std::shared_ptr<::arrow::Buffer>(std::move(buffer));
    }
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(CheckOpenLocked());
    auto status = input_->ReadFully(
        position, std::span(reinterpret_cast<std::byte*>(buffer->mutable_data()),
                            static_cast<size_t>(bytes_to_read)));
    if (!status.has_value()) {
      return ToArrowStatus(status.error());
    }
    return std::shared_ptr<::arrow::Buffer>(std::move(buffer));
  }

 private:
  ::arrow::Status CheckOpenLocked() const {
    if (closed_) {
      return ::arrow::Status::IOError("Operation on closed FileIO input stream");
    }
    return ::arrow::Status::OK();
  }

  std::unique_ptr<SeekableInputStream> input_;
  int64_t size_;
  bool closed_ = false;
  mutable std::mutex mutex_;
};

/// Adapts the generic Iceberg output stream API to Arrow's OutputStream API.
///
/// Avro and Parquet writers in the bundle layer consume Arrow IO streams. This
/// fallback keeps those writers usable with non-Arrow FileIO implementations without
/// requiring them to downcast to ArrowFileSystemFileIO.
class OutputStreamAdapter : public ::arrow::io::OutputStream {
 public:
  explicit OutputStreamAdapter(std::unique_ptr<PositionOutputStream> output)
      : output_(std::move(output)) {
    OutputStream::set_mode(::arrow::io::FileMode::WRITE);
  }

  ::arrow::Status Close() override {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return ::arrow::Status::OK();
    }
    auto status = output_->Close();
    if (!status.has_value()) {
      return ToArrowStatus(status.error());
    }
    closed_ = true;
    return ::arrow::Status::OK();
  }

  ::arrow::Result<int64_t> Tell() const override {
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(CheckOpenLocked());
    auto position = output_->Position();
    if (!position.has_value()) {
      return ToArrowStatus(position.error());
    }
    if (position.value() < 0) {
      return ::arrow::Status::IOError("FileIO output stream returned negative position");
    }
    return position.value();
  }

  bool closed() const override {
    std::lock_guard lock(mutex_);
    return closed_;
  }

  ::arrow::Status Write(const void* data, int64_t nbytes) override {
    if (nbytes < 0) {
      return ::arrow::Status::Invalid("Cannot write a negative number of bytes");
    }
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(CheckOpenLocked());
    if (nbytes == 0) {
      return ::arrow::Status::OK();
    }
    auto status = output_->Write(
        std::span(reinterpret_cast<const std::byte*>(data), static_cast<size_t>(nbytes)));
    if (!status.has_value()) {
      return ToArrowStatus(status.error());
    }
    return ::arrow::Status::OK();
  }

  ::arrow::Status Flush() override {
    std::lock_guard lock(mutex_);
    ARROW_RETURN_NOT_OK(CheckOpenLocked());
    auto status = output_->Flush();
    if (!status.has_value()) {
      return ToArrowStatus(status.error());
    }
    return ::arrow::Status::OK();
  }

 private:
  ::arrow::Status CheckOpenLocked() const {
    if (closed_) {
      return ::arrow::Status::IOError("Operation on closed FileIO output stream");
    }
    return ::arrow::Status::OK();
  }

  std::unique_ptr<PositionOutputStream> output_;
  bool closed_ = false;
  mutable std::mutex mutex_;
};

class ArrowSeekableInputStream : public SeekableInputStream {
 public:
  explicit ArrowSeekableInputStream(std::shared_ptr<::arrow::io::RandomAccessFile> input)
      : input_(std::move(input)) {}

  Result<int64_t> Position() const override {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto position, input_->Tell());
    return position;
  }

  Status Seek(int64_t position) override {
    ICEBERG_ARROW_RETURN_NOT_OK(input_->Seek(position));
    return {};
  }

  Result<int64_t> Read(std::span<std::byte> out) override {
    ICEBERG_ASSIGN_OR_RAISE(auto size, ToInt64Length(out.size()));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto bytes_read, input_->Read(size, out.data()));
    if (bytes_read < 0 || bytes_read > size) {
      return IOError("Arrow input stream returned invalid byte count");
    }
    return bytes_read;
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    if (position < 0) {
      return InvalidArgument("Cannot read from negative position {}", position);
    }
    ICEBERG_ASSIGN_OR_RAISE(auto size, ToInt64Length(out.size()));
    if (size == 0) {
      return {};
    }
    if (position > std::numeric_limits<int64_t>::max() - size) {
      return InvalidArgument(
          "Read range starting at {} with length {} exceeds int64_t max", position, size);
    }

    Status read_status = {};
    int64_t bytes_read = 0;
    while (bytes_read < size) {
      auto* data = out.data() + bytes_read;
      auto remaining = size - bytes_read;
      auto read_result = input_->ReadAt(position + bytes_read, remaining, data);
      if (!read_result.ok()) {
        read_status =
            std::unexpected<Error>{{.kind = ToErrorKind(read_result.status()),
                                    .message = read_result.status().ToString()}};
        break;
      }
      auto read = read_result.ValueOrDie();
      if (read < 0 || read > remaining) {
        read_status = IOError("Arrow input stream returned invalid byte count");
        break;
      }
      if (read == 0) {
        read_status =
            IOError("Unexpected EOF reading at offset {}", position + bytes_read);
        break;
      }
      bytes_read += read;
    }
    return read_status;
  }

  Status Close() override {
    if (input_->closed()) {
      return {};
    }
    ICEBERG_ARROW_RETURN_NOT_OK(input_->Close());
    return {};
  }

 private:
  std::shared_ptr<::arrow::io::RandomAccessFile> input_;
};

class ArrowPositionOutputStream : public PositionOutputStream {
 public:
  explicit ArrowPositionOutputStream(std::shared_ptr<::arrow::io::OutputStream> output)
      : output_(std::move(output)) {}

  Result<int64_t> Position() const override {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto position, output_->Tell());
    return position;
  }

  Status Write(std::span<const std::byte> data) override {
    ICEBERG_ASSIGN_OR_RAISE(auto size, ToInt64Length(data.size()));
    ICEBERG_ARROW_RETURN_NOT_OK(output_->Write(data.data(), size));
    return {};
  }

  Status Flush() override {
    ICEBERG_ARROW_RETURN_NOT_OK(output_->Flush());
    return {};
  }

  Status Close() override {
    if (output_->closed()) {
      return {};
    }
    ICEBERG_ARROW_RETURN_NOT_OK(output_->Close());
    return {};
  }

 private:
  std::shared_ptr<::arrow::io::OutputStream> output_;
};

class ArrowInputFile : public InputFile {
 public:
  ArrowInputFile(std::shared_ptr<::arrow::fs::FileSystem> fs, std::string location,
                 std::string path, std::optional<int64_t> file_size)
      : fs_(std::move(fs)),
        location_(std::move(location)),
        path_(std::move(path)),
        file_size_(file_size) {}

  std::string_view location() const override { return location_; }

  Result<int64_t> Size() const override {
    if (file_size_.has_value()) {
      return *file_size_;
    }
    ::arrow::fs::FileInfo file_info(path_, ::arrow::fs::FileType::File);
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto input, fs_->OpenInputFile(file_info));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto size, input->GetSize());
    return size;
  }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    ::arrow::fs::FileInfo file_info(path_, ::arrow::fs::FileType::File);
    if (file_size_.has_value()) {
      file_info.set_size(*file_size_);
    }
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto input, fs_->OpenInputFile(file_info));
    return std::make_unique<ArrowSeekableInputStream>(std::move(input));
  }

 private:
  std::shared_ptr<::arrow::fs::FileSystem> fs_;
  std::string location_;
  std::string path_;
  std::optional<int64_t> file_size_;
};

class ArrowOutputFile : public OutputFile {
 public:
  ArrowOutputFile(std::shared_ptr<::arrow::fs::FileSystem> fs, std::string location,
                  std::string path)
      : fs_(std::move(fs)), location_(std::move(location)), path_(std::move(path)) {}

  std::string_view location() const override { return location_; }

  Result<std::unique_ptr<PositionOutputStream>> Create() override {
    return Create(/*overwrite=*/false);
  }

  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
    return Create(/*overwrite=*/true);
  }

 private:
  Result<std::unique_ptr<PositionOutputStream>> Create(bool overwrite) {
    if (!overwrite) {
      ICEBERG_ARROW_ASSIGN_OR_RETURN(auto info, fs_->GetFileInfo(path_));
      if (info.type() != ::arrow::fs::FileType::NotFound) {
        return AlreadyExists("File already exists: {}", location_);
      }
    }
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto output, fs_->OpenOutputStream(path_));
    return std::make_unique<ArrowPositionOutputStream>(std::move(output));
  }

  std::shared_ptr<::arrow::fs::FileSystem> fs_;
  std::string location_;
  std::string path_;
};

}  // namespace

Result<std::string> ArrowFileSystemFileIO::ResolvePath(const std::string& file_location) {
  if (file_location.find("://") != std::string::npos) {
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto path, arrow_fs_->PathFromUri(file_location));
    return path;
  }
  return file_location;
}

Result<std::shared_ptr<::arrow::io::RandomAccessFile>> OpenArrowInputStream(
    const std::shared_ptr<FileIO>& io, const std::string& path,
    std::optional<size_t> length) {
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");

  if (auto arrow_io = std::dynamic_pointer_cast<ArrowFileSystemFileIO>(io)) {
    ICEBERG_ASSIGN_OR_RAISE(auto resolved_path, arrow_io->ResolvePath(path));
    ::arrow::fs::FileInfo file_info(resolved_path, ::arrow::fs::FileType::File);
    if (length.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(auto size, ToInt64Length(*length));
      file_info.set_size(size);
    }
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto input,
                                   arrow_io->arrow_fs_->OpenInputFile(file_info));
    return input;
  }

  int64_t size;
  std::unique_ptr<InputFile> input_file;
  if (length.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(input_file, io->NewInputFile(path, *length));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(input_file, io->NewInputFile(path));
  }
  ICEBERG_ASSIGN_OR_RAISE(size, input_file->Size());
  if (size < 0) {
    return Invalid("Invalid negative file size {} for {}", size, path);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto input, input_file->Open());
  return std::make_shared<InputStreamAdapter>(std::move(input), size);
}

Result<std::shared_ptr<::arrow::io::OutputStream>> OpenArrowOutputStream(
    const std::shared_ptr<FileIO>& io, const std::string& path, bool overwrite) {
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");

  if (auto arrow_io = std::dynamic_pointer_cast<ArrowFileSystemFileIO>(io)) {
    ICEBERG_ASSIGN_OR_RAISE(auto resolved_path, arrow_io->ResolvePath(path));
    if (!overwrite) {
      ICEBERG_ARROW_ASSIGN_OR_RETURN(auto info,
                                     arrow_io->arrow_fs_->GetFileInfo(resolved_path));
      if (info.type() != ::arrow::fs::FileType::NotFound) {
        return AlreadyExists("File already exists: {}", path);
      }
    }
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto output,
                                   arrow_io->arrow_fs_->OpenOutputStream(resolved_path));
    return output;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto output_file, io->NewOutputFile(path));
  std::unique_ptr<PositionOutputStream> output;
  if (overwrite) {
    ICEBERG_ASSIGN_OR_RAISE(output, output_file->CreateOrOverwrite());
  } else {
    ICEBERG_ASSIGN_OR_RAISE(output, output_file->Create());
  }
  return std::make_shared<OutputStreamAdapter>(std::move(output));
}

Result<std::unique_ptr<InputFile>> ArrowFileSystemFileIO::NewInputFile(
    std::string file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto path, ResolvePath(file_location));
  return std::make_unique<ArrowInputFile>(arrow_fs_, std::move(file_location),
                                          std::move(path), std::nullopt);
}

Result<std::unique_ptr<InputFile>> ArrowFileSystemFileIO::NewInputFile(
    std::string file_location, size_t length) {
  ICEBERG_ASSIGN_OR_RAISE(auto size, ToInt64Length(length));
  ICEBERG_ASSIGN_OR_RAISE(auto path, ResolvePath(file_location));
  return std::make_unique<ArrowInputFile>(arrow_fs_, std::move(file_location),
                                          std::move(path), size);
}

Result<std::unique_ptr<OutputFile>> ArrowFileSystemFileIO::NewOutputFile(
    std::string file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto path, ResolvePath(file_location));
  return std::make_unique<ArrowOutputFile>(arrow_fs_, std::move(file_location),
                                           std::move(path));
}

/// \brief Delete a file at the given location.
Status ArrowFileSystemFileIO::DeleteFile(const std::string& file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto path, ResolvePath(file_location));
  ICEBERG_ARROW_RETURN_NOT_OK(arrow_fs_->DeleteFile(path));
  return {};
}

std::unique_ptr<FileIO> ArrowFileSystemFileIO::MakeMockFileIO() {
  return std::make_unique<ArrowFileSystemFileIO>(
      std::make_shared<::arrow::fs::internal::MockFileSystem>(
          std::chrono::system_clock::now()));
}

std::unique_ptr<FileIO> ArrowFileSystemFileIO::MakeLocalFileIO() {
  return std::make_unique<ArrowFileSystemFileIO>(
      std::make_shared<::arrow::fs::LocalFileSystem>());
}

std::unique_ptr<FileIO> MakeMockFileIO() {
  return ArrowFileSystemFileIO::MakeMockFileIO();
}

std::unique_ptr<FileIO> MakeLocalFileIO() {
  return ArrowFileSystemFileIO::MakeLocalFileIO();
}

}  // namespace iceberg::arrow
