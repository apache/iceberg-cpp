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

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>

#include "iceberg/metadata_cache.h"
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

Result<std::string> ReadInputFile(InputFile& input_file, int64_t read_size,
                                  std::string_view file_location) {
  if (read_size < 0) {
    return Invalid("Invalid negative file size {} for {}", read_size, file_location);
  }
  if (static_cast<uint64_t>(read_size) >
      static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return Invalid("File size {} exceeds size_t max for {}", read_size, file_location);
  }

  auto size = static_cast<size_t>(read_size);
  std::string content(size, '\0');
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file.Open());
  Status read_status = {};
  if (size > 0) {
    auto bytes = std::as_writable_bytes(std::span(content.data(), content.size()));
    read_status = stream->ReadFully(/*position=*/0, bytes);
  }
  ICEBERG_RETURN_UNEXPECTED(
      FinishWithCloseStatus(std::move(read_status), stream->Close()));
  return content;
}

class CachedSeekableInputStream : public SeekableInputStream {
 public:
  explicit CachedSeekableInputStream(std::shared_ptr<const std::string> content)
      : content_(std::move(content)) {}

  Result<int64_t> Position() const override { return position_; }

  Status Seek(int64_t position) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    ICEBERG_PRECHECK(position >= 0, "Position must not be negative: {}", position);
    ICEBERG_PRECHECK(static_cast<uint64_t>(position) <= content_->size(),
                     "Position {} exceeds file size {}", position, content_->size());
    position_ = position;
    return {};
  }

  Result<int64_t> Read(std::span<std::byte> out) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    auto position = static_cast<size_t>(position_);
    auto bytes_to_read = std::min(out.size(), content_->size() - position);
    if (bytes_to_read > 0) {
      std::memcpy(out.data(), content_->data() + position, bytes_to_read);
      position_ += static_cast<int64_t>(bytes_to_read);
    }
    return static_cast<int64_t>(bytes_to_read);
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    ICEBERG_PRECHECK(!closed_, "Input stream is closed");
    ICEBERG_PRECHECK(position >= 0, "Position must not be negative: {}", position);
    ICEBERG_PRECHECK(static_cast<uint64_t>(position) <= content_->size(),
                     "Position {} exceeds file size {}", position, content_->size());
    auto offset = static_cast<size_t>(position);
    ICEBERG_PRECHECK(out.size() <= content_->size() - offset,
                     "Read out of bounds: offset {} + length {} exceeds file size {}",
                     position, out.size(), content_->size());
    if (!out.empty()) {
      std::memcpy(out.data(), content_->data() + offset, out.size());
    }
    return {};
  }

  Status Close() override {
    closed_ = true;
    return {};
  }

 private:
  std::shared_ptr<const std::string> content_;
  int64_t position_ = 0;
  bool closed_ = false;
};

class CachedInputFile : public InputFile {
 public:
  CachedInputFile(std::unique_ptr<InputFile> input_file,
                  std::shared_ptr<MetadataCache> cache, int64_t size)
      : input_file_(std::move(input_file)),
        cache_(std::move(cache)),
        location_(input_file_->location()),
        size_(size) {}

  std::string_view location() const override { return location_; }

  Result<int64_t> Size() const override {
    if (auto content = cache_->GetIfPresent(location_)) {
      return static_cast<int64_t>(content->size());
    }
    return size_;
  }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    auto content =
        cache_->Get(location_, static_cast<size_t>(size_),
                    [this]() -> Result<MetadataCache::Content> {
                      ICEBERG_ASSIGN_OR_RAISE(
                          auto loaded, ReadInputFile(*input_file_, size_, location_));
                      return std::make_shared<const std::string>(std::move(loaded));
                    });
    if (!content.has_value()) {
      // Cache loading is an optimization. Match Java's ContentCache by falling back to
      // the underlying input when a read-ahead attempt fails.
      return input_file_->Open();
    }
    return std::make_unique<CachedSeekableInputStream>(std::move(content).value());
  }

 private:
  std::unique_ptr<InputFile> input_file_;
  std::shared_ptr<MetadataCache> cache_;
  std::string location_;
  int64_t size_;
};

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
  return ReadInputFile(*input_file, read_size, file_location);
}

Result<std::unique_ptr<InputFile>> FileIO::NewCachedInputFile(
    std::string file_location, std::optional<size_t> length) {
  std::unique_ptr<InputFile> input_file;
  if (length.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(input_file, NewInputFile(file_location, *length));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(input_file, NewInputFile(file_location));
  }

  auto cache = GetMetadataCache();
  if (cache == nullptr || !cache->options().enabled) {
    return input_file;
  }

  if (auto cached = cache->GetIfPresent(file_location)) {
    return std::make_unique<CachedInputFile>(std::move(input_file), std::move(cache),
                                             static_cast<int64_t>(cached->size()));
  }

  int64_t size;
  if (length.has_value()) {
    if (*length > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
      return InvalidArgument("File length {} exceeds int64_t max", *length);
    }
    size = static_cast<int64_t>(*length);
  } else {
    ICEBERG_ASSIGN_OR_RAISE(size, input_file->Size());
  }
  if (size < 0) {
    return Invalid("Invalid negative file size {} for {}", size, file_location);
  }
  if (static_cast<size_t>(size) > cache->options().max_content_length) {
    return input_file;
  }
  return std::make_unique<CachedInputFile>(std::move(input_file), std::move(cache), size);
}

Result<std::string> FileIO::ReadFileCached(const std::string& file_location,
                                           std::optional<size_t> length) {
  auto cache = GetMetadataCache();
  if (cache == nullptr || !cache->options().enabled) {
    return ReadFile(file_location, length);
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto content,
      cache->Get(file_location, length,
                 [this, &file_location, length]() -> Result<MetadataCache::Content> {
                   ICEBERG_ASSIGN_OR_RAISE(auto loaded, ReadFile(file_location, length));
                   return std::make_shared<const std::string>(std::move(loaded));
                 }));
  return *content;
}

Status FileIO::ConfigureMetadataCache(
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_ASSIGN_OR_RAISE(auto options, MetadataCacheOptions::FromProperties(properties));
  ICEBERG_ASSIGN_OR_RAISE(auto cache, MetadataCache::Make(options));
  std::lock_guard lock(metadata_cache_mutex_);
  if (metadata_cache_ == nullptr) {
    metadata_cache_ = std::move(cache);
    return {};
  }
  if (metadata_cache_->options() == options) {
    return {};
  }
  return InvalidArgument("Metadata cache is already configured with different options");
}

bool FileIO::MetadataCacheEnabled() const {
  auto cache = GetMetadataCache();
  return cache != nullptr && cache->options().enabled;
}

void FileIO::InvalidateMetadataCache(std::string_view file_location) {
  auto cache = GetMetadataCache();
  if (cache != nullptr) {
    cache->Invalidate(file_location);
  }
}

void FileIO::ClearMetadataCache() {
  auto cache = GetMetadataCache();
  if (cache != nullptr) {
    cache->Clear();
  }
}

std::shared_ptr<MetadataCache> FileIO::GetMetadataCache() const {
  std::lock_guard lock(metadata_cache_mutex_);
  return metadata_cache_;
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

Status FileIO::DeleteFiles(const std::vector<std::string>& file_locations) {
  for (const auto& file_location : file_locations) {
    ICEBERG_RETURN_UNEXPECTED(DeleteFile(file_location));
  }
  return {};
}

}  // namespace iceberg
