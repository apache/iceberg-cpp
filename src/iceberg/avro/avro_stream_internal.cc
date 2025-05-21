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

#include "avro_stream_internal.h"

#include <format>

#include <arrow/result.h>

#include "iceberg/exception.h"

namespace iceberg::avro {

AvroInputStream::AvroInputStream(
    std::shared_ptr<arrow::io::RandomAccessFile> input_stream, int64_t buffer_size)
    : input_stream_(std::move(input_stream)),
      buffer_size_(buffer_size),
      buffer_(buffer_size) {}

AvroInputStream::~AvroInputStream() = default;

bool AvroInputStream::next(const uint8_t** data, size_t* len) {
  // Return all unconsumed data in the buffer
  if (buffer_pos_ < available_bytes_) {
    *data = buffer_.data() + buffer_pos_;
    *len = available_bytes_ - buffer_pos_;
    byte_count_ += available_bytes_ - buffer_pos_;
    buffer_pos_ = available_bytes_;
    return true;
  }

  // Read from the input stream when the buffer is empty
  auto result = input_stream_->Read(buffer_.size(), buffer_.data());
  // TODO(xiao.dong) Avro interface requires to return false if an error has occurred or
  // reach EOF, so error message can not be raised to the caller, add some log after we
  // have a logging system
  if (!result.ok() || result.ValueUnsafe() <= 0) {
    return false;
  }
  available_bytes_ = result.ValueUnsafe();
  buffer_pos_ = 0;

  // Return the whole buffer
  *data = buffer_.data();
  *len = available_bytes_;
  byte_count_ += available_bytes_;
  buffer_pos_ = available_bytes_;

  return true;
}

void AvroInputStream::backup(size_t len) {
  if (len > buffer_pos_) {
    throw IcebergError(
        std::format("Cannot backup {} bytes, only {} bytes available", len, buffer_pos_));
  }

  buffer_pos_ -= len;
  byte_count_ -= len;
}

void AvroInputStream::skip(size_t len) {
  // The range to skip is within the buffer
  if (buffer_pos_ + len <= available_bytes_) {
    buffer_pos_ += len;
    byte_count_ += len;
    return;
  }

  seek(byte_count_ + len);
}

size_t AvroInputStream::byteCount() const { return byte_count_; }

void AvroInputStream::seek(int64_t position) {
  auto status = input_stream_->Seek(position);
  if (!status.ok()) {
    throw IcebergError(
        std::format("Failed to seek to {}, got {}", position, status.ToString()));
  }

  buffer_pos_ = 0;
  available_bytes_ = 0;
  byte_count_ = position;
}

AvroOutputStream::AvroOutputStream(std::shared_ptr<arrow::io::OutputStream> output_stream,
                                   int64_t buffer_size)
    : output_stream_(std::move(output_stream)),
      buffer_size_(buffer_size),
      buffer_(buffer_size) {}

AvroOutputStream::~AvroOutputStream() = default;

bool AvroOutputStream::next(uint8_t** data, size_t* len) {
  if (buffer_pos_ > 0) {
    flush();
  }

  *data = buffer_.data();
  *len = buffer_.size();
  buffer_pos_ = buffer_.size();  // Assume all will be used until backup is called

  return true;
}

void AvroOutputStream::backup(size_t len) {
  if (len > buffer_pos_) {
    throw IcebergError(
        std::format("Cannot backup {} bytes, only {} bytes available", len, buffer_pos_));
  }
  buffer_pos_ -= len;
}

uint64_t AvroOutputStream::byteCount() const { return flushed_bytes_ + buffer_pos_; }

void AvroOutputStream::flush() {
  if (buffer_pos_ > 0) {
    auto status = output_stream_->Write(buffer_.data(), buffer_pos_);
    if (!status.ok()) {
      throw IcebergError(std::format("Write failed {}", status.ToString()));
    }
    flushed_bytes_ += buffer_pos_;
    buffer_pos_ = 0;
  }
  auto status = output_stream_->Flush();
  if (!status.ok()) {
    throw IcebergError(std::format("Flush failed {}", status.ToString()));
  }
}

}  // namespace iceberg::avro
