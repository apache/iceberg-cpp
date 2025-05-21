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

#pragma once

#include <arrow/io/interfaces.h>
#include <avro/Stream.hh>

namespace iceberg::avro {

class AvroInputStream : public ::avro::SeekableInputStream {
 public:
  explicit AvroInputStream(std::shared_ptr<::arrow::io::RandomAccessFile> input_stream,
                           int64_t buffer_size);

  ~AvroInputStream() override;

  /// \brief Returns some of available data.
  /// \return true if some data is available, false if no more data is available or an
  /// error has occurred.
  bool next(const uint8_t** data, size_t* len) override;

  /// \brief "Returns" back some of the data to the stream. The returned data must be less
  /// than what was obtained in the last call to next().
  void backup(size_t len) override;

  /// \brief Skips number of bytes specified by len.
  void skip(size_t len) override;

  /// \brief Returns the number of bytes read from this stream so far.
  /// All the bytes made available through next are considered to be used unless,
  /// returned back using backup.
  size_t byteCount() const override;

  /// \brief Seek to a specific position in the stream. This may invalidate pointers
  /// returned from next(). This will also reset byteCount() to the given
  /// position.
  void seek(int64_t position) override;

 private:
  std::shared_ptr<::arrow::io::RandomAccessFile> input_stream_;
  const int64_t buffer_size_;
  std::vector<uint8_t> buffer_;
  size_t byte_count_ = 0;       // bytes read from the input stream
  size_t buffer_pos_ = 0;       // next position to read in the buffer
  size_t available_bytes_ = 0;  // bytes available in the buffer
};

class AvroOutputStream : public ::avro::OutputStream {
 public:
  explicit AvroOutputStream(std::shared_ptr<::arrow::io::OutputStream> output_stream,
                            int64_t buffer_size);

  ~AvroOutputStream() override;

  /// \brief Returns a buffer that can be written into.
  /// On successful return, data has the pointer to the buffer
  /// and len has the number of bytes available at data.
  bool next(uint8_t** data, size_t* len) override;

  /// \brief "Returns" back to the stream some of the buffer obtained
  /// from in the last call to next().
  void backup(size_t len) override;

  /// \brief Number of bytes written so far into this stream. The whole buffer
  /// returned by next() is assumed to be written unless some of
  /// it was returned using backup().
  uint64_t byteCount() const override;

  /// \brief Flushes any data remaining in the buffer to the stream's underlying
  /// store, if any.
  void flush() override;

 private:
  std::shared_ptr<::arrow::io::OutputStream> output_stream_;
  const int64_t buffer_size_;
  std::vector<uint8_t> buffer_;
  size_t buffer_pos_ = 0;       // position in the buffer
  uint64_t flushed_bytes_ = 0;  // bytes flushed to the output stream
};

}  // namespace iceberg::avro
