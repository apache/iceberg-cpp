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

#include "iceberg/data/equality_delete_writer.h"

namespace iceberg {

//=============================================================================
// EqualityDeleteWriter - stub implementation (to be completed in separate PR per #441)
//=============================================================================

class EqualityDeleteWriter::Impl {
 public:
  explicit Impl(EqualityDeleteWriterOptions options) : options_(std::move(options)) {}
  EqualityDeleteWriterOptions options_;
  bool is_closed_ = false;
};

EqualityDeleteWriter::EqualityDeleteWriter(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
EqualityDeleteWriter::~EqualityDeleteWriter() = default;

Status EqualityDeleteWriter::Write(ArrowArray* data) {
  if (!data) {
    return InvalidArgument("Cannot write null data");
  }
  if (impl_->is_closed_) {
    return Invalid("Writer is already closed");
  }
  return NotImplemented("EqualityDeleteWriter not yet implemented - see #441");
}

Result<int64_t> EqualityDeleteWriter::Length() const {
  return NotImplemented("EqualityDeleteWriter not yet implemented - see #441");
}

Status EqualityDeleteWriter::Close() {
  if (impl_->is_closed_) {
    return {};  // Close is idempotent
  }
  impl_->is_closed_ = true;
  return NotImplemented("EqualityDeleteWriter not yet implemented - see #441");
}

Result<FileWriter::WriteResult> EqualityDeleteWriter::Metadata() {
  if (!impl_->is_closed_) {
    return Invalid("Writer must be closed before getting metadata");
  }
  return NotImplemented("EqualityDeleteWriter not yet implemented - see #441");
}

const std::vector<int32_t>& EqualityDeleteWriter::equality_field_ids() const {
  return impl_->options_.equality_field_ids;
}

// Internal factory function for FileWriterFactory
std::unique_ptr<EqualityDeleteWriter> MakeEqualityDeleteWriterInternal(
    const EqualityDeleteWriterOptions& options) {
  auto impl = std::make_unique<EqualityDeleteWriter::Impl>(options);
  return std::unique_ptr<EqualityDeleteWriter>(new EqualityDeleteWriter(std::move(impl)));
}

}  // namespace iceberg
