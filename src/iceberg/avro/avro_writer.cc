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

#include "iceberg/avro/avro_writer.h"

#include <memory>

#include <arrow/array/builder_base.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <avro/DataFile.hh>
#include <avro/GenericDatum.hh>
#include <avro/NodeImpl.hh>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/avro/avro_stream_internal.h"
#include "iceberg/schema.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

namespace {

Result<std::unique_ptr<AvroOutputStream>> CreateOutputStream(const WriterOptions& options,
                                                             int64_t buffer_size) {
  auto io = internal::checked_pointer_cast<arrow::ArrowFileSystemFileIO>(options.io);
  auto result = io->fs()->OpenOutputStream(options.path);
  if (!result.ok()) {
    return IOError("Failed to open file {} for {}", options.path,
                   result.status().message());
  }
  return std::make_unique<AvroOutputStream>(result.MoveValueUnsafe(), buffer_size);
}

}  // namespace

// A stateful context to keep track of the writing progress.
struct WriteContext {};

class AvroWriter::Impl {
 public:
  Status Open(const WriterOptions& options) {
    write_schema_ = options.schema;

    auto root = std::make_shared<::avro::NodeRecord>();
    ToAvroNodeVisitor visitor;
    for (const auto& field : write_schema_->fields()) {
      ::avro::NodePtr node;
      ICEBERG_RETURN_UNEXPECTED(visitor.Visit(field, &node));
      root->addLeaf(node);
    }
    avro_schema_ = std::make_shared<::avro::ValidSchema>(root);

    // Open the output stream and adapt to the avro interface.
    constexpr int64_t kDefaultBufferSize = 1024 * 1024;
    ICEBERG_ASSIGN_OR_RAISE(auto output_stream,
                            CreateOutputStream(options, kDefaultBufferSize));

    writer_ = std::make_unique<::avro::DataFileWriter<::avro::GenericDatum>>(
        std::move(output_stream), *avro_schema_);
    return {};
  }

  Status Write(ArrowArray /*data*/) {
    if (!context_) {
      ICEBERG_RETURN_UNEXPECTED(InitWriteContext());
    }
    // TODO(xiao.dong) convert data and write to avro
    // total_bytes_+= written_bytes;
    return {};
  }

  Status Close() {
    if (writer_ != nullptr) {
      writer_->close();
      writer_.reset();
    }
    context_.reset();
    return {};
  }

  bool Closed() const { return writer_ == nullptr; }

  int64_t length() { return total_bytes_; }

 private:
  Status InitWriteContext() { return {}; }

 private:
  int64_t total_bytes_ = 0;
  // The schema to write.
  std::shared_ptr<::iceberg::Schema> write_schema_;
  // The avro schema to write.
  std::shared_ptr<::avro::ValidSchema> avro_schema_;
  // The avro writer to write the data into a datum.
  std::unique_ptr<::avro::DataFileWriter<::avro::GenericDatum>> writer_;
  // The context to keep track of the writing progress.
  std::unique_ptr<WriteContext> context_;
};

AvroWriter::~AvroWriter() = default;

Status AvroWriter::Write(ArrowArray data) { return impl_->Write(data); }

Status AvroWriter::Open(const WriterOptions& options) {
  impl_ = std::make_unique<Impl>();
  return impl_->Open(options);
}

Status AvroWriter::Close() {
  if (!impl_->Closed()) {
    return impl_->Close();
  }
  return {};
}

std::shared_ptr<Metrics> AvroWriter::metrics() {
  if (impl_->Closed()) {
    // TODO(xiao.dong) implement metrics
    return std::make_shared<Metrics>();
  }
  return nullptr;
}

int64_t AvroWriter::length() {
  if (impl_->Closed()) {
    return impl_->length();
  }
  return 0;
}

std::vector<int64_t> AvroWriter::splitOffsets() { return {}; }

void AvroWriter::Register() {
  static WriterFactoryRegistry avro_writer_register(
      FileFormatType::kAvro,
      []() -> Result<std::unique_ptr<Writer>> { return std::make_unique<AvroWriter>(); });
}

}  // namespace iceberg::avro
