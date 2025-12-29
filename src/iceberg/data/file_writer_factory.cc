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

#include "iceberg/data/file_writer_factory.h"

#include "iceberg/data/data_writer.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/position_delete_writer.h"

namespace iceberg {

// Forward declarations for internal factory functions
std::unique_ptr<DataWriter> MakeDataWriterInternal(const DataWriterOptions& options);
std::unique_ptr<PositionDeleteWriter> MakePositionDeleteWriterInternal(
    const PositionDeleteWriterOptions& options);
std::unique_ptr<EqualityDeleteWriter> MakeEqualityDeleteWriterInternal(
    const EqualityDeleteWriterOptions& options);

//=============================================================================
// FileWriterFactory::Impl
//=============================================================================

class FileWriterFactory::Impl {
 public:
  Impl(std::shared_ptr<Schema> schema, std::shared_ptr<PartitionSpec> spec,
       std::shared_ptr<FileIO> io, std::shared_ptr<WriterProperties> properties)
      : schema_(std::move(schema)),
        spec_(std::move(spec)),
        io_(std::move(io)),
        properties_(std::move(properties)) {}

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<WriterProperties> properties_;

  std::shared_ptr<Schema> eq_delete_schema_;
  std::vector<int32_t> equality_field_ids_;
  std::shared_ptr<Schema> pos_delete_row_schema_;
};

//=============================================================================
// FileWriterFactory
//=============================================================================

FileWriterFactory::FileWriterFactory(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<PartitionSpec> spec,
                                     std::shared_ptr<FileIO> io,
                                     std::shared_ptr<WriterProperties> properties)
    : impl_(std::make_unique<Impl>(std::move(schema), std::move(spec), std::move(io),
                                   std::move(properties))) {}

FileWriterFactory::~FileWriterFactory() = default;

void FileWriterFactory::SetEqualityDeleteConfig(std::shared_ptr<Schema> eq_delete_schema,
                                                std::vector<int32_t> equality_field_ids) {
  impl_->eq_delete_schema_ = std::move(eq_delete_schema);
  impl_->equality_field_ids_ = std::move(equality_field_ids);
}

void FileWriterFactory::SetPositionDeleteRowSchema(
    std::shared_ptr<Schema> pos_delete_row_schema) {
  impl_->pos_delete_row_schema_ = std::move(pos_delete_row_schema);
}

Result<std::unique_ptr<DataWriter>> FileWriterFactory::NewDataWriter(
    std::string path, FileFormatType format, PartitionValues partition,
    std::optional<int32_t> sort_order_id) {
  // Input validation
  if (path.empty()) {
    return InvalidArgument("Path cannot be empty");
  }
  if (!impl_->schema_) {
    return InvalidArgument("Schema cannot be null");
  }
  if (!impl_->spec_) {
    return InvalidArgument("PartitionSpec cannot be null");
  }

  DataWriterOptions options;
  options.path = std::move(path);
  options.schema = impl_->schema_;
  options.spec = impl_->spec_;
  options.partition = std::move(partition);
  options.format = format;
  options.io = impl_->io_;
  options.sort_order_id = sort_order_id;
  options.properties = impl_->properties_;

  return MakeDataWriterInternal(options);
}

Result<std::unique_ptr<PositionDeleteWriter>> FileWriterFactory::NewPositionDeleteWriter(
    std::string path, FileFormatType format, PartitionValues partition) {
  // Input validation
  if (path.empty()) {
    return InvalidArgument("Path cannot be empty");
  }
  if (!impl_->schema_) {
    return InvalidArgument("Schema cannot be null");
  }
  if (!impl_->spec_) {
    return InvalidArgument("PartitionSpec cannot be null");
  }

  PositionDeleteWriterOptions options;
  options.path = std::move(path);
  options.schema = impl_->schema_;
  options.spec = impl_->spec_;
  options.partition = std::move(partition);
  options.format = format;
  options.io = impl_->io_;
  options.row_schema = impl_->pos_delete_row_schema_;
  options.properties = impl_->properties_;

  return MakePositionDeleteWriterInternal(options);
}

Result<std::unique_ptr<EqualityDeleteWriter>> FileWriterFactory::NewEqualityDeleteWriter(
    std::string path, FileFormatType format, PartitionValues partition,
    std::optional<int32_t> sort_order_id) {
  // Input validation
  if (path.empty()) {
    return InvalidArgument("Path cannot be empty");
  }
  if (!impl_->schema_) {
    return InvalidArgument("Schema cannot be null");
  }
  if (!impl_->spec_) {
    return InvalidArgument("PartitionSpec cannot be null");
  }
  if (impl_->equality_field_ids_.empty()) {
    return InvalidArgument(
        "Equality field IDs cannot be empty - call SetEqualityDeleteConfig first");
  }

  EqualityDeleteWriterOptions options;
  options.path = std::move(path);
  options.schema = impl_->eq_delete_schema_ ? impl_->eq_delete_schema_ : impl_->schema_;
  options.spec = impl_->spec_;
  options.partition = std::move(partition);
  options.format = format;
  options.io = impl_->io_;
  options.equality_field_ids = impl_->equality_field_ids_;
  options.sort_order_id = sort_order_id;
  options.properties = impl_->properties_;

  return MakeEqualityDeleteWriterInternal(options);
}

}  // namespace iceberg
