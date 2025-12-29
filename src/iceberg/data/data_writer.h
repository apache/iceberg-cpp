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

/// \file iceberg/data/data_writer.h
/// Data writer for Iceberg tables.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "iceberg/arrow_c_data.h"
#include "iceberg/data/writer.h"
#include "iceberg/file_format.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Options for creating a DataWriter.
///
/// \note The following features from Java DataWriter are not yet supported:
/// - Encryption key metadata (uses FileIO instead of EncryptedOutputFile)
/// - Metrics collection and reporting
/// - Split offsets tracking
struct ICEBERG_EXPORT DataWriterOptions {
  std::string path;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<PartitionSpec> spec;
  PartitionValues partition;
  FileFormatType format = FileFormatType::kParquet;
  std::shared_ptr<FileIO> io;
  std::optional<int32_t> sort_order_id;
  std::shared_ptr<class WriterProperties> properties;
};

/// \brief Writer for Iceberg data files.
///
/// \warning Thread Safety: Writer instances are NOT thread-safe. Each writer should only
/// be used by a single thread. Do not call Write(), Close(), or Metadata() concurrently.
class ICEBERG_EXPORT DataWriter : public FileWriter {
 public:
  ~DataWriter() override;

  Status Write(ArrowArray* data) override;
  Result<int64_t> Length() const override;
  Status Close() override;
  Result<WriteResult> Metadata() override;

 private:
  friend class FileWriterFactory;
  friend std::unique_ptr<DataWriter> MakeDataWriterInternal(const DataWriterOptions&);
  class Impl;
  std::unique_ptr<Impl> impl_;
  explicit DataWriter(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg
