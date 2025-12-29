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

/// \file iceberg/data/file_writer_factory.h
/// Factory for creating Iceberg file writers.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iceberg/file_format.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

// Forward declarations
class DataWriter;
class PositionDeleteWriter;
class EqualityDeleteWriter;

/// \brief Factory for creating Iceberg file writers.
///
/// \warning Thread Safety: This class is NOT thread-safe. Each FileWriterFactory instance
/// should only be used by a single thread. To use from multiple threads, create separate
/// factory instances or use external synchronization.
///
/// \note Differences from Java FileWriterFactory:
/// - Java uses EncryptedOutputFile parameter, C++ uses path + FileIO
/// - C++ factory has state (schema, spec, io) configured once, reused for all writers
/// - Java FileWriterFactory is an interface, C++ is a concrete class with configuration
/// - C++ provides SetEqualityDeleteConfig() and SetPositionDeleteRowSchema() for
/// customization
class ICEBERG_EXPORT FileWriterFactory {
 public:
  FileWriterFactory(std::shared_ptr<Schema> schema, std::shared_ptr<PartitionSpec> spec,
                    std::shared_ptr<FileIO> io,
                    std::shared_ptr<class WriterProperties> properties = nullptr);
  ~FileWriterFactory();

  void SetEqualityDeleteConfig(std::shared_ptr<Schema> eq_delete_schema,
                               std::vector<int32_t> equality_field_ids);
  void SetPositionDeleteRowSchema(std::shared_ptr<Schema> pos_delete_row_schema);

  Result<std::unique_ptr<DataWriter>> NewDataWriter(
      std::string path, FileFormatType format, PartitionValues partition,
      std::optional<int32_t> sort_order_id = std::nullopt);

  Result<std::unique_ptr<PositionDeleteWriter>> NewPositionDeleteWriter(
      std::string path, FileFormatType format, PartitionValues partition);

  Result<std::unique_ptr<EqualityDeleteWriter>> NewEqualityDeleteWriter(
      std::string path, FileFormatType format, PartitionValues partition,
      std::optional<int32_t> sort_order_id = std::nullopt);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
