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

/// \file iceberg/data/equality_delete_writer.h
/// Equality delete writer for Iceberg tables.

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>

#include "iceberg/arrow_c_data.h"
#include "iceberg/data/writer.h"
#include "iceberg/file_format.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Options for creating an EqualityDeleteWriter.
struct ICEBERG_EXPORT EqualityDeleteWriterOptions {
  std::string path;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<PartitionSpec> spec;
  PartitionValues partition;
  FileFormatType format = FileFormatType::kParquet;
  std::shared_ptr<FileIO> io;
  std::vector<int32_t> equality_field_ids;
  std::optional<int32_t> sort_order_id;
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Writer for Iceberg equality delete files.
class ICEBERG_EXPORT EqualityDeleteWriter : public FileWriter {
 public:
  ~EqualityDeleteWriter() override;

  Status Write(ArrowArray* data) override;
  Result<int64_t> Length() const override;
  Status Close() override;
  Result<WriteResult> Metadata() override;

  std::span<const int32_t> equality_field_ids() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
