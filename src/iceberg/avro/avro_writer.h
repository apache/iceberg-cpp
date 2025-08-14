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

#include "iceberg/file_writer.h"
#include "iceberg/iceberg_bundle_export.h"

namespace iceberg::avro {

/// \brief A writer for serializing ArrowArray to Avro files.
class ICEBERG_BUNDLE_EXPORT AvroWriter : public Writer {
 public:
  AvroWriter() = default;

  ~AvroWriter() override;

  Status Open(const WriterOptions& options) final;

  Status Close() final;

  Status Write(ArrowArray data) final;

  std::optional<Metrics> metrics() final;

  std::optional<int64_t> length() final;

  std::vector<int64_t> split_offsets() final;

  /// \brief Register this Avro writer implementation.
  static void Register();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg::avro
