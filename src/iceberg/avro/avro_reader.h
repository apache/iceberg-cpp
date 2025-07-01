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

#include "iceberg/file_reader.h"
#include "iceberg/iceberg_bundle_export.h"

namespace iceberg::avro {

/// \brief A reader that reads ArrowArray from Avro files.
class ICEBERG_BUNDLE_EXPORT AvroReader : public Reader {
 public:
  AvroReader() = default;

  ~AvroReader() override = default;

  Status Open(const ReaderOptions& options) final;

  Status Close() final;

  Result<std::optional<ArrowArray>> Next() final;

  Result<ArrowSchema> Schema() final;

  /// \brief Register this Avro reader implementation.
  static void Register();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg::avro
