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

#include "iceberg/file_writer.h"

#include <unordered_map>

#include "iceberg/result.h"
#include "iceberg/util/formatter.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

WriterFactory GetNotImplementedFactory(FileFormatType format_type) {
  return [format_type]() -> Result<std::unique_ptr<Writer>> {
    return NotImplemented("Missing writer factory for file format: {}", format_type);
  };
}

}  // namespace

WriterFactory& WriterFactoryRegistry::GetFactory(FileFormatType format_type) {
  static std::unordered_map<FileFormatType, WriterFactory> factories = {
      {FileFormatType::kAvro, GetNotImplementedFactory(FileFormatType::kAvro)},
      {FileFormatType::kParquet, GetNotImplementedFactory(FileFormatType::kParquet)},
      {FileFormatType::kOrc, GetNotImplementedFactory(FileFormatType::kOrc)},
      {FileFormatType::kPuffin, GetNotImplementedFactory(FileFormatType::kPuffin)},
  };
  return factories.at(format_type);
}

WriterFactoryRegistry::WriterFactoryRegistry(FileFormatType format_type,
                                             WriterFactory factory) {
  GetFactory(format_type) = std::move(factory);
}

Result<std::unique_ptr<Writer>> WriterFactoryRegistry::Open(
    FileFormatType format_type, const WriterOptions& options) {
  ICEBERG_ASSIGN_OR_RAISE(auto writer, GetFactory(format_type)());
  ICEBERG_RETURN_UNEXPECTED(writer->Open(options));
  return writer;
}

std::unique_ptr<WriterProperties> WriterProperties::default_properties() {
  return std::make_unique<WriterProperties>();
}

std::unique_ptr<WriterProperties> WriterProperties::FromMap(
    const std::unordered_map<std::string, std::string>& properties) {
  auto writer_properties = std::make_unique<WriterProperties>();
  writer_properties->configs_ = properties;
  return writer_properties;
}

}  // namespace iceberg
