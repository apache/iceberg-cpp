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

#include "iceberg/file_reader.h"

#include <unordered_map>

#include "iceberg/result.h"
#include "iceberg/util/formatter.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

ReaderFactory GetNotImplementedFactory(FileFormatType format_type) {
  return [format_type]() -> Result<std::unique_ptr<Reader>> {
    return NotImplemented("Missing reader factory for file format: {}", format_type);
  };
}

}  // namespace

ReaderFactory& ReaderFactoryRegistry::GetFactory(FileFormatType format_type) {
  static std::unordered_map<FileFormatType, ReaderFactory> factories = {
      {FileFormatType::kAvro, GetNotImplementedFactory(FileFormatType::kAvro)},
      {FileFormatType::kParquet, GetNotImplementedFactory(FileFormatType::kParquet)},
      {FileFormatType::kOrc, GetNotImplementedFactory(FileFormatType::kOrc)},
      {FileFormatType::kPuffin, GetNotImplementedFactory(FileFormatType::kPuffin)},
  };
  return factories.at(format_type);
}

ReaderFactoryRegistry::ReaderFactoryRegistry(FileFormatType format_type,
                                             ReaderFactory factory) {
  GetFactory(format_type) = std::move(factory);
}

Result<std::unique_ptr<Reader>> ReaderFactoryRegistry::Open(
    FileFormatType format_type, const ReaderOptions& options) {
  ICEBERG_ASSIGN_OR_RAISE(auto reader, GetFactory(format_type)());
  ICEBERG_RETURN_UNEXPECTED(reader->Open(options));
  return reader;
}

std::unique_ptr<ReaderProperties> ReaderProperties::default_properties() {
  return std::make_unique<ReaderProperties>();
}

std::unique_ptr<ReaderProperties> ReaderProperties::FromMap(
    const std::unordered_map<std::string, std::string>& properties) {
  auto reader_properties = std::make_unique<ReaderProperties>();
  reader_properties->configs_ = properties;
  return reader_properties;
}

}  // namespace iceberg
