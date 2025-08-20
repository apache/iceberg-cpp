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

#include "iceberg/manifest_reader.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<ManifestReader>> ManifestReader::Make(
    const ManifestFile& manifest, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<Schema> partition_schema) {
  auto manifest_entry_schema = ManifestEntry::TypeFromPartitionType(partition_schema);
  std::shared_ptr<Schema> schema =
      FromStructType(std::move(*manifest_entry_schema), std::nullopt);

  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ReaderFactoryRegistry::Open(FileFormatType::kAvro,
                                                      {.path = manifest.manifest_path,
                                                       .length = manifest.manifest_length,
                                                       .io = std::move(file_io),
                                                       .projection = schema}));
  // Create inheritable metadata for this manifest
  ICEBERG_ASSIGN_OR_RAISE(auto inheritable_metadata,
                          InheritableMetadataFactory::FromManifest(manifest));

  return std::make_unique<ManifestReaderImpl>(std::move(reader), std::move(schema),
                                              std::move(inheritable_metadata));
}

Result<std::unique_ptr<ManifestReader>> ManifestReader::Make(
    std::string_view manifest_location, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<Schema> partition_schema) {
  auto manifest_entry_schema = ManifestEntry::TypeFromPartitionType(partition_schema);
  auto fields_span = manifest_entry_schema->fields();
  std::vector<SchemaField> fields(fields_span.begin(), fields_span.end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader, ReaderFactoryRegistry::Open(FileFormatType::kAvro,
                                               {.path = std::string(manifest_location),
                                                .io = std::move(file_io),
                                                .projection = schema}));
  ICEBERG_ASSIGN_OR_RAISE(auto inheritable_metadata, InheritableMetadataFactory::Empty());
  return std::make_unique<ManifestReaderImpl>(std::move(reader), std::move(schema),
                                              std::move(inheritable_metadata));
}

Result<std::unique_ptr<ManifestListReader>> ManifestListReader::Make(
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  std::vector<SchemaField> fields(ManifestFile::Type().fields().begin(),
                                  ManifestFile::Type().fields().end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(auto reader, ReaderFactoryRegistry::Open(
                                           FileFormatType::kAvro,
                                           {.path = std::string(manifest_list_location),
                                            .io = std::move(file_io),
                                            .projection = schema}));
  return std::make_unique<ManifestListReaderImpl>(std::move(reader), std::move(schema));
}

}  // namespace iceberg
