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

#include "iceberg/manifest_writer.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_writer_internal.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<ManifestWriter>> ManifestWriter::Make(
    int32_t format_version, int64_t snapshot_id, std::optional<int64_t> first_row_id,
    std::string_view manifest_location, std::shared_ptr<FileIO> file_io,
    std::shared_ptr<Schema> partition_schema) {
  auto manifest_entry_schema =
      ManifestEntry::TypeFromPartitionType(std::move(partition_schema));
  auto fields_span = manifest_entry_schema->fields();
  std::vector<SchemaField> fields(fields_span.begin(), fields_span.end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, WriterFactoryRegistry::Open(FileFormatType::kAvro,
                                               {.path = std::string(manifest_location),
                                                .schema = schema,
                                                .io = std::move(file_io)}));
  switch (format_version) {
    case 1:
      return std::make_unique<ManifestWriterV1>(snapshot_id, std::move(writer),
                                                std::move(schema));
    case 2:
      return std::make_unique<ManifestWriterV2>(snapshot_id, std::move(writer),
                                                std::move(schema));
    case 3:
      // first_row_id is required for V3 manifest entry
      if (!first_row_id.has_value()) {
        return InvalidManifest("first_row_id is required for V3 manifest entry");
      }
      return std::make_unique<ManifestWriterV3>(snapshot_id, first_row_id.value(),
                                                std::move(writer), std::move(schema));

    default:
      return NotSupported("Unsupported manifest format version: {}", format_version);
  }
}

Result<std::unique_ptr<ManifestListWriter>> ManifestListWriter::Make(
    int32_t format_version, int64_t snapshot_id, int64_t parent_snapshot_id,
    std::optional<int64_t> sequence_number, std::optional<int64_t> first_row_id,
    std::string_view manifest_list_location, std::shared_ptr<FileIO> file_io) {
  std::vector<SchemaField> fields(ManifestFile::Type().fields().begin(),
                                  ManifestFile::Type().fields().end());
  auto schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(auto writer, WriterFactoryRegistry::Open(
                                           FileFormatType::kAvro,
                                           {.path = std::string(manifest_list_location),
                                            .schema = schema,
                                            .io = std::move(file_io)}));
  switch (format_version) {
    case 1:
      return std::make_unique<ManifestListWriterV1>(snapshot_id, parent_snapshot_id,
                                                    std::move(writer), std::move(schema));
    case 2:
      return std::make_unique<ManifestListWriterV2>(snapshot_id, parent_snapshot_id,
                                                    sequence_number.value(),
                                                    std::move(writer), std::move(schema));
    case 3:
      // sequence_number&first_row_id is required for V3 manifest list
      if (!sequence_number.has_value()) {
        return InvalidManifestList("sequence_number is required for V3 manifest list");
      }
      if (!first_row_id.has_value()) {
        return InvalidManifestList("first_row_id is required for V3 manifest list");
      }
      return std::make_unique<ManifestListWriterV3>(
          snapshot_id, parent_snapshot_id, sequence_number.value(), first_row_id.value(),
          std::move(writer), std::move(schema));

    default:
      return NotSupported("Unsupported manifest list format version: {}", format_version);
  }
}

}  // namespace iceberg
