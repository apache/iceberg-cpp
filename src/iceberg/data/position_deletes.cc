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

#include <algorithm>
#include <format>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/data/delete_loader.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/data/position_deletes_internal.h"
#include "iceberg/deletes/dv_writer.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/location_provider.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg::internal {
namespace {

Result<DeleteWriteResult> WriteParquetDeletes(const Table& table,
                                              std::vector<FilePositionDeletes>& files,
                                              std::set<std::string>& output_paths) {
  ICEBERG_ASSIGN_OR_RAISE(auto location_provider, table.location_provider());
  ICEBERG_ASSIGN_OR_RAISE(auto schema, table.schema());

  DeleteWriteResult result;
  result.data_files.reserve(files.size());
  result.referenced_data_files.reserve(files.size());
  auto properties = table.properties().configs();
  properties[TableProperties::kParquetCompression.key()] =
      table.properties().Get(TableProperties::kDeleteParquetCompression);
  properties[TableProperties::kParquetCompressionLevel.key()] =
      table.properties().Get(TableProperties::kDeleteParquetCompressionLevel);
  const auto write_uuid = Uuid::GenerateV7().ToString();
  size_t file_number = 0;

  for (auto& [task, positions] : files) {
    const auto& data_file = *task->data_file();
    const auto& path = data_file.file_path;
    std::ranges::sort(positions);

    ICEBERG_ASSIGN_OR_RAISE(auto spec, table.metadata()->PartitionSpecById(
                                           data_file.partition_spec_id.value()));
    const auto filename =
        std::format("position-deletes-{}-{}.parquet", write_uuid, file_number++);
    auto output_path = location_provider->NewDataLocation(filename);
    output_paths.insert(output_path);

    ICEBERG_ASSIGN_OR_RAISE(auto writer,
                            PositionDeleteWriter::Make(PositionDeleteWriterOptions{
                                .path = output_path,
                                .schema = schema,
                                .spec = spec,
                                .partition = data_file.partition,
                                .format = FileFormatType::kParquet,
                                .io = table.io(),
                                .properties = properties,
                            }));
    for (int64_t pos : positions) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteDelete(path, pos));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    result.data_files.insert(result.data_files.end(),
                             std::make_move_iterator(metadata.data_files.begin()),
                             std::make_move_iterator(metadata.data_files.end()));
    result.referenced_data_files.push_back(path);
  }
  return result;
}

Result<DeleteWriteResult> WriteDeletionVectors(
    const Table& table, const std::shared_ptr<FileIO>& scan_io,
    const std::vector<FilePositionDeletes>& files, std::set<std::string>& output_paths) {
  ICEBERG_ASSIGN_OR_RAISE(auto locations, table.location_provider());
  auto path = locations->NewDataLocation(
      std::format("position-deletes-{}.puffin", Uuid::GenerateV7().ToString()));
  output_paths.insert(path);
  std::unordered_map<std::string_view, std::vector<std::shared_ptr<DataFile>>> previous;
  for (const auto& file : files) {
    for (const auto& entry : file.task->delete_files()) {
      if (entry->content == DataFile::Content::kPositionDeletes) {
        previous[file.task->data_file()->file_path].push_back(entry);
      }
    }
  }
  DeleteLoader loader(scan_io);
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      DVWriter::Make({.path = path,
                      .io = table.io(),
                      .load_previous_deletes = [&](std::string_view target)
                          -> Result<std::optional<PositionDeleteIndex>> {
                        auto it = previous.find(target);
                        if (it == previous.end()) return std::nullopt;
                        ICEBERG_ASSIGN_OR_RAISE(
                            auto index, loader.LoadPositionDeletes(it->second, target));
                        return std::optional<PositionDeleteIndex>(std::move(index));
                      }}));
  for (const auto& [task, positions] : files) {
    const auto& file = *task->data_file();
    ICEBERG_ASSIGN_OR_RAISE(
        auto spec, table.metadata()->PartitionSpecById(file.partition_spec_id.value()));
    for (int64_t pos : positions) {
      ICEBERG_RETURN_UNEXPECTED(
          writer->Delete(file.file_path, pos, spec, file.partition));
    }
  }
  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  return writer->Metadata();
}

}  // namespace

Result<DeleteWriteResult> WritePositionDeletes(const Table& table,
                                               const std::shared_ptr<FileIO>& scan_io,
                                               std::vector<FilePositionDeletes> files,
                                               std::set<std::string>& output_paths) {
  const auto version = table.metadata()->format_version;
  ICEBERG_PRECHECK(version >= 2,
                   "Position deletes require table format version 2 or later");
  std::erase_if(files, [](const auto& file) { return file.positions.empty(); });
  if (files.empty()) return DeleteWriteResult{};
  for (const auto& [task, positions] : files) {
    const auto& file = *task->data_file();
    ICEBERG_PRECHECK(file.partition_spec_id.has_value(),
                     "Missing partition spec ID for {}", file.file_path);
    for (int64_t pos : positions) {
      ICEBERG_PRECHECK(pos >= 0 && pos < file.record_count,
                       "Invalid position {} for {} ({} rows)", pos, file.file_path,
                       file.record_count);
    }
  }
  return version >= 3 ? WriteDeletionVectors(table, scan_io, files, output_paths)
                      : WriteParquetDeletes(table, files, output_paths);
}

}  // namespace iceberg::internal
