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

#include "iceberg/table_scan.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/util/macros.h"

namespace iceberg {

TableScanBuilder::TableScanBuilder(const Table& table) : table_(table) {}

TableScanBuilder& TableScanBuilder::WithColumnNames(
    const std::vector<std::string>& column_names) {
  column_names_ = column_names;
  return *this;
}

TableScanBuilder& TableScanBuilder::WithSnapshotId(int64_t snapshot_id) {
  snapshot_id_ = snapshot_id;
  return *this;
}

TableScanBuilder& TableScanBuilder::WithFilter(
    const std::shared_ptr<Expression>& filter) {
  filter_ = filter;
  return *this;
}

Result<std::unique_ptr<TableScan>> TableScanBuilder::Build() {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, snapshot_id_ ? table_.snapshot(*snapshot_id_)
                                                      : Result<std::shared_ptr<Snapshot>>(
                                                            table_.current_snapshot()));

  auto ResolveSchema = [&]() -> Result<std::shared_ptr<Schema>> {
    if (snapshot->schema_id) {
      const auto& schemas = table_.schemas();
      if (auto it = schemas.find(*snapshot->schema_id); it != schemas.end()) {
        return it->second;
      }
      return InvalidData("Schema {} in snapshot {} is not found", *snapshot->schema_id,
                         snapshot->snapshot_id);
    }
    return table_.schema();
  };

  ICEBERG_ASSIGN_OR_RAISE(auto schema, ResolveSchema());

  std::vector<int32_t> field_ids;
  field_ids.reserve(column_names_.size());
  for (const auto& column_name : column_names_) {
    auto field_opt = schema->GetFieldByName(column_name);
    if (!field_opt) {
      return InvalidArgument("Column {} not found in schema", column_name);
    }
    field_ids.emplace_back(field_opt.value().get().field_id());
  }

  auto context = std::make_unique<TableScan::ScanContext>(
      std::move(snapshot), std::move(schema), std::move(field_ids), std::move(filter_));
  return std::make_unique<TableScan>(std::move(context), table_.io());
}

TableScan::TableScan(std::unique_ptr<ScanContext> context,
                     std::shared_ptr<FileIO> file_io)
    : context_(std::move(context)), file_io_(std::move(file_io)) {}

Result<std::vector<std::shared_ptr<FileScanTask>>> TableScan::PlanFiles() const {
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_list_reader,
                          CreateManifestListReader(context_->snapshot_->manifest_list));
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_files, manifest_list_reader->Files());

  std::vector<std::shared_ptr<FileScanTask>> tasks;
  for (const auto& manifest_file : manifest_files) {
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_reader,
                            CreateManifestReader(manifest_file->manifest_path));
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, manifest_reader->Entries());

    for (const auto& manifest : manifests) {
      const auto& data_file = manifest->data_file;
      tasks.emplace_back(std::make_shared<FileScanTask>(
          data_file.file_path, 0, data_file.file_size_in_bytes, data_file.record_count,
          data_file.content, data_file.file_format, context_->schema_,
          context_->field_ids_, context_->filter_));
    }
  }
  return tasks;
}

Result<std::unique_ptr<ManifestListReader>> TableScan::CreateManifestListReader(
    const std::string& file_path) const {
  return NotImplemented("manifest list reader");
}

Result<std::unique_ptr<ManifestReader>> TableScan::CreateManifestReader(
    const std::string& file_path) const {
  return NotImplemented("manifest reader");
}

}  // namespace iceberg
