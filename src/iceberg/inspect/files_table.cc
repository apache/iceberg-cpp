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

#include "iceberg/inspect/files_table.h"

#include <memory>
#include <mutex>
#include <utility>

#include "iceberg/inspect/files_table_util_internal.h"
#include "iceberg/inspect/metadata_scan_util_internal.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

FilesTable::FilesTable(std::shared_ptr<const Table> table)
    : MetadataTable(std::move(table)) {}

FilesTable::~FilesTable() = default;

std::shared_ptr<Schema> FilesTable::schema() const {
  std::lock_guard lock(schema_mutex_);
  return schema_;
}

Result<std::unique_ptr<FilesTable>> FilesTable::Make(std::shared_ptr<const Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  auto files = std::unique_ptr<FilesTable>(new FilesTable(std::move(table)));
  ICEBERG_RETURN_UNEXPECTED(files->SchemaForScan(*files->source_table()->metadata()));
  return files;
}

Result<std::shared_ptr<Schema>> FilesTable::SchemaForScan(
    const TableMetadata& metadata) const {
  ICEBERG_ASSIGN_OR_RAISE(auto table_schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, internal::UnifiedPartitionType(metadata));
  ICEBERG_ASSIGN_OR_RAISE(auto schema,
                          internal::FilesTableSchema(*table_schema, partition_type));
  {
    std::lock_guard lock(schema_mutex_);
    schema_ = schema;
  }
  return schema;
}

Result<std::unique_ptr<MetadataScan>> FilesTable::MakeScan(
    internal::MetadataScanContext context) const {
  return internal::MakeSnapshotMetadataScan(std::move(context), ReadRows);
}

Result<ArrowArrayStream> FilesTable::ReadRows(
    const internal::MetadataScanContext& context, const std::shared_ptr<FileIO>& io,
    int64_t batch_size) {
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type,
                          internal::UnifiedPartitionType(*context.table_metadata));
  ICEBERG_ASSIGN_OR_RAISE(auto table_schema, context.table_metadata->Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, internal::ResolveMetadataTableSnapshot(
                                             *context.table_metadata, context));
  ICEBERG_ASSIGN_OR_RAISE(auto files,
                          internal::LiveFiles(*context.table_metadata, io, snapshot));
  auto schema = context.projected_schema;
  const int32_t readable_metrics_id = context.schema->fields().back().field_id();
  return internal::MakeMetadataBatchStream(
      schema, std::move(files), batch_size,
      [schema, readable_metrics_id, table_schema = std::move(table_schema),
       partition_type = std::move(partition_type)](ArrowRowBuilder& builder,
                                                   const internal::LiveFile& file) {
        return internal::AppendDataFile(builder, *schema, *table_schema, *partition_type,
                                        readable_metrics_id, file);
      });
}

}  // namespace iceberg
