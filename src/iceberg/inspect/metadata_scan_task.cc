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

#include "iceberg/inspect/metadata_scan_task.h"

#include <utility>

#include "iceberg/inspect/metadata_table.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// StaticMetadataTask

Result<std::shared_ptr<StaticMetadataTask>> StaticMetadataTask::Make(
    MetadataTableKind table_kind, std::shared_ptr<const TableMetadata> metadata,
    std::shared_ptr<Schema> projected_schema) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(projected_schema != nullptr, "Projected schema cannot be null");
  if (table_kind != MetadataTableKind::kSnapshots &&
      table_kind != MetadataTableKind::kHistory &&
      table_kind != MetadataTableKind::kRefs) {
    return NotImplemented("Static metadata table kind is not implemented");
  }
  return std::shared_ptr<StaticMetadataTask>(new StaticMetadataTask(
      table_kind, std::move(metadata), std::move(projected_schema)));
}

StaticMetadataTask::StaticMetadataTask(MetadataTableKind table_kind,
                                       std::shared_ptr<const TableMetadata> metadata,
                                       std::shared_ptr<Schema> projected_schema)
    : table_kind_(table_kind),
      metadata_(std::move(metadata)),
      projected_schema_(std::move(projected_schema)) {}

// SnapshotMetadataTask

Result<std::shared_ptr<SnapshotMetadataTask>> SnapshotMetadataTask::Make(
    internal::MetadataScanContext context, std::shared_ptr<FileIO> io,
    ReadRowsFunction read_rows) {
  ICEBERG_PRECHECK(read_rows != nullptr, "Snapshot row reader cannot be null");
  ICEBERG_RETURN_UNEXPECTED(context.Validate());
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::shared_ptr<SnapshotMetadataTask>(
      new SnapshotMetadataTask(std::move(context), std::move(io), read_rows));
}

SnapshotMetadataTask::SnapshotMetadataTask(internal::MetadataScanContext context,
                                           std::shared_ptr<FileIO> io,
                                           ReadRowsFunction read_rows)
    : context_(std::move(context)), io_(std::move(io)), read_rows_(read_rows) {}

Result<ArrowArrayStream> SnapshotMetadataTask::Open(int64_t batch_size) const {
  return read_rows_(context_, io_, batch_size);
}

}  // namespace iceberg
