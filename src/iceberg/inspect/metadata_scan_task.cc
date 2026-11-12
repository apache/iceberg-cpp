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
      table_kind != MetadataTableKind::kHistory) {
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

}  // namespace iceberg
