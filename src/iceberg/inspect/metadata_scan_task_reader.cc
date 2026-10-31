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

#include "iceberg/inspect/metadata_scan_task_reader.h"

#include <memory>
#include <utility>

#include "iceberg/inspect/history_table.h"
#include "iceberg/inspect/metadata_scan_task.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/snapshots_table.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

MetadataScanTaskReader::MetadataScanTaskReader(Options options)
    : options_(std::move(options)) {}

MetadataScanTaskReader::~MetadataScanTaskReader() = default;

Result<std::unique_ptr<MetadataScanTaskReader>> MetadataScanTaskReader::Make(
    Options options) {
  ICEBERG_PRECHECK(options.batch_size > 0, "batch size must be positive");
  return std::unique_ptr<MetadataScanTaskReader>(
      new MetadataScanTaskReader(std::move(options)));
}

Result<ArrowArrayStream> MetadataScanTaskReader::Open(
    const MetadataScanTask& task) const {
  switch (task.task_kind()) {
    case MetadataScanTask::TaskKind::kStatic: {
      const auto& static_task = internal::checked_cast<const StaticMetadataTask&>(task);
      switch (static_task.table_kind()) {
        case MetadataTableKind::kSnapshots:
          return SnapshotsTable::ReadRows(static_task.metadata(),
                                          static_task.projected_schema(),
                                          options_.batch_size);
        case MetadataTableKind::kHistory:
          return HistoryTable::ReadRows(static_task.metadata(),
                                        static_task.projected_schema(),
                                        options_.batch_size);
        default:
          return NotImplemented("Static metadata table kind is not implemented");
      }
    }
  }
  return NotImplemented("Metadata scan task kind is not implemented");
}

}  // namespace iceberg
