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

#pragma once

/// \file iceberg/incremental_append_scan.h
/// \brief Scan that reads the data files appended between snapshots.

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A scan that reads data files added between snapshots (incremental appends).
class ICEBERG_EXPORT IncrementalAppendScan : public IncrementalScan<FileScanTask> {
 public:
  /// \brief Constructs an IncrementalAppendScan instance.
  static Result<std::unique_ptr<IncrementalAppendScan>> Make(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileIO> io, internal::TableScanContext context);

  ~IncrementalAppendScan() override = default;

  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles() const override;

 protected:
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles(
      std::optional<int64_t> from_snapshot_id_exclusive,
      int64_t to_snapshot_id_inclusive) const override;

  using IncrementalScan::IncrementalScan;
};

extern template class ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT
    TableScanBuilder<IncrementalAppendScan>;

}  // namespace iceberg
