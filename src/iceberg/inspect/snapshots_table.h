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

/// \file iceberg/inspect/snapshots_table.h
/// \brief Define the snapshots metadata table.

#include <cstdint>
#include <memory>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_scan.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Snapshots metadata table.
class ICEBERG_EXPORT SnapshotsTable : public MetadataTable {
 public:
  static Result<std::unique_ptr<SnapshotsTable>> Make(std::shared_ptr<const Table> table);

  /// \brief Return the canonical schema of the snapshots metadata table.
  static const std::shared_ptr<Schema>& CanonicalSchema();

  ~SnapshotsTable() override;

  MetadataTableKind kind() const noexcept override {
    return MetadataTableKind::kSnapshots;
  }

  const std::shared_ptr<Schema>& schema() const override;

 protected:
  Result<std::unique_ptr<MetadataScan>> MakeScan(
      internal::MetadataScanContext context) const override;

 private:
  friend class MetadataScanTaskReader;

  static Result<ArrowArrayStream> ReadRows(
      std::shared_ptr<const TableMetadata> metadata,
      const std::shared_ptr<Schema>& projected_schema, int64_t batch_size);

  explicit SnapshotsTable(std::shared_ptr<const Table> table);
};

/// \brief Scan the snapshots metadata captured from a table.
class ICEBERG_EXPORT SnapshotsScan final : public MetadataScan {
 public:
  /// \brief Create a scan from captured metadata table settings.
  static Result<std::unique_ptr<SnapshotsScan>> Make(
      internal::MetadataScanContext context);

  ~SnapshotsScan() override;

  Result<MetadataScanTaskStreamPtr> PlanTasksStream() const override;

 private:
  SnapshotsScan(internal::MetadataScanContext context,
                std::shared_ptr<MetadataScanTask> task);

  const std::shared_ptr<MetadataScanTask> task_;
};

}  // namespace iceberg
