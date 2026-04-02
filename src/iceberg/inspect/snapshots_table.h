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

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/result.h"
#include "iceberg/table.h"

namespace iceberg {

/// \brief Snapshots metadata table
///
/// Exposes all snapshots in the table as rows with columns:
/// - committed_at (timestamp)
/// - snapshot_id (long)
/// - parent_id (long)
/// - manifest_list (string)
/// - summary (map<string, string>)
class ICEBERG_EXPORT SnapshotsTable : public BaseMetadataTable {
 public:
  /// \brief Create a SnapshotsTable from table metadata
  ///
  /// \param[in] table The source table
  /// \return A SnapshotsTable instance or error status
  static Result<std::shared_ptr<SnapshotsTable>> Make(std::shared_ptr<Table> table);

  ~SnapshotsTable() override;

 private:
  SnapshotsTable(std::shared_ptr<Table> table);

  std::shared_ptr<Schema> CreateSchema();

  TableIdentifier CreateName(const TableIdentifier& source_name);
};

}  // namespace iceberg
