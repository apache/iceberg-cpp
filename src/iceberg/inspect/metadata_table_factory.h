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
#include "iceberg/result.h"
#include "iceberg/table.h"

namespace iceberg {

class HistoryTable;
class SnapshotsTable;
class Table;

/// \brief Metadata table factory and inspector
///
/// MetadataTable provides factory methods to create specific metadata tables for
/// inspecting table metadata. Each metadata table exposes a different aspect of the
/// table's metadata as a scannable Iceberg table.
///
/// Usage:
///   auto snapshots = ICEBERG_TRY(MetadataTable::GetSnapshotsTable(table));
///   auto scan = ICEBERG_TRY(snapshots->NewScan());
///   // ... scan and read snapshot data
class ICEBERG_EXPORT MetadataTableFactory {
 public:
  /// \brief Create a SnapshotsTable from a table
  ///
  /// \param table The source table
  /// \return A SnapshotsTable exposing all snapshots or error status
  static Result<std::shared_ptr<SnapshotsTable>> GetSnapshotsTable(
      std::shared_ptr<Table> table);

  /// \brief Create a HistoryTable from a table
  ///
  /// \param table The source table
  /// \return A HistoryTable exposing snapshot history or error status
  static Result<std::shared_ptr<HistoryTable>> GetHistoryTable(
      std::shared_ptr<Table> table);
};

}  // namespace iceberg
