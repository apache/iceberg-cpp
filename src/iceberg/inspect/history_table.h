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

/// \brief History metadata table
///
/// History is based on the table's snapshot log, which logs each update
/// to the table's current snapshot. Each row has columns:
/// - made_current_at (long, timestamp)
/// - snapshot_id (long)
/// - parent_id (long, optional)
/// - is_current_ancestor (bool)
class ICEBERG_EXPORT HistoryTable : public BaseMetadataTable {
 public:
  /// \brief Create a HistoryTable from table metadata
  ///
  /// \param[in] table The source table
  /// \return A HistoryTable instance or error status
  static Result<std::shared_ptr<HistoryTable>> Make(std::shared_ptr<Table> table);

  ~HistoryTable() override;

 private:
  HistoryTable(std::shared_ptr<Table> table);

  std::shared_ptr<Schema> CreateSchema();

  TableIdentifier CreateName(const TableIdentifier& source_name);
};

}  // namespace iceberg
