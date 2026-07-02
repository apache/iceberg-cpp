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

/// \file iceberg/inspect/metadata_table.h
/// \brief Define base APIs for metadata tables.

#include <memory>
#include <optional>
#include <string>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief Parameters for snapshot selection (time travel).
struct SnapshotSelection {
  /// \brief The snapshot ID to read.
  std::optional<int64_t> snapshot_id;
  /// \brief Read the snapshot that was current at this timestamp.
  std::optional<TimePointMs> as_of_timestamp;
  /// \brief Read the snapshot referenced by this named ref (branch or tag).
  std::optional<std::string> ref_name;
};

/// \brief Base class for Iceberg metadata tables.
class ICEBERG_EXPORT MetadataTable {
 public:
  enum class Kind {
    kSnapshots,
    kHistory,
  };

  static Result<std::unique_ptr<MetadataTable>> Make(std::shared_ptr<Table> table,
                                                     Kind kind);

  virtual ~MetadataTable();

  virtual Kind kind() const noexcept = 0;

  /// \brief Whether this metadata table supports time-travel queries.
  ///
  /// Time travel is supported for tables that read from a single snapshot's
  /// manifests (e.g., Entries, Files, Manifests, Partitions). Tables that
  /// scan all snapshots (All*) or return in-memory history (Snapshots,
  /// History, Refs) do not support time travel.
  bool supports_time_travel() const noexcept;

  /// \brief Scan the metadata table using the current snapshot.
  ///
  /// Convenience overload — delegates to Scan(std::nullopt).
  Result<ArrowArray> Scan() { return Scan(std::nullopt); }

  /// \brief Scan the metadata table and return all rows as an Arrow struct array.
  ///
  /// The returned ArrowArray is a struct array where each element is one row.
  /// The caller takes ownership and must call ArrowArrayRelease when done.
  ///
  /// The default implementation returns NotSupported. Subclasses override this
  /// to materialize their data.
  virtual Result<ArrowArray> Scan(std::optional<SnapshotSelection> snapshot_selection);

  const TableIdentifier& name() const { return identifier_; }

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  const std::shared_ptr<Table>& source_table() const { return source_table_; }

 protected:
  explicit MetadataTable(std::shared_ptr<Table> source_table, TableIdentifier identifier,
                         std::shared_ptr<Schema> schema);

 private:
  TableIdentifier identifier_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Table> source_table_;
};

}  // namespace iceberg
