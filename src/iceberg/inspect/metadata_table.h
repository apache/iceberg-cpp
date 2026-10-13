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
/// \brief Base APIs for inspecting Iceberg metadata tables.

#include <concepts>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief Column selection for a metadata table scan.
struct ColumnSelection {
  /// \brief Names of the columns to return.
  ///
  /// Nested names are dot-concatenated. Columns are returned in schema order, and
  /// names that match no column are ignored. std::nullopt or a list containing `*`
  /// returns all columns. An empty list returns rows without columns.
  std::optional<std::vector<std::string>> names;

  /// \brief Whether names are matched case-sensitively.
  bool case_sensitive = true;
};

/// \brief Base interface for an Iceberg metadata table.
class ICEBERG_EXPORT MetadataTable {
 public:
  /// \brief Supported metadata table kinds.
  enum class Kind {
    kSnapshots,
    kHistory,
  };

  /// \brief Maximum number of rows emitted in each Arrow batch.
  static constexpr int64_t kBatchSize = 1024;

  /// \brief Create a metadata table of the requested concrete type.
  ///
  /// \tparam MetadataTableType Concrete class derived from MetadataTable.
  /// \param table Source table whose metadata will be exposed.
  /// \return The constructed metadata table, or an error.
  template <typename MetadataTableType>
    requires std::derived_from<MetadataTableType, MetadataTable>
  static Result<std::unique_ptr<MetadataTableType>> Make(std::shared_ptr<Table> table) {
    return MetadataTableType::Make(std::move(table));
  }

  virtual ~MetadataTable();

  /// \brief Return this metadata table's kind.
  virtual Kind kind() const noexcept = 0;

  /// \brief Return the schema of rows emitted by scans.
  virtual const std::shared_ptr<Schema>& schema() const = 0;

  /// \brief Return the source table whose metadata is exposed.
  const std::shared_ptr<Table>& source_table() const;

  /// \brief Return whether this metadata table supports time travel.
  virtual bool supports_time_travel() const noexcept;

  /// \brief Scan all columns of the metadata table without time travel.
  ///
  /// The caller owns the returned stream and must invoke its `release` callback
  /// when the stream is no longer needed.
  Result<ArrowArrayStream> Scan();

  /// \brief Scan the selected columns of the metadata table without time travel.
  ///
  /// The caller owns the returned stream and must invoke its `release` callback
  /// when the stream is no longer needed.
  ///
  /// \param columns Names of the columns to return.
  /// \return An Arrow stream containing the selected columns, or an error.
  Result<ArrowArrayStream> Scan(const ColumnSelection& columns);

 protected:
  explicit MetadataTable(std::shared_ptr<Table> source_table);

  /// \brief Implement a scan that returns the columns of a projected schema.
  ///
  /// \param projected_schema Projection of schema() with the columns to return. It
  /// is only valid during this call.
  virtual Result<ArrowArrayStream> ScanProjection(const Schema& projected_schema) = 0;

 private:
  std::shared_ptr<Table> source_table_;
};

/// \brief Snapshot selection parameters for a time-travel scan.
struct SnapshotSelection {
  /// \brief Select the current snapshot, a snapshot ID, or an as-of timestamp.
  ///
  /// std::monostate selects the current snapshot.
  std::variant<std::monostate, int64_t, TimePointMs> snapshot;

  /// \brief Resolve the snapshot relative to this branch or tag.
  ///
  /// An empty string uses the main branch.
  std::string ref_name;
};

/// \brief Base interface for metadata tables that support time travel.
///
/// Keeping snapshot selection on this capability-specific interface prevents it from
/// being exposed by metadata tables that do not support time travel.
class ICEBERG_EXPORT TimeTravelMetadataTable : public MetadataTable {
 public:
  ~TimeTravelMetadataTable() override;

  /// \brief Return true because this interface supports time travel.
  bool supports_time_travel() const noexcept final;

  // Scans without a snapshot selection read the current snapshot on the main branch.
  using MetadataTable::Scan;

  /// \brief Scan all columns using the requested snapshot selection.
  ///
  /// \param snapshot_selection Snapshot ID, timestamp, and optional ref selection.
  /// \return An Arrow stream containing the metadata table rows, or an error.
  Result<ArrowArrayStream> Scan(const SnapshotSelection& snapshot_selection);

  /// \brief Scan the selected columns using the requested snapshot selection.
  ///
  /// \param snapshot_selection Snapshot ID, timestamp, and optional ref selection.
  /// \param columns Names of the columns to return.
  /// \return An Arrow stream containing the selected columns, or an error.
  Result<ArrowArrayStream> Scan(const SnapshotSelection& snapshot_selection,
                                const ColumnSelection& columns);

 protected:
  explicit TimeTravelMetadataTable(std::shared_ptr<Table> source_table);

  /// \brief Scan the current snapshot on the main branch.
  Result<ArrowArrayStream> ScanProjection(const Schema& projected_schema) final;

  /// \brief Implement a scan for the requested snapshot selection.
  ///
  /// \param snapshot_selection Snapshot ID, timestamp, and optional ref selection.
  /// \param projected_schema Projection of schema() with the columns to return. It
  /// is only valid during this call.
  virtual Result<ArrowArrayStream> ScanSnapshot(
      const SnapshotSelection& snapshot_selection, const Schema& projected_schema) = 0;
};

}  // namespace iceberg
