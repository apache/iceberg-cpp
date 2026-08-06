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
#include <string>
#include <utility>
#include <variant>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief Base interface for an Iceberg metadata table.
class ICEBERG_EXPORT MetadataTable {
 public:
  /// \brief Supported metadata table kinds.
  enum class Kind {
    kSnapshots,
    kHistory,
    kBranches,
    kTags,
    kFiles,
    kPartitions,
    kManifests,
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

  /// \brief Scan the metadata table without time travel.
  ///
  /// The caller owns the returned stream and must release it with
  /// ArrowArrayStreamRelease.
  virtual Result<ArrowArrayStream> Scan() = 0;

 protected:
  explicit MetadataTable(std::shared_ptr<Table> source_table);

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
class ICEBERG_EXPORT TimeTravelMetadataTable : public MetadataTable {
 public:
  ~TimeTravelMetadataTable() override;

  /// \brief Return true because this interface supports time travel.
  bool supports_time_travel() const noexcept final;

  /// \brief Scan using the current snapshot on the main branch.
  Result<ArrowArrayStream> Scan() final;

  /// \brief Scan using the requested snapshot selection.
  ///
  /// \param snapshot_selection Snapshot ID, timestamp, and optional ref selection.
  /// \return An Arrow stream containing the metadata table rows, or an error.
  Result<ArrowArrayStream> Scan(const SnapshotSelection& snapshot_selection);

 protected:
  explicit TimeTravelMetadataTable(std::shared_ptr<Table> source_table);

  /// \brief Implement a scan for the requested snapshot selection.
  virtual Result<ArrowArrayStream> ScanSnapshot(
      const SnapshotSelection& snapshot_selection) = 0;
};

}  // namespace iceberg
