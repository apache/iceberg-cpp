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

/// \file iceberg/inspect/metadata_scan.h
/// \brief Scan configuration for Iceberg metadata tables.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

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

  /// \brief Return whether this selects the current snapshot on the main branch.
  bool IsCurrentSnapshot() const {
    return std::holds_alternative<std::monostate>(snapshot) && ref_name.empty();
  }
};

/// \brief A configured scan of a metadata table.
///
/// Scans are created by MetadataScanBuilder::Build(). The metadata table that created
/// the builder must outlive the scan.
class ICEBERG_EXPORT MetadataScan {
 public:
  ~MetadataScan();

  /// \brief Return the schema of the rows this scan emits.
  ///
  /// This is schema() of the metadata table projected to the selected columns.
  const std::shared_ptr<Schema>& schema() const;

  /// \brief Return the snapshot this scan reads.
  const SnapshotSelection& snapshot_selection() const;

  /// \brief Read the scan as an Arrow stream.
  ///
  /// Each call returns a new stream. The caller owns the returned stream and must
  /// invoke its `release` callback when the stream is no longer needed.
  ///
  /// \return An Arrow stream containing the selected columns, or an error.
  Result<ArrowArrayStream> ToArrowArrayStream() const;

 private:
  friend class MetadataScanBuilder;

  MetadataScan(MetadataTable& table, std::shared_ptr<Schema> schema,
               SnapshotSelection snapshot_selection);

  MetadataTable* table_;
  std::shared_ptr<Schema> schema_;
  SnapshotSelection snapshot_selection_;
};

/// \brief Builder for metadata table scans.
///
/// Builders are created by MetadataTable::NewScan(). Options are validated when the
/// scan is built, and the metadata table must outlive the builder.
///
/// \code
///   ICEBERG_ASSIGN_OR_RAISE(auto builder, snapshots_table->NewScan());
///   ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Select({"snapshot_id"}).Build());
///   ICEBERG_ASSIGN_OR_RAISE(auto stream, scan->ToArrowArrayStream());
/// \endcode
class ICEBERG_EXPORT MetadataScanBuilder : public ErrorCollector {
 public:
  ~MetadataScanBuilder() override;

  /// \brief Request this scan to return the given columns.
  ///
  /// Nested names are dot-concatenated. Columns are returned in schema order, and
  /// names that match no column are ignored. An empty list selects no columns, and a
  /// list containing `*` selects all columns. If this method is not called, all
  /// columns are selected. Selecting a map's value returns the whole map; selecting
  /// only its key is an error.
  ///
  /// \param column_names Column names from the metadata table's schema.
  MetadataScanBuilder& Select(const std::vector<std::string>& column_names);

  /// \brief Control whether Select() matches column names case-sensitively.
  ///
  /// Default is true.
  ///
  /// \param case_sensitive Whether column names are matched case-sensitively.
  MetadataScanBuilder& CaseSensitive(bool case_sensitive);

  /// \brief Request this scan to use the given snapshot by ID.
  ///
  /// Only metadata tables that support time travel accept a snapshot; Build()
  /// returns NotSupported otherwise. The table resolves the snapshot when the scan
  /// is read.
  ///
  /// \param snapshot_id A snapshot ID.
  MetadataScanBuilder& UseSnapshot(int64_t snapshot_id);

  /// \brief Request this scan to resolve the snapshot on the given branch or tag.
  ///
  /// Only metadata tables that support time travel accept a reference; Build()
  /// returns NotSupported otherwise. The table resolves the reference when the scan
  /// is read.
  ///
  /// \param ref A branch or tag name.
  MetadataScanBuilder& UseRef(const std::string& ref);

  /// \brief Request this scan to use the most recent snapshot as of the given time.
  ///
  /// The snapshot is resolved on the branch set by UseRef(), or main if no branch
  /// is set. Only metadata tables that support time travel accept a timestamp;
  /// Build() returns NotSupported otherwise.
  ///
  /// \param timestamp_millis A timestamp in milliseconds since the Unix epoch.
  MetadataScanBuilder& AsOfTime(int64_t timestamp_millis);

  /// \brief Validate the options and build the scan.
  ///
  /// \return The scan, or an error if an option is invalid, the selected columns
  /// cannot be projected, or the table does not support the requested time travel.
  Result<std::unique_ptr<MetadataScan>> Build();

 private:
  friend class MetadataTable;

  explicit MetadataScanBuilder(MetadataTable& table);

  MetadataTable* table_;
  std::optional<std::vector<std::string>> selected_columns_;
  bool case_sensitive_ = true;
  SnapshotSelection snapshot_selection_;
};

}  // namespace iceberg
