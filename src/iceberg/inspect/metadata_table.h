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
/// \brief Metadata table factories, schemas, and scan entry points.

#include <concepts>
#include <cstdint>
#include <memory>
#include <utility>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {
namespace internal {
struct MetadataScanContext;
}  // namespace internal

/// \brief Iceberg metadata tables exposed by Table::Inspect().
enum class MetadataTableKind : uint8_t {
  kEntries,
  kFiles,
  kDataFiles,
  kDeleteFiles,
  kHistory,
  kMetadataLogEntries,
  kSnapshots,
  kRefs,
  kManifests,
  kPartitions,
  kPositionDeletes,
  kAllEntries,
  kAllFiles,
  kAllDataFiles,
  kAllDeleteFiles,
  kAllManifests,
};

/// \brief A metadata table backed by a source Iceberg table.
class ICEBERG_EXPORT MetadataTable {
 public:
  using Kind = MetadataTableKind;

  /// \brief Create a metadata table; unsupported kinds return NotImplemented.
  static Result<std::unique_ptr<MetadataTable>> Make(std::shared_ptr<const Table> table,
                                                     Kind kind);

  /// \brief Create a specific metadata table from its concrete class.
  template <typename MetadataTableType>
    requires std::derived_from<MetadataTableType, MetadataTable>
  static Result<std::unique_ptr<MetadataTableType>> Make(
      std::shared_ptr<const Table> table) {
    return MetadataTableType::Make(std::move(table));
  }

  virtual ~MetadataTable();

  /// \brief Return this metadata table's kind.
  virtual Kind kind() const noexcept = 0;

  /// \brief Return the complete canonical schema of this metadata table.
  virtual const std::shared_ptr<Schema>& schema() const = 0;

  /// \brief Return the source table whose metadata is exposed.
  const std::shared_ptr<const Table>& source_table() const;

  /// \brief Capture the source metadata and create a scan builder.
  ///
  /// This metadata table must outlive the builder. Built scans retain their own
  /// inputs and can outlive both tables.
  virtual Result<std::unique_ptr<MetadataScanBuilder>> NewScan() const;

 protected:
  explicit MetadataTable(std::shared_ptr<const Table> source_table);

  /// \brief Plan a scan using the captured metadata, projection, and snapshot selection.
  virtual Result<std::unique_ptr<MetadataScan>> MakeScan(
      internal::MetadataScanContext context) const = 0;

 private:
  friend class MetadataScanBuilder;

  const std::shared_ptr<const Table> source_table_;
};

}  // namespace iceberg
