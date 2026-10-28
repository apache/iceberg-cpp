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
#include <utility>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_scan.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

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

  /// \brief Return the complete canonical schema of this metadata table.
  virtual const std::shared_ptr<Schema>& schema() const = 0;

  /// \brief Return the source table whose metadata is exposed.
  const std::shared_ptr<Table>& source_table() const;

  /// \brief Create a builder for configuring a metadata table scan.
  ///
  /// A builder with no options scans all columns. This table must outlive the builder
  /// and the scans built from it.
  virtual Result<std::unique_ptr<MetadataScanBuilder>> NewScan() const;

 protected:
  explicit MetadataTable(std::shared_ptr<Table> source_table);

  /// \brief Implement a scan that returns the columns of a projected schema.
  ///
  /// \param projected_schema Projection of schema() with the columns to return. It
  /// is only valid during this call.
  virtual Result<ArrowArrayStream> ScanProjection(
      const Schema& projected_schema) const = 0;

 private:
  friend class MetadataScan;

  std::shared_ptr<Table> source_table_;
};

}  // namespace iceberg
