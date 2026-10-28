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

#include <memory>
#include <string>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

namespace iceberg {

/// \brief Configure a scan over a metadata table.
///
/// Builders are created by MetadataTable::NewScan(), and the metadata table must
/// outlive the builder.
///
/// \code
///   ICEBERG_ASSIGN_OR_RAISE(auto builder, snapshots_table->NewScan());
///   ICEBERG_ASSIGN_OR_RAISE(auto stream, builder->Select({"snapshot_id"}).Build());
/// \endcode
class ICEBERG_EXPORT MetadataScanBuilder : public ErrorCollector {
 public:
  ~MetadataScanBuilder() override;

  /// \brief Select columns by name; nested columns use dot-separated paths.
  ///
  /// Columns are returned in schema order, and names that match no column are
  /// ignored. An empty list selects no columns; `*` selects all columns. Names are
  /// resolved immediately, using the current CaseSensitive() setting. Selecting a
  /// map's value returns the whole map; selecting only its key is an error.
  ///
  /// \param columns Column names from the metadata table's schema.
  MetadataScanBuilder& Select(const std::vector<std::string>& columns);

  /// \brief Set the output schema to a projection of the metadata table schema.
  ///
  /// Each field must select a whole column of the metadata table by field id, with
  /// the same type and without tightening its nullability. Fields may be reordered.
  ///
  /// \param schema The output schema.
  MetadataScanBuilder& Project(std::shared_ptr<Schema> schema);

  /// \brief Use case-sensitive column names in Select(); enabled by default.
  ///
  /// \param value Whether column names are matched case-sensitively.
  MetadataScanBuilder& CaseSensitive(bool value);

  /// \brief Validate the settings and scan the metadata table.
  ///
  /// Each call returns a new stream with the output schema. The caller owns the
  /// returned stream and must invoke its `release` callback when the stream is no
  /// longer needed.
  ///
  /// \return An Arrow stream containing the selected columns, or an error.
  Result<ArrowArrayStream> Build();

 private:
  friend class MetadataTable;

  explicit MetadataScanBuilder(const MetadataTable& table);

  const MetadataTable* table_;
  std::shared_ptr<Schema> projected_schema_;
  bool case_sensitive_ = true;
};

}  // namespace iceberg
