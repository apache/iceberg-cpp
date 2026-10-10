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

/// \file iceberg/data_table_scan.h
/// \brief Scan that reads the data files of a table snapshot.

#include <memory>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A scan that reads data files and applies delete files to filter rows.
class ICEBERG_EXPORT DataTableScan : public TableScan {
 public:
  ~DataTableScan() override = default;

  /// \brief Constructs a DataTableScan instance.
  static Result<std::unique_ptr<DataTableScan>> Make(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileIO> io, internal::TableScanContext context);

  /// \brief Plans the scan tasks by resolving manifests and data files.
  ///
  /// Collects PlanFilesStream() into a vector.
  /// \return A Result containing scan tasks or an error.
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles() const;

  /// \brief Lazily plans scan tasks by resolving manifests and data files on demand.
  ///
  /// The returned fallible, single-pass stream owns its planning resources and
  /// can outlive this scan. An executor configured through PlanWith() is borrowed and
  /// must remain alive until the stream is destroyed, as later Next() calls may submit
  /// work to it.
  Result<FileScanTaskStreamPtr> PlanFilesStream() const;

 protected:
  using TableScan::TableScan;
};

extern template class ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT
    TableScanBuilder<DataTableScan>;

}  // namespace iceberg
