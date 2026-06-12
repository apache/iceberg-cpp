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

/// \file iceberg/update/delete_files.h

#include <memory>
#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/merging_snapshot_update.h"

namespace iceberg {

/// \brief API for deleting data files from a table.
///
/// This accumulates data-file deletions, produces a new snapshot, and commits that
/// snapshot as current. File paths are matched exactly against table metadata values;
/// equivalent but differently-normalized URIs are not considered matches.
class ICEBERG_EXPORT DeleteFiles : public MergingSnapshotUpdate {
 public:
  static Result<std::unique_ptr<DeleteFiles>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Delete a data-file path from the table.
  DeleteFiles& DeleteFile(std::string_view path);

  /// \brief Delete a data file tracked by object identity and path.
  DeleteFiles& DeleteFile(const std::shared_ptr<DataFile>& file);

  /// \brief Delete files whose rows all match the given expression.
  DeleteFiles& DeleteFromRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Set case sensitivity for expression binding.
  DeleteFiles& CaseSensitive(bool case_sensitive);

  /// \brief Validate that explicitly requested deleted files still exist.
  DeleteFiles& ValidateFilesExist();

  std::string operation() override;

 protected:
  Status Validate(const TableMetadata& current_metadata,
                  const std::shared_ptr<Snapshot>& snapshot) override;

 private:
  DeleteFiles(std::string table_name, std::shared_ptr<TransactionContext> ctx);

  bool validate_files_to_delete_exist_ = false;
};

}  // namespace iceberg
