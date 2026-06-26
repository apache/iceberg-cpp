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
/// This API accumulates file deletions, produces a new Snapshot of the table,
/// and commits that snapshot as current. When committing, these changes are
/// applied to the latest table snapshot. Commit conflicts are resolved by
/// applying the changes to the new latest snapshot and reattempting the commit.
///
/// File paths are matched exactly against table metadata values; equivalent but
/// differently-normalized URIs are not considered matches.
class ICEBERG_EXPORT DeleteFiles : public MergingSnapshotUpdate {
 public:
  static Result<std::unique_ptr<DeleteFiles>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Delete a file by path from the underlying table.
  ///
  /// \param path A path to remove from the table.
  /// \return This DeleteFiles for method chaining.
  DeleteFiles& DeleteFile(std::string_view path);

  /// \brief Delete a file tracked by a DataFile from the underlying table.
  ///
  /// \param file A DataFile to remove from the table.
  /// \return This DeleteFiles for method chaining.
  DeleteFiles& DeleteFile(const std::shared_ptr<DataFile>& file);

  /// \brief Delete files that match an expression on data rows from the table.
  ///
  /// A file is selected to be deleted by the expression if it could contain any
  /// rows that match the expression. Candidate files are selected using an
  /// inclusive partition projection. These candidate files are deleted if all of
  /// the rows in the file must match the expression, determined by the
  /// expression's strict partition projection. This guarantees that files are
  /// deleted if and only if all rows in the file must match the expression.
  ///
  /// Files that may contain some rows that match the expression and some rows
  /// that do not will result in a validation error.
  ///
  /// \param expr An expression on rows in the table.
  /// \return This DeleteFiles for method chaining.
  DeleteFiles& DeleteFromRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Enable or disable case-sensitive expression binding for validations.
  ///
  /// \param case_sensitive Whether expression binding should be case sensitive.
  /// \return This DeleteFiles for method chaining.
  DeleteFiles& CaseSensitive(bool case_sensitive);

  /// \brief Enable validation that deleted files still exist.
  ///
  /// If this method is called, any files that are part of the deletion must
  /// still exist when committing the operation.
  ///
  /// \return This DeleteFiles for method chaining.
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
