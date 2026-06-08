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

#include "iceberg/update/delete_files.h"

#include <memory>
#include <string>
#include <string_view>

#include "iceberg/snapshot.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<DeleteFiles>> DeleteFiles::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create DeleteFiles without a context");
  return std::unique_ptr<DeleteFiles>(
      new DeleteFiles(std::move(table_name), std::move(ctx)));
}

DeleteFiles::DeleteFiles(std::string table_name, std::shared_ptr<TransactionContext> ctx)
    : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {}

DeleteFiles& DeleteFiles::DeleteFile(std::string_view path) {
  ICEBERG_BUILDER_CHECK(!path.empty(), "Cannot delete an empty file path");
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteByPath(path));
  return *this;
}

DeleteFiles& DeleteFiles::DeleteFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDataFile(file));
  return *this;
}

DeleteFiles& DeleteFiles::DeleteFromRowFilter(std::shared_ptr<Expression> expr) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteByRowFilter(std::move(expr)));
  return *this;
}

DeleteFiles& DeleteFiles::CaseSensitive(bool case_sensitive) {
  MergingSnapshotUpdate::CaseSensitive(case_sensitive);
  return *this;
}

DeleteFiles& DeleteFiles::ValidateFilesExist() {
  validate_files_to_delete_exist_ = true;
  return *this;
}

std::string DeleteFiles::operation() { return DataOperation::kDelete; }

Status DeleteFiles::Validate(const TableMetadata&, const std::shared_ptr<Snapshot>&) {
  if (validate_files_to_delete_exist_) {
    FailMissingDeletePaths();
  }
  return {};
}

}  // namespace iceberg
