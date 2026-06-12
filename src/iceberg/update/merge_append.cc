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

#include "iceberg/update/merge_append.h"

#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<MergeAppend>> MergeAppend::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create MergeAppend without a context");
  return std::unique_ptr<MergeAppend>(
      new MergeAppend(std::move(table_name), std::move(ctx)));
}

MergeAppend::MergeAppend(std::string table_name, std::shared_ptr<TransactionContext> ctx)
    : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {}

MergeAppend& MergeAppend::AppendFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(file));
  return *this;
}

MergeAppend& MergeAppend::AppendManifest(const ManifestFile& manifest) {
  ICEBERG_BUILDER_CHECK(!manifest.has_existing_files(),
                        "Cannot append manifest with existing files");
  ICEBERG_BUILDER_CHECK(!manifest.has_deleted_files(),
                        "Cannot append manifest with deleted files");
  ICEBERG_BUILDER_CHECK(manifest.added_snapshot_id == kInvalidSnapshotId,
                        "Snapshot id must be assigned during commit");
  ICEBERG_BUILDER_CHECK(manifest.sequence_number == kInvalidSequenceNumber,
                        "Sequence number must be assigned during commit");

  ICEBERG_BUILDER_RETURN_IF_ERROR(AddManifest(manifest));
  return *this;
}

std::string MergeAppend::operation() { return DataOperation::kAppend; }

}  // namespace iceberg
