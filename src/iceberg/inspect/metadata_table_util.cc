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

#include <memory>
#include <string>
#include <utility>

#include "iceberg/constants.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util.h"

namespace iceberg::internal {

namespace {
/// A scan that plans one task for a metadata table's selected snapshot.
///
/// The task retains the captured metadata, schemas, and FileIO so it can outlive the
/// scan. Table-specific row reading is deferred to the supplied callback when
/// MetadataScanTaskReader opens the task.
class SnapshotMetadataScan : public MetadataScan {
 public:
  SnapshotMetadataScan(MetadataScanContext context,
                       std::shared_ptr<SnapshotMetadataTask> task)
      : MetadataScan(std::move(context)), task_(std::move(task)) {}

  Result<MetadataScanTaskStreamPtr> PlanTasksStream() const override {
    // Each planning call gets a fresh stream over the same retained task.
    return MetadataScanTaskStream::FromVector({task_});
  }

 private:
  const std::shared_ptr<MetadataScanTask> task_;
};
}  // namespace

Result<std::unique_ptr<MetadataScan>> MakeSnapshotMetadataScan(
    MetadataScanContext context, std::shared_ptr<FileIO> io,
    SnapshotMetadataTask::ReadRowsFunction read_rows) {
  ICEBERG_ASSIGN_OR_RAISE(auto task,
                          SnapshotMetadataTask::Make(context, std::move(io), read_rows));
  for (const auto& projected : context.projected_schema->fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto field,
                            context.schema->GetFieldById(projected.field_id()));
    ICEBERG_PRECHECK(field.has_value(), "Unknown metadata column: {}", projected.name());
    if (*field->get().type() != *projected.type()) {
      return NotSupported("Partial projection of metadata column is not supported: {}",
                          projected.name());
    }
    ICEBERG_PRECHECK(!field->get().optional() || projected.optional(),
                     "Nullable metadata column cannot be projected as required: {}",
                     projected.name());
  }
  ICEBERG_RETURN_UNEXPECTED(
      ResolveMetadataTableSnapshot(*context.table_metadata, context));
  return std::make_unique<SnapshotMetadataScan>(std::move(context), std::move(task));
}

Result<std::shared_ptr<Snapshot>> ResolveMetadataTableSnapshot(
    const TableMetadata& metadata, const MetadataScanContext& selection) {
  const bool is_main_ref = SnapshotUtil::IsMainBranch(selection.ref_name);
  if (!is_main_ref) {
    ICEBERG_PRECHECK(
        !selection.snapshot_id.has_value() && !selection.as_of_time_in_millis.has_value(),
        "Cannot combine a snapshot reference with a snapshot ID or timestamp");
  }
  if (selection.snapshot_id.has_value()) {
    return metadata.SnapshotById(*selection.snapshot_id);
  }
  if (selection.as_of_time_in_millis.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto snapshot_id,
        SnapshotUtil::SnapshotIdAsOfTime(
            metadata, TimePointMsFromUnixMs(*selection.as_of_time_in_millis)));
    return metadata.SnapshotById(snapshot_id);
  }
  if (is_main_ref && metadata.current_snapshot_id == kInvalidSnapshotId) {
    return std::shared_ptr<Snapshot>{nullptr};
  }
  if (!is_main_ref) {
    auto ref = metadata.refs.find(selection.ref_name);
    ICEBERG_PRECHECK(ref != metadata.refs.end(), "Cannot find snapshot reference '{}'",
                     selection.ref_name);
    ICEBERG_PRECHECK(ref->second != nullptr, "Snapshot reference '{}' is null",
                     selection.ref_name);
  }
  return SnapshotUtil::LatestSnapshot(metadata, selection.ref_name);
}

}  // namespace iceberg::internal
