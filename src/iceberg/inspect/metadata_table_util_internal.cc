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

#include "iceberg/inspect/metadata_table_util_internal.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "iceberg/constants.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util.h"

namespace iceberg::internal {

namespace {

Result<std::shared_ptr<Snapshot>> SnapshotAtRef(const TableMetadata& metadata,
                                                std::string_view ref_name) {
  if (ref_name.empty() || ref_name == SnapshotRef::kMainBranch) {
    if (metadata.current_snapshot_id == kInvalidSnapshotId) {
      return std::shared_ptr<Snapshot>{nullptr};
    }
    return metadata.SnapshotById(metadata.current_snapshot_id);
  }

  auto ref = metadata.refs.find(std::string(ref_name));
  ICEBERG_PRECHECK(ref != metadata.refs.end(), "Cannot find snapshot reference '{}'",
                   ref_name);
  ICEBERG_PRECHECK(ref->second != nullptr, "Snapshot reference '{}' is null", ref_name);
  return metadata.SnapshotById(ref->second->snapshot_id);
}

}  // namespace

namespace {
class SnapshotMetadataScan : public MetadataScan {
 public:
  SnapshotMetadataScan(MetadataScanContext context,
                       SnapshotMetadataTask::ReadRowsFunction read_rows)
      : MetadataScan(context),
        task_(std::make_shared<SnapshotMetadataTask>(std::move(context), read_rows)) {}

  Result<MetadataScanTaskStreamPtr> PlanTasksStream() const override {
    return MetadataScanTaskStream::FromVector({task_});
  }

 private:
  const std::shared_ptr<MetadataScanTask> task_;
};
}  // namespace

Result<std::unique_ptr<MetadataScan>> MakeSnapshotMetadataScan(
    MetadataScanContext context, SnapshotMetadataTask::ReadRowsFunction read_rows) {
  ICEBERG_PRECHECK(read_rows != nullptr, "Snapshot row reader cannot be null");
  ICEBERG_PRECHECK(context.table_metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(context.schema != nullptr, "Metadata table schema cannot be null");
  ICEBERG_PRECHECK(context.projected_schema != nullptr,
                   "Projected schema cannot be null");
  ICEBERG_PRECHECK(context.io != nullptr, "FileIO cannot be null");
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
  return std::make_unique<SnapshotMetadataScan>(std::move(context), read_rows);
}

Result<std::shared_ptr<Snapshot>> ResolveMetadataTableSnapshot(
    const TableMetadata& metadata, const MetadataScanContext& selection) {
  if (!selection.ref_name.empty() && selection.ref_name != SnapshotRef::kMainBranch) {
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
  return SnapshotAtRef(metadata, selection.ref_name);
}

}  // namespace iceberg::internal
