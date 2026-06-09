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

#include "iceberg/catalog/rest/types.h"

#include <algorithm>
#include <optional>

#include "iceberg/expression/expression.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_scan.h"
#include "iceberg/table_update.h"

namespace iceberg::rest {

std::string_view ToString(PlanStatus status) {
  switch (status) {
    case PlanStatus::kSubmitted:
      return "submitted";
    case PlanStatus::kCompleted:
      return "completed";
    case PlanStatus::kCancelled:
      return "cancelled";
    case PlanStatus::kFailed:
      return "failed";
  }
  return "unknown";
}

Result<PlanStatus> PlanStatusFromString(std::string_view status_str) {
  if (status_str == "submitted") return PlanStatus::kSubmitted;
  if (status_str == "completed") return PlanStatus::kCompleted;
  if (status_str == "cancelled") return PlanStatus::kCancelled;
  if (status_str == "failed") return PlanStatus::kFailed;
  return JsonParseError("Unknown plan status: {}", status_str);
}

bool CreateTableRequest::operator==(const CreateTableRequest& other) const {
  if (name != other.name || location != other.location ||
      stage_create != other.stage_create || properties != other.properties) {
    return false;
  }

  if (!schema != !other.schema) {
    return false;
  }
  if (schema && *schema != *other.schema) {
    return false;
  }

  if (!partition_spec != !other.partition_spec) {
    return false;
  }
  if (partition_spec && *partition_spec != *other.partition_spec) {
    return false;
  }

  if (!write_order != !other.write_order) {
    return false;
  }
  if (write_order && *write_order != *other.write_order) {
    return false;
  }
  return true;
}

bool LoadTableResult::operator==(const LoadTableResult& other) const {
  if (metadata_location != other.metadata_location || config != other.config) {
    return false;
  }

  if (!metadata != !other.metadata) {
    return false;
  }
  if (metadata && *metadata != *other.metadata) {
    return false;
  }
  return true;
}

bool CommitTableRequest::operator==(const CommitTableRequest& other) const {
  if (identifier != other.identifier) {
    return false;
  }
  if (requirements.size() != other.requirements.size()) {
    return false;
  }
  if (updates.size() != other.updates.size()) {
    return false;
  }

  for (size_t i = 0; i < requirements.size(); ++i) {
    if (!requirements[i] != !other.requirements[i]) {
      return false;
    }
    if (requirements[i] && !requirements[i]->Equals(*other.requirements[i])) {
      return false;
    }
  }

  for (size_t i = 0; i < updates.size(); ++i) {
    if (!updates[i] != !other.updates[i]) {
      return false;
    }
    if (updates[i] && !updates[i]->Equals(*other.updates[i])) {
      return false;
    }
  }

  return true;
}

bool CommitTableResponse::operator==(const CommitTableResponse& other) const {
  if (metadata_location != other.metadata_location) {
    return false;
  }
  if (!metadata != !other.metadata) {
    return false;
  }
  if (metadata && *metadata != *other.metadata) {
    return false;
  }
  return true;
}

namespace {

bool ExpressionPtrEqual(const std::shared_ptr<Expression>& lhs,
                        const std::shared_ptr<Expression>& rhs) {
  if (lhs == rhs) {
    return true;
  }
  return lhs && rhs && lhs->Equals(*rhs);
}

}  // namespace

bool PlanTableScanRequest::operator==(const PlanTableScanRequest& other) const {
  return snapshot_id == other.snapshot_id && select == other.select &&
         ExpressionPtrEqual(filter, other.filter) &&
         case_sensitive == other.case_sensitive &&
         use_snapshot_schema == other.use_snapshot_schema &&
         start_snapshot_id == other.start_snapshot_id &&
         end_snapshot_id == other.end_snapshot_id && stats_fields == other.stats_fields &&
         min_rows_requested == other.min_rows_requested;
}

namespace {

template <typename T>
bool SharedPtrEqual(const std::shared_ptr<T>& lhs, const std::shared_ptr<T>& rhs) {
  if (lhs == rhs) {
    return true;
  }
  return lhs && rhs && *lhs == *rhs;
}

template <typename T>
bool SharedPtrVectorEqual(const std::vector<std::shared_ptr<T>>& lhs,
                          const std::vector<std::shared_ptr<T>>& rhs) {
  return std::ranges::equal(lhs, rhs, SharedPtrEqual<T>);
}

bool FileScanTaskEqual(const std::shared_ptr<FileScanTask>& lhs,
                       const std::shared_ptr<FileScanTask>& rhs) {
  if (lhs == rhs) {
    return true;
  }
  if (!lhs || !rhs) {
    return false;
  }
  return SharedPtrEqual(lhs->data_file(), rhs->data_file()) &&
         SharedPtrVectorEqual(lhs->delete_files(), rhs->delete_files()) &&
         ExpressionPtrEqual(lhs->residual_filter(), rhs->residual_filter());
}

template <typename T>
bool OptionalSharedPtrVectorEqual(
    const std::optional<std::vector<std::shared_ptr<T>>>& lhs,
    const std::optional<std::vector<std::shared_ptr<T>>>& rhs,
    bool (*eq)(const std::shared_ptr<T>&, const std::shared_ptr<T>&)) {
  if (lhs.has_value() != rhs.has_value()) {
    return false;
  }
  return !lhs.has_value() || std::ranges::equal(*lhs, *rhs, eq);
}

template <typename Response>
bool ScanTaskFieldsEqual(const Response& lhs, const Response& rhs) {
  return lhs.plan_tasks == rhs.plan_tasks &&
         SharedPtrVectorEqual(lhs.delete_files, rhs.delete_files) &&
         OptionalSharedPtrVectorEqual(lhs.file_scan_tasks, rhs.file_scan_tasks,
                                      FileScanTaskEqual);
}

template <typename Response>
bool HasTaskFields(const Response& response) {
  return response.plan_tasks.has_value() || response.file_scan_tasks.has_value();
}

template <typename Response>
bool HasNonEmptyFileScanTasks(const Response& response) {
  return response.file_scan_tasks.has_value() && !response.file_scan_tasks->empty();
}

}  // namespace

bool PlanTableScanResponse::operator==(const PlanTableScanResponse& other) const {
  return ScanTaskFieldsEqual(*this, other) && plan_status == other.plan_status &&
         plan_id == other.plan_id && error == other.error;
}

bool FetchPlanningResultResponse::operator==(
    const FetchPlanningResultResponse& other) const {
  return ScanTaskFieldsEqual(*this, other) && plan_status == other.plan_status &&
         error == other.error;
}

bool FetchScanTasksRequest::operator==(const FetchScanTasksRequest& other) const {
  return planTask == other.planTask;
}

bool FetchScanTasksResponse::operator==(const FetchScanTasksResponse& other) const {
  return ScanTaskFieldsEqual(*this, other);
}

Status OAuthTokenResponse::Validate() const {
  if (access_token.empty()) {
    return ValidationFailed("OAuth2 token response missing required 'access_token'");
  }
  if (token_type.empty()) {
    return ValidationFailed("OAuth2 token response missing required 'token_type'");
  }
  // token_type must be "bearer" or "N_A" (case-insensitive).
  std::string lower_type = token_type;
  std::ranges::transform(lower_type, lower_type.begin(), ::tolower);
  if (lower_type != "bearer" && lower_type != "n_a") {
    return ValidationFailed(R"(Unsupported token type: {} (must be "bearer" or "N_A"))",
                            token_type);
  }
  return {};
}

Status PlanTableScanRequest::Validate() const {
  if (snapshot_id.has_value()) {
    if (start_snapshot_id.has_value() || end_snapshot_id.has_value()) {
      return ValidationFailed(
          "Invalid scan: cannot provide both snapshotId and "
          "startSnapshotId/endSnapshotId");
    }
  }
  if (start_snapshot_id.has_value() || end_snapshot_id.has_value()) {
    if (!start_snapshot_id.has_value() || !end_snapshot_id.has_value()) {
      return ValidationFailed(
          "Invalid incremental scan: startSnapshotId and endSnapshotId is required");
    }
  }
  if (min_rows_requested.has_value() && min_rows_requested.value() < 0) {
    return ValidationFailed("Invalid scan: minRowsRequested is negative");
  }
  return {};
}

Status PlanTableScanResponse::Validate() const {
  if (plan_status == PlanStatus::kSubmitted && plan_id.empty()) {
    return ValidationFailed(
        "Invalid response: plan id should be defined when status is 'submitted'");
  }
  if (plan_status == PlanStatus::kCancelled) {
    return ValidationFailed(
        "Invalid response: 'cancelled' is not a valid status for planTableScan");
  }
  if (plan_status != PlanStatus::kCompleted && HasTaskFields(*this)) {
    return ValidationFailed(
        "Invalid response: tasks can only be defined when status is 'completed'");
  }
  if (!plan_id.empty() && plan_status != PlanStatus::kSubmitted &&
      plan_status != PlanStatus::kCompleted) {
    return ValidationFailed(
        "Invalid response: plan id can only be defined when status is 'submitted' or "
        "'completed'");
  }
  if (!HasNonEmptyFileScanTasks(*this) && !delete_files.empty()) {
    return ValidationFailed(
        "Invalid response: deleteFiles should only be returned with fileScanTasks that "
        "reference them");
  }
  if (plan_status == PlanStatus::kFailed && !error.has_value()) {
    return ValidationFailed(
        "Invalid response: error must be present when status is 'failed'");
  }
  if (plan_status != PlanStatus::kFailed && error.has_value()) {
    return ValidationFailed(
        "Invalid response: error can only be present when status is 'failed'");
  }
  return {};
}

Status FetchPlanningResultResponse::Validate() const {
  if (plan_status != PlanStatus::kCompleted && HasTaskFields(*this)) {
    return ValidationFailed(
        "Invalid response: tasks can only be returned in a 'completed' status");
  }
  if (!HasNonEmptyFileScanTasks(*this) && !delete_files.empty()) {
    return ValidationFailed(
        "Invalid response: deleteFiles should only be returned with fileScanTasks that "
        "reference them");
  }
  if (plan_status == PlanStatus::kFailed && !error.has_value()) {
    return ValidationFailed(
        "Invalid response: error must be present when status is 'failed'");
  }
  if (plan_status != PlanStatus::kFailed && error.has_value()) {
    return ValidationFailed(
        "Invalid response: error can only be present when status is 'failed'");
  }
  return {};
}

Status FetchScanTasksRequest::Validate() const {
  if (planTask.empty()) {
    return ValidationFailed("Invalid planTask: null");
  }
  return {};
}

Status FetchScanTasksResponse::Validate() const {
  if (!HasNonEmptyFileScanTasks(*this) && !delete_files.empty()) {
    return ValidationFailed(
        "Invalid response: deleteFiles should only be returned with fileScanTasks that "
        "reference them");
  }
  if (!HasTaskFields(*this)) {
    return ValidationFailed(
        "Invalid response: planTasks and fileScanTask cannot both be null");
  }
  return {};
}

}  // namespace iceberg::rest
