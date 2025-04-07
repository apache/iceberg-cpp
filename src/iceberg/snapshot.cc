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

#include "iceberg/snapshot.h"

#include "iceberg/util/formatter.h"

namespace iceberg {

namespace {
/// \brief Get the relative Operation name
constexpr std::string_view ToString(Operation operation) {
  switch (operation) {
    case Operation::kAppend:
      return "append";
    case Operation::kOverwrite:
      return "overwrite";
    case Operation::kReplace:
      return "replace";
    case Operation::kDelete:
      return "delete";
    default:
      return "invalid";
  }
}
}  // namespace

Summary::Summary(Operation op, std::unordered_map<std::string, std::string> props)
    : operation_(op), additional_properties_(std::move(props)) {}

Operation Summary::operation() const { return operation_; }

const std::unordered_map<std::string, std::string>& Summary::properties() const {
  return additional_properties_;
}

std::string Summary::ToString() const {
  std::string repr =
      "summary: { operation: " + std::string(iceberg::ToString(operation_));
  for (const auto& [key, value] : additional_properties_) {
    repr += ", " + key + ": " + value;
  }
  repr += "}";
  return repr;
}

bool Summary::Equals(const Summary& other) const {
  return operation_ == other.operation_ &&
         additional_properties_ == other.additional_properties_;
}

Snapshot::Snapshot(int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
                   int64_t sequence_number, int64_t timestamp_ms,
                   std::string manifest_list, std::shared_ptr<Summary> summary,
                   std::optional<int64_t> schema_id)
    : snapshot_id_(snapshot_id),
      parent_snapshot_id_(parent_snapshot_id),
      sequence_number_(sequence_number),
      timestamp_ms_(timestamp_ms),
      manifest_list_(std::move(manifest_list)),
      summary_(std::move(summary)),
      schema_id_(schema_id) {}

int64_t Snapshot::snapshot_id() const { return snapshot_id_; }

std::optional<int64_t> Snapshot::parent_snapshot_id() const {
  return parent_snapshot_id_;
}

int64_t Snapshot::sequence_number() const { return sequence_number_; }

int64_t Snapshot::timestamp_ms() const { return timestamp_ms_; }

const std::string& Snapshot::manifest_list() const { return manifest_list_; }

const std::shared_ptr<Summary>& Snapshot::summary() const { return summary_; }

std::optional<int32_t> Snapshot::schema_id() const { return schema_id_; }

std::string Snapshot::ToString() const {
  std::string repr = "snapshot: { id: " + std::to_string(snapshot_id_);
  if (parent_snapshot_id_.has_value()) {
    repr += ", parent_id: " + std::to_string(parent_snapshot_id_.value());
  }
  repr += ", sequence_number: " + std::to_string(sequence_number_);
  repr += ", timestamp_ms: " + std::to_string(timestamp_ms_);
  repr += ", manifest_list: " + manifest_list_;
  repr += ", summary: " + summary_->ToString();

  if (schema_id_.has_value()) {
    repr += ", schema_id: " + std::to_string(schema_id_.value());
  }

  repr += " }";

  return repr;
}

bool Snapshot::Equals(const Snapshot& other) const {
  return snapshot_id_ == other.snapshot_id_ &&
         parent_snapshot_id_ == other.parent_snapshot_id_ &&
         sequence_number_ == other.sequence_number_ &&
         timestamp_ms_ == other.timestamp_ms_ && manifest_list_ == other.manifest_list_ &&
         *summary_ == *other.summary_ && schema_id_ == other.schema_id_;
}

}  // namespace iceberg
