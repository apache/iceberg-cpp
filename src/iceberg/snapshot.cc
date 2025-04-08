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

#include <format>

#include "iceberg/util/formatter.h"

namespace iceberg {

namespace {
/// \brief Get the relative Operation name
constexpr std::string_view ToString(Summary::Operation operation) {
  switch (operation) {
    case Summary::Operation::kAppend:
      return "append";
    case Summary::Operation::kOverwrite:
      return "overwrite";
    case Summary::Operation::kReplace:
      return "replace";
    case Summary::Operation::kDelete:
      return "delete";
    default:
      return "invalid";
  }
}
}  // namespace

Summary::Summary(Operation op, std::unordered_map<std::string, std::string> props)
    : operation_(op), additional_properties_(std::move(props)) {}

Summary::Operation Summary::operation() const { return operation_; }

const std::unordered_map<std::string, std::string>& Summary::properties() const {
  return additional_properties_;
}

std::string Summary::ToString() const {
  std::string repr = std::format("summary<operation: {}", iceberg::ToString(operation_));
  for (const auto& [key, value] : additional_properties_) {
    std::format_to(std::back_inserter(repr), ", {}: {}", key, value);
  }
  repr += ">";
  return repr;
}

Snapshot::Snapshot(int64_t snapshot_id, std::optional<int64_t> parent_snapshot_id,
                   int64_t sequence_number, int64_t timestamp_ms,
                   std::string manifest_list, Summary summary,
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

const Summary& Snapshot::summary() const { return summary_; }

std::optional<int32_t> Snapshot::schema_id() const { return schema_id_; }

std::string Snapshot::ToString() const {
  std::string repr;
  std::format_to(std::back_inserter(repr), "snapshot<\n  id: {}\n", snapshot_id_);
  if (parent_snapshot_id_.has_value()) {
    std::format_to(std::back_inserter(repr), "  parent_id: {}\n",
                   parent_snapshot_id_.value());
  }
  std::format_to(std::back_inserter(repr), "  sequence_number: {}\n", sequence_number_);
  std::format_to(std::back_inserter(repr), "  timestamp_ms: {}\n", timestamp_ms_);
  std::format_to(std::back_inserter(repr), "  manifest_list: {}\n", manifest_list_);
  std::format_to(std::back_inserter(repr), "  summary: {}\n", summary_);

  if (schema_id_.has_value()) {
    std::format_to(std::back_inserter(repr), "  schema_id: {}\n", schema_id_.value());
  }

  repr += ">";
  return repr;
}

bool Snapshot::Equals(const Snapshot& other) const {
  if (this == &other) {
    return true;
  }
  return snapshot_id_ == other.snapshot_id_ &&
         parent_snapshot_id_ == other.parent_snapshot_id_ &&
         sequence_number_ == other.sequence_number_ &&
         timestamp_ms_ == other.timestamp_ms_ && schema_id_ == other.schema_id_;
}

}  // namespace iceberg
