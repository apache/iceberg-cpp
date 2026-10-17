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

#include "iceberg/inspect/snapshots_table.h"

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_table_stream_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

Status AppendSnapshot(ArrowRowBuilder& builder, const Snapshot& snapshot) {
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(0), std::chrono::duration_cast<std::chrono::microseconds>(
                                       snapshot.timestamp_ms.time_since_epoch())
                                       .count()));
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(1), snapshot.snapshot_id));

  if (snapshot.parent_snapshot_id.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(2), *snapshot.parent_snapshot_id));
  } else {
    ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(2)));
  }

  auto operation = snapshot.Operation();
  if (operation.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(3), *operation));
  } else {
    ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(3)));
  }

  ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(4), snapshot.manifest_list));

  if (!snapshot.summary.empty()) {
    if (snapshot.summary.contains(SnapshotSummaryFields::kOperation)) {
      auto summary = snapshot.summary;
      summary.erase(SnapshotSummaryFields::kOperation);
      ICEBERG_RETURN_UNEXPECTED(AppendStringMap(builder.column(5), summary));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendStringMap(builder.column(5), snapshot.summary));
    }
  } else {
    ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(5)));
  }

  return builder.FinishRow();
}

class SnapshotsIterator : public Stream<std::shared_ptr<Snapshot>> {
 public:
  explicit SnapshotsIterator(std::shared_ptr<const TableMetadata> metadata)
      : metadata_(std::move(metadata)) {}

 protected:
  Result<std::optional<std::shared_ptr<Snapshot>>> NextImpl() override {
    const auto& snapshots = metadata_->snapshots;
    while (next_snapshot_ < snapshots.size()) {
      const auto& snapshot = snapshots[next_snapshot_++];
      if (snapshot != nullptr) {
        return snapshot;
      }
    }
    return std::nullopt;
  }

 private:
  std::shared_ptr<const TableMetadata> metadata_;
  size_t next_snapshot_ = 0;
};

}  // namespace

SnapshotsTable::SnapshotsTable(std::shared_ptr<Table> table)
    : MetadataTable(std::move(table)) {}

SnapshotsTable::~SnapshotsTable() = default;

const std::shared_ptr<Schema>& SnapshotsTable::schema() const {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(6, "summary",
                                std::make_shared<iceberg::MapType>(
                                    SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
  return schema;
}

Result<std::unique_ptr<SnapshotsTable>> SnapshotsTable::Make(
    std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<SnapshotsTable>(new SnapshotsTable(std::move(table)));
}

Result<ArrowArrayStream> SnapshotsTable::Scan() {
  std::unique_ptr<Stream<std::shared_ptr<Snapshot>>> rows =
      std::make_unique<SnapshotsIterator>(source_table()->metadata());
  return internal::MakeMetadataTableStream(
      *schema(), std::move(rows),
      [](ArrowRowBuilder& builder, const std::shared_ptr<Snapshot>& snapshot) {
        return AppendSnapshot(builder, *snapshot);
      });
}

}  // namespace iceberg
