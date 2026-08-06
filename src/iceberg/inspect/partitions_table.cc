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

#include "iceberg/inspect/partitions_table.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/inspect/metadata_table_stream_internal.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

constexpr int32_t kPartitionFieldId = 1;
constexpr int32_t kRecordCountFieldId = 2;
constexpr int32_t kFileCountFieldId = 3;
constexpr int32_t kSpecIdFieldId = 4;
constexpr int32_t kPositionDeleteRecordCountFieldId = 5;
constexpr int32_t kPositionDeleteFileCountFieldId = 6;
constexpr int32_t kEqualityDeleteRecordCountFieldId = 7;
constexpr int32_t kEqualityDeleteFileCountFieldId = 8;
constexpr int32_t kLastUpdatedAtFieldId = 9;
constexpr int32_t kLastUpdatedSnapshotIdFieldId = 10;
constexpr int32_t kTotalDataFileSizeFieldId = 11;

struct PartitionKey {
  PartitionValues values;
  size_t projected_fields;

  bool operator==(const PartitionKey& other) const {
    if (projected_fields != other.projected_fields ||
        values.num_fields() != other.values.num_fields()) {
      return false;
    }
    for (size_t index = 0; index < values.num_fields(); ++index) {
      const auto& lhs = values.values()[index];
      const auto& rhs = other.values.values()[index];
      if (lhs.IsNull() || rhs.IsNull()) {
        if (lhs.IsNull() != rhs.IsNull()) {
          return false;
        }
      } else if (lhs != rhs) {
        return false;
      }
    }
    return true;
  }
};

struct PartitionKeyHash {
  size_t operator()(const PartitionKey& key) const noexcept {
    size_t result = 17;
    for (const auto& value : key.values.values()) {
      size_t value_hash;
      if (value.IsNaN()) {
        const bool negative = std::holds_alternative<float>(value.value())
                                  ? std::signbit(std::get<float>(value.value()))
                                  : std::signbit(std::get<double>(value.value()));
        value_hash = negative ? 0x9e3779b97f4a7c15ULL : 0x7ff8000000000000ULL;
      } else {
        value_hash = LiteralHash{}(value);
      }
      result = result * 37 + value_hash;
    }
    return result * 37 + key.projected_fields;
  }
};

size_t ProjectedFieldCount(const StructType& partition_type, const PartitionSpec& spec) {
  size_t count = 0;
  for (const auto& field : partition_type.fields()) {
    count += std::ranges::any_of(
        spec.fields(), [field_id = field.field_id()](const PartitionField& spec_field) {
          return spec_field.field_id() == field_id;
        });
  }
  return count;
}

struct PartitionStats {
  explicit PartitionStats(PartitionValues values) : partition(std::move(values)) {}

  PartitionValues partition;
  int32_t spec_id = PartitionSpec::kInitialSpecId;
  int64_t data_record_count = 0;
  int32_t data_file_count = 0;
  int64_t data_file_size = 0;
  int64_t position_delete_record_count = 0;
  int32_t position_delete_file_count = 0;
  int64_t equality_delete_record_count = 0;
  int32_t equality_delete_file_count = 0;
  std::optional<TimePointMs> last_updated_at;
  std::optional<int64_t> last_updated_snapshot_id;
};

Status AppendPartition(ArrowRowBuilder& builder, const Schema& schema,
                       const StructType& partition_type,
                       const PartitionStats& partition) {
  for (size_t index = 0; index < schema.fields().size(); ++index) {
    auto* array = builder.column(index);
    switch (schema.fields()[index].field_id()) {
      case kPartitionFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            internal::AppendPartitionValues(array, partition_type, partition.partition));
        break;
      case kSpecIdFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, partition.spec_id));
        break;
      case kRecordCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, partition.data_record_count));
        break;
      case kFileCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, partition.data_file_count));
        break;
      case kTotalDataFileSizeFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, partition.data_file_size));
        break;
      case kPositionDeleteRecordCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(array, partition.position_delete_record_count));
        break;
      case kPositionDeleteFileCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, partition.position_delete_file_count));
        break;
      case kEqualityDeleteRecordCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(
            AppendInt(array, partition.equality_delete_record_count));
        break;
      case kEqualityDeleteFileCountFieldId:
        ICEBERG_RETURN_UNEXPECTED(AppendInt(array, partition.equality_delete_file_count));
        break;
      case kLastUpdatedAtFieldId:
        if (partition.last_updated_at.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendInt(array, std::chrono::duration_cast<std::chrono::microseconds>(
                                   partition.last_updated_at->time_since_epoch())
                                   .count()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      case kLastUpdatedSnapshotIdFieldId:
        if (partition.last_updated_snapshot_id.has_value()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendInt(array, *partition.last_updated_snapshot_id));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendNull(array));
        }
        break;
      default:
        return InvalidSchema("Unsupported partitions metadata field {}",
                             schema.fields()[index].field_id());
    }
  }
  return builder.FinishRow();
}

void UpdateCounts(PartitionStats& partition, const DataFile& file) {
  switch (file.content) {
    case DataFile::Content::kData:
      partition.data_record_count += file.record_count;
      ++partition.data_file_count;
      partition.data_file_size += file.file_size_in_bytes;
      break;
    case DataFile::Content::kPositionDeletes:
      partition.position_delete_record_count += file.record_count;
      ++partition.position_delete_file_count;
      break;
    case DataFile::Content::kEqualityDeletes:
      partition.equality_delete_record_count += file.record_count;
      ++partition.equality_delete_file_count;
      break;
  }
}

}  // namespace

PartitionsTable::PartitionsTable(std::shared_ptr<Table> table,
                                 std::shared_ptr<Schema> schema,
                                 std::shared_ptr<StructType> partition_type)
    : TimeTravelMetadataTable(std::move(table)),
      schema_(std::move(schema)),
      partition_type_(std::move(partition_type)) {}

PartitionsTable::~PartitionsTable() = default;

const std::shared_ptr<Schema>& PartitionsTable::schema() const { return schema_; }

Result<std::unique_ptr<PartitionsTable>> PartitionsTable::Make(
    std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, internal::UnifiedPartitionType(*table));

  std::vector<SchemaField> fields;
  if (!partition_type->fields().empty()) {
    fields.push_back(
        SchemaField::MakeRequired(kPartitionFieldId, "partition", partition_type));
    fields.push_back(SchemaField::MakeRequired(kSpecIdFieldId, "spec_id", int32()));
  }
  fields.push_back(
      SchemaField::MakeRequired(kRecordCountFieldId, "record_count", int64()));
  fields.push_back(SchemaField::MakeRequired(kFileCountFieldId, "file_count", int32()));
  fields.push_back(SchemaField::MakeRequired(kTotalDataFileSizeFieldId,
                                             "total_data_file_size_in_bytes", int64()));
  fields.push_back(SchemaField::MakeRequired(kPositionDeleteRecordCountFieldId,
                                             "position_delete_record_count", int64()));
  fields.push_back(SchemaField::MakeRequired(kPositionDeleteFileCountFieldId,
                                             "position_delete_file_count", int32()));
  fields.push_back(SchemaField::MakeRequired(kEqualityDeleteRecordCountFieldId,
                                             "equality_delete_record_count", int64()));
  fields.push_back(SchemaField::MakeRequired(kEqualityDeleteFileCountFieldId,
                                             "equality_delete_file_count", int32()));
  fields.push_back(SchemaField::MakeOptional(kLastUpdatedAtFieldId, "last_updated_at",
                                             timestamp_tz()));
  fields.push_back(SchemaField::MakeOptional(kLastUpdatedSnapshotIdFieldId,
                                             "last_updated_snapshot_id", int64()));

  auto schema = std::make_shared<Schema>(std::move(fields));
  return std::unique_ptr<PartitionsTable>(new PartitionsTable(
      std::move(table), std::move(schema), std::move(partition_type)));
}

Result<ArrowArrayStream> PartitionsTable::ScanSnapshot(
    const SnapshotSelection& snapshot_selection) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, internal::ResolveMetadataTableSnapshot(
                                             *source_table(), snapshot_selection));
  ICEBERG_ASSIGN_OR_RAISE(auto files, internal::LoadLiveFiles(*source_table(), snapshot));

  std::vector<PartitionStats> partitions;
  std::unordered_map<PartitionKey, size_t, PartitionKeyHash> positions;
  std::unordered_map<int64_t, std::shared_ptr<Snapshot>> snapshots;
  for (const auto& live_file : files) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_values, internal::ProjectPartitionValues(
                                                       *partition_type_, *live_file.spec,
                                                       live_file.file->partition));
    PartitionKey key{
        .values = std::move(partition_values),
        .projected_fields = ProjectedFieldCount(*partition_type_, *live_file.spec)};
    auto [position, inserted] = positions.try_emplace(key, partitions.size());
    if (inserted) {
      partitions.emplace_back(std::move(key.values));
    }
    auto& partition = partitions[position->second];
    UpdateCounts(partition, *live_file.file);

    if (live_file.snapshot_id.has_value()) {
      auto snapshot_iter = snapshots.find(*live_file.snapshot_id);
      if (snapshot_iter == snapshots.end()) {
        auto file_snapshot = source_table()->SnapshotById(*live_file.snapshot_id);
        if (!file_snapshot.has_value() &&
            file_snapshot.error().kind != ErrorKind::kNotFound) {
          return std::unexpected<Error>(file_snapshot.error());
        }
        snapshot_iter =
            snapshots.emplace(*live_file.snapshot_id, file_snapshot.value_or(nullptr))
                .first;
      }
      const auto& file_snapshot = snapshot_iter->second;
      if (file_snapshot != nullptr &&
          (!partition.last_updated_at.has_value() ||
           file_snapshot->timestamp_ms > *partition.last_updated_at)) {
        partition.spec_id = live_file.spec->spec_id();
        partition.last_updated_at = file_snapshot->timestamp_ms;
        partition.last_updated_snapshot_id = file_snapshot->snapshot_id;
      }
    }
  }

  auto schema = schema_;
  auto partition_type = partition_type_;
  return internal::MakeMetadataTableStream(
      *schema_, std::move(partitions),
      [schema = std::move(schema), partition_type = std::move(partition_type)](
          ArrowRowBuilder& builder, const PartitionStats& partition) {
        return AppendPartition(builder, *schema, *partition_type, partition);
      });
}

}  // namespace iceberg
