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

#include "iceberg/inspect/manifests_table.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/inspect/metadata_scan_util_internal.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/conversions.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

struct ManifestRow {
  ManifestFile manifest;
  std::shared_ptr<PartitionSpec> spec;
  std::shared_ptr<StructType> partition_type;
};

Status AppendPartitionBound(ArrowArray* array, const PartitionField& field,
                            const std::shared_ptr<Type>& type,
                            const std::optional<std::vector<uint8_t>>& bound) {
  if (!bound.has_value() || type->type_id() == TypeId::kUnknown) {
    return AppendString(array, "null");
  }
  auto primitive = internal::checked_pointer_cast<PrimitiveType>(type);
  ICEBERG_ASSIGN_OR_RAISE(auto literal,
                          Conversions::FromBytes(std::move(primitive), *bound));
  ICEBERG_ASSIGN_OR_RAISE(auto text, field.transform()->ToHumanString(literal));
  return AppendString(array, text);
}

Status AppendPartitionSummaries(ArrowArray* array, const ManifestRow& row) {
  const auto spec_fields = row.spec->fields();
  const auto& result_fields = row.partition_type->fields();
  ICEBERG_PRECHECK(row.manifest.partitions.size() <= spec_fields.size() &&
                       row.manifest.partitions.size() <= result_fields.size(),
                   "Manifest '{}' has more partition summaries than spec {} fields",
                   row.manifest.manifest_path, row.spec->spec_id());
  auto* entries = array->children[0];
  ICEBERG_PRECHECK(entries != nullptr && entries->n_children == 4,
                   "Partition summaries must contain four fields");

  for (size_t index = 0; index < row.manifest.partitions.size(); ++index) {
    const auto& summary = row.manifest.partitions[index];
    ICEBERG_RETURN_UNEXPECTED(AppendBoolean(entries->children[0], summary.contains_null));
    if (summary.contains_nan.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(
          AppendBoolean(entries->children[1], *summary.contains_nan));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(entries->children[1]));
    }

    const auto& field = spec_fields[index];
    const auto& type = result_fields[index].type();
    if (type->type_id() == TypeId::kUnknown &&
        (summary.lower_bound.has_value() || summary.upper_bound.has_value())) {
      ICEBERG_LOG_WARN(
          "Cannot render partition bounds with unknown type in manifest '{}' "
          "(spec {}, field '{}' ({}), source column {})",
          row.manifest.manifest_path, row.spec->spec_id(), field.name(), field.field_id(),
          field.source_id());
    }
    ICEBERG_RETURN_UNEXPECTED(
        AppendPartitionBound(entries->children[2], field, type, summary.lower_bound));
    ICEBERG_RETURN_UNEXPECTED(
        AppendPartitionBound(entries->children[3], field, type, summary.upper_bound));
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(entries));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

template <std::optional<int32_t> ManifestFile::* CountField, ManifestContent Content>
Status AppendManifestCount(ArrowArray* array, const ManifestRow& row) {
  const auto& manifest = row.manifest;
  if (manifest.content == Content) {
    const auto& count = manifest.*CountField;
    return count.has_value() ? AppendInt(array, *count) : AppendNull(array);
  } else {
    return AppendInt(array, 0);
  }
}

constexpr std::array<internal::MetadataTableColumn<ManifestRow>, 12> kManifestColumns = {{
    {.field_id = 14,
     .append =
         [](ArrowArray* array, const ManifestRow& row) {
           return AppendInt(array, static_cast<int64_t>(row.manifest.content));
         }},
    {.field_id = 1,
     .append =
         [](ArrowArray* array, const ManifestRow& row) {
           return AppendString(array, row.manifest.manifest_path);
         }},
    {.field_id = 2,
     .append =
         [](ArrowArray* array, const ManifestRow& row) {
           return AppendInt(array, row.manifest.manifest_length);
         }},
    {.field_id = 3,
     .append =
         [](ArrowArray* array, const ManifestRow& row) {
           return AppendInt(array, row.manifest.partition_spec_id);
         }},
    {.field_id = 4,
     .append = [](ArrowArray* array, const ManifestRow& row) -> Status {
       ICEBERG_PRECHECK(row.manifest.added_snapshot_id.has_value(),
                        "Manifest '{}' has no added snapshot ID",
                        row.manifest.manifest_path);
       return AppendInt(array, *row.manifest.added_snapshot_id);
     }},
    {.field_id = 5,
     .append =
         AppendManifestCount<&ManifestFile::added_files_count, ManifestContent::kData>},
    {.field_id = 6,
     .append = AppendManifestCount<&ManifestFile::existing_files_count,
                                   ManifestContent::kData>},
    {.field_id = 7,
     .append =
         AppendManifestCount<&ManifestFile::deleted_files_count, ManifestContent::kData>},
    {.field_id = 15,
     .append = AppendManifestCount<&ManifestFile::added_files_count,
                                   ManifestContent::kDeletes>},
    {.field_id = 16,
     .append = AppendManifestCount<&ManifestFile::existing_files_count,
                                   ManifestContent::kDeletes>},
    {.field_id = 17,
     .append = AppendManifestCount<&ManifestFile::deleted_files_count,
                                   ManifestContent::kDeletes>},
    {.field_id = 8, .append = AppendPartitionSummaries},
}};

}  // namespace

ManifestsTable::ManifestsTable(std::shared_ptr<const Table> table)
    : MetadataTable(std::move(table)) {}

ManifestsTable::~ManifestsTable() = default;

std::shared_ptr<Schema> ManifestsTable::schema() const {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(14, "content", int32()),
      SchemaField::MakeRequired(1, "path", string()),
      SchemaField::MakeRequired(2, "length", int64()),
      SchemaField::MakeRequired(3, "partition_spec_id", int32()),
      SchemaField::MakeRequired(4, "added_snapshot_id", int64()),
      // Legacy manifests can omit counts; unknown counts must stay null.
      SchemaField::MakeOptional(5, "added_data_files_count", int32()),
      SchemaField::MakeOptional(6, "existing_data_files_count", int32()),
      SchemaField::MakeOptional(7, "deleted_data_files_count", int32()),
      SchemaField::MakeOptional(15, "added_delete_files_count", int32()),
      SchemaField::MakeOptional(16, "existing_delete_files_count", int32()),
      SchemaField::MakeOptional(17, "deleted_delete_files_count", int32()),
      SchemaField::MakeRequired(
          8, "partition_summaries",
          list(SchemaField::MakeRequired(
              9, std::string(ListType::kElementName),
              struct_({SchemaField::MakeRequired(10, "contains_null", boolean()),
                       SchemaField::MakeOptional(11, "contains_nan", boolean()),
                       SchemaField::MakeOptional(12, "lower_bound", string()),
                       SchemaField::MakeOptional(13, "upper_bound", string())})))),
  });
  return schema;
}

Result<std::unique_ptr<ManifestsTable>> ManifestsTable::Make(
    std::shared_ptr<const Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<ManifestsTable>(new ManifestsTable(std::move(table)));
}

Result<std::unique_ptr<MetadataScan>> ManifestsTable::MakeScan(
    internal::MetadataScanContext context, std::shared_ptr<FileIO> io) const {
  return internal::MakeSnapshotMetadataScan(std::move(context), std::move(io), ReadRows);
}

Result<ArrowArrayStream> ManifestsTable::ReadRows(
    const internal::MetadataScanContext& context, const std::shared_ptr<FileIO>& io,
    int64_t batch_size) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, internal::ResolveMetadataTableSnapshot(
                                             *context.table_metadata, context));
  std::vector<ManifestRow> rows;
  std::unordered_map<int32_t, std::shared_ptr<StructType>> partition_types;
  if (snapshot != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(auto table_schema, context.table_metadata->Schema());
    SnapshotReader snapshot_reader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_reader.Manifests(io));
    rows.reserve(manifests.size());
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, context.table_metadata->PartitionSpecById(
                                             manifest.partition_spec_id));
      auto [entry, inserted] = partition_types.try_emplace(spec->spec_id());
      if (inserted) {
        ICEBERG_ASSIGN_OR_RAISE(entry->second, spec->PartitionType(*table_schema));
      }
      rows.push_back(ManifestRow{.manifest = manifest,
                                 .spec = std::move(spec),
                                 .partition_type = entry->second});
    }
  }

  const size_t num_rows = rows.size();
  return internal::MakeMetadataBatchStream<ManifestRow>(
      context.schema, context.projected_schema, kManifestColumns, num_rows, batch_size,
      [rows = std::move(rows)](size_t index) -> const ManifestRow& {
        return rows[index];
      });
}

}  // namespace iceberg
