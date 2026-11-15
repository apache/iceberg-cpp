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
#include "iceberg/inspect/metadata_scan_util_internal.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/nanoarrow_status_internal.h"
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

Result<std::shared_ptr<StructType>> ManifestPartitionType(const TableMetadata& metadata,
                                                          const PartitionSpec& spec) {
  ICEBERG_ASSIGN_OR_RAISE(auto current_schema, metadata.Schema());
  std::vector<SchemaField> fields;
  fields.reserve(spec.fields().size());
  for (const auto& partition_field : spec.fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto source,
                            current_schema->FindFieldById(partition_field.source_id()));
    // A retained spec can reference a dropped column. Recover its most recent
    // known type by field ID, including any promotion before the column was dropped.
    if (!source) {
      int32_t latest_schema_id = -1;
      for (const auto& schema : metadata.schemas) {
        ICEBERG_PRECHECK(schema != nullptr, "Table schema cannot be null");
        if (schema->schema_id() > latest_schema_id) {
          ICEBERG_ASSIGN_OR_RAISE(auto historical,
                                  schema->FindFieldById(partition_field.source_id()));
          if (historical) {
            source = historical;
            latest_schema_id = schema->schema_id();
          }
        }
      }
    }
    auto source_type = source ? source->get().type() : unknown();
    fields.push_back(SchemaField::MakeOptional(
        partition_field.field_id(), std::string(partition_field.name()),
        partition_field.transform()->ResultType(source_type)));
  }
  return std::make_shared<StructType>(std::move(fields));
}

Result<std::string> HumanReadableBound(const PartitionSpec& spec,
                                       const StructType& partition_type, size_t index,
                                       const std::vector<uint8_t>& bytes) {
  ICEBERG_PRECHECK(index < spec.fields().size() && index < partition_type.fields().size(),
                   "Partition summary index {} is out of range", index);
  auto primitive = internal::checked_pointer_cast<PrimitiveType>(
      partition_type.fields()[index].type());
  ICEBERG_ASSIGN_OR_RAISE(auto literal,
                          Conversions::FromBytes(std::move(primitive), bytes));
  return spec.fields()[index].transform()->ToHumanString(literal);
}

Status AppendPartitionSummaries(ArrowArray* array, const ManifestRow& row) {
  ICEBERG_PRECHECK(row.manifest.partitions.size() <= row.spec->fields().size(),
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

    if (summary.lower_bound.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower,
                              HumanReadableBound(*row.spec, *row.partition_type, index,
                                                 *summary.lower_bound));
      ICEBERG_RETURN_UNEXPECTED(AppendString(entries->children[2], lower));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendString(entries->children[2], "null"));
    }
    if (summary.upper_bound.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper,
                              HumanReadableBound(*row.spec, *row.partition_type, index,
                                                 *summary.upper_bound));
      ICEBERG_RETURN_UNEXPECTED(AppendString(entries->children[3], upper));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendString(entries->children[3], "null"));
    }
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
    SnapshotReader snapshot_reader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_reader.Manifests(io));
    rows.reserve(manifests.size());
    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, context.table_metadata->PartitionSpecById(
                                             manifest.partition_spec_id));
      auto [entry, inserted] = partition_types.try_emplace(spec->spec_id());
      if (inserted) {
        ICEBERG_ASSIGN_OR_RAISE(entry->second,
                                ManifestPartitionType(*context.table_metadata, *spec));
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
