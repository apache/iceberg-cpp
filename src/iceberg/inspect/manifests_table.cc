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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_table_stream_internal.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
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
};

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

Status AppendPartitionSummaries(ArrowArray* array, const ManifestRow& row,
                                const std::shared_ptr<StructType>& partition_type) {
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
      ICEBERG_ASSIGN_OR_RAISE(
          auto lower,
          HumanReadableBound(*row.spec, *partition_type, index, *summary.lower_bound));
      ICEBERG_RETURN_UNEXPECTED(AppendString(entries->children[2], lower));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(entries->children[2]));
    }
    if (summary.upper_bound.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto upper,
          HumanReadableBound(*row.spec, *partition_type, index, *summary.upper_bound));
      ICEBERG_RETURN_UNEXPECTED(AppendString(entries->children[3], upper));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(entries->children[3]));
    }
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(entries));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status AppendManifest(ArrowRowBuilder& builder, const ManifestRow& row,
                      const std::shared_ptr<Schema>& table_schema) {
  const auto& manifest = row.manifest;
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(0), static_cast<int64_t>(manifest.content)));
  ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(1), manifest.manifest_path));
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(2), manifest.manifest_length));
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(3), manifest.partition_spec_id));
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(4), manifest.added_snapshot_id));

  const bool data = manifest.content == ManifestContent::kData;
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(5), data ? manifest.added_files_count.value_or(0) : 0));
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(6), data ? manifest.existing_files_count.value_or(0) : 0));
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(7), data ? manifest.deleted_files_count.value_or(0) : 0));
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(8), data ? 0 : manifest.added_files_count.value_or(0)));
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(9), data ? 0 : manifest.existing_files_count.value_or(0)));
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(10), data ? 0 : manifest.deleted_files_count.value_or(0)));

  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, row.spec->PartitionType(*table_schema));
  ICEBERG_RETURN_UNEXPECTED(AppendPartitionSummaries(
      builder.column(11), row, std::shared_ptr<StructType>(std::move(partition_type))));
  return builder.FinishRow();
}

}  // namespace

ManifestsTable::ManifestsTable(std::shared_ptr<Table> table)
    : TimeTravelMetadataTable(std::move(table)) {}

ManifestsTable::~ManifestsTable() = default;

const std::shared_ptr<Schema>& ManifestsTable::schema() const {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(14, "content", int32()),
      SchemaField::MakeRequired(1, "path", string()),
      SchemaField::MakeRequired(2, "length", int64()),
      SchemaField::MakeRequired(3, "partition_spec_id", int32()),
      SchemaField::MakeRequired(4, "added_snapshot_id", int64()),
      SchemaField::MakeRequired(5, "added_data_files_count", int32()),
      SchemaField::MakeRequired(6, "existing_data_files_count", int32()),
      SchemaField::MakeRequired(7, "deleted_data_files_count", int32()),
      SchemaField::MakeRequired(15, "added_delete_files_count", int32()),
      SchemaField::MakeRequired(16, "existing_delete_files_count", int32()),
      SchemaField::MakeRequired(17, "deleted_delete_files_count", int32()),
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
    std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<ManifestsTable>(new ManifestsTable(std::move(table)));
}

Result<ArrowArrayStream> ManifestsTable::ScanSnapshot(
    const SnapshotSelection& snapshot_selection) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, internal::ResolveMetadataTableSnapshot(
                                             *source_table(), snapshot_selection));
  std::vector<ManifestRow> rows;
  if (snapshot != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(auto specs_ref, source_table()->specs());
    SnapshotCache snapshot_cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests,
                            snapshot_cache.Manifests(source_table()->io()));
    rows.reserve(manifests.size());
    for (const auto& manifest : manifests) {
      auto spec = specs_ref.get().find(manifest.partition_spec_id);
      ICEBERG_CHECK(spec != specs_ref.get().end(),
                    "Cannot find partition spec {} for manifest '{}'",
                    manifest.partition_spec_id, manifest.manifest_path);
      ICEBERG_PRECHECK(spec->second != nullptr, "Partition spec {} is null",
                       manifest.partition_spec_id);
      rows.push_back(ManifestRow{.manifest = manifest, .spec = spec->second});
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto table_schema, source_table()->schema());
  return internal::MakeMetadataTableStream(
      *schema(), std::move(rows),
      [table_schema = std::move(table_schema)](ArrowRowBuilder& builder,
                                               const ManifestRow& row) {
        return AppendManifest(builder, row, table_schema);
      });
}

}  // namespace iceberg
