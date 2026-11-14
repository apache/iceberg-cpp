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

#include <algorithm>
#include <cstdint>
#include <format>
#include <functional>
#include <map>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/constants.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
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

Status AppendLiteral(ArrowArray* array, const Literal& literal) {
  if (literal.IsNull()) {
    return AppendNull(array);
  }
  if (literal.IsAboveMax() || literal.IsBelowMin()) {
    return InvalidArgument("Cannot append non-value partition literal {}",
                           literal.ToString());
  }

  switch (literal.type()->type_id()) {
    case TypeId::kBoolean:
      return AppendBoolean(array, std::get<bool>(literal.value()));
    case TypeId::kInt:
    case TypeId::kDate:
      return AppendInt(array, std::get<int32_t>(literal.value()));
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs:
      return AppendInt(array, std::get<int64_t>(literal.value()));
    case TypeId::kFloat:
      return AppendDouble(array, std::get<float>(literal.value()));
    case TypeId::kDouble:
      return AppendDouble(array, std::get<double>(literal.value()));
    case TypeId::kString:
      return AppendString(array, std::get<std::string>(literal.value()));
    case TypeId::kBinary:
    case TypeId::kFixed:
      return AppendBytes(array, std::get<std::vector<uint8_t>>(literal.value()));
    case TypeId::kDecimal:
      return AppendDecimal(array, std::get<Decimal>(literal.value()));
    case TypeId::kUuid:
      return AppendBytes(array, std::get<Uuid>(literal.value()).bytes());
    case TypeId::kUnknown:
    case TypeId::kStruct:
    case TypeId::kList:
    case TypeId::kMap:
    case TypeId::kVariant:
    case TypeId::kGeometry:
    case TypeId::kGeography:
      return NotSupported("Cannot append partition literal of type {}",
                          literal.type()->ToString());
  }
  std::unreachable();
}

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

Result<std::shared_ptr<StructType>> UnifiedPartitionType(const TableMetadata& metadata) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  auto specs = metadata.partition_specs;
  for (const auto& spec : specs) {
    ICEBERG_PRECHECK(spec != nullptr, "Partition spec cannot be null");
  }
  std::ranges::sort(specs, std::greater{}, &PartitionSpec::spec_id);

  std::unordered_set<int32_t> active_field_ids;
  for (const auto& spec : specs) {
    for (const auto& field : spec->fields()) {
      ICEBERG_PRECHECK(field.transform() != nullptr,
                       "Partition field {} has a null transform", field.field_id());
      ICEBERG_CHECK(field.transform()->transform_type() != TransformType::kUnknown,
                    "Cannot build table partition type with unknown transform '{}'",
                    field.transform()->ToString());
      ICEBERG_ASSIGN_OR_RAISE(auto source_field,
                              schema->FindFieldById(field.source_id()));
      if (source_field.has_value()) {
        active_field_ids.insert(field.field_id());
      }
    }
  }

  struct ProjectedField {
    const PartitionField* definition;
    std::string name;
    std::shared_ptr<Type> type;
  };
  std::map<int32_t, ProjectedField> fields_by_id;
  for (const auto& spec : specs) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_type, spec->PartitionType(*schema));
    ICEBERG_PRECHECK(spec_type->fields().size() == spec->fields().size(),
                     "Partition spec {} has mismatched field and type counts",
                     spec->spec_id());
    for (size_t index = 0; index < spec->fields().size(); ++index) {
      const auto& partition_field = spec->fields()[index];
      if (!active_field_ids.contains(partition_field.field_id())) {
        continue;
      }

      const auto& spec_field = spec_type->fields()[index];
      auto [iter, inserted] =
          fields_by_id.try_emplace(partition_field.field_id(),
                                   ProjectedField{.definition = &partition_field,
                                                  .name = std::string(spec_field.name()),
                                                  .type = spec_field.type()});
      if (!inserted) {
        const auto& existing = *iter->second.definition;
        const auto current_transform = partition_field.transform()->transform_type();
        const auto existing_transform = existing.transform()->transform_type();
        const bool compatible_transform =
            *partition_field.transform() == *existing.transform() ||
            current_transform == TransformType::kVoid ||
            existing_transform == TransformType::kVoid;
        ICEBERG_CHECK(
            partition_field.source_id() == existing.source_id() && compatible_transform,
            "Conflicting partition fields with ID {}: '{}' and '{}'",
            partition_field.field_id(), partition_field.ToString(), existing.ToString());

        if (existing_transform == TransformType::kVoid &&
            current_transform != TransformType::kVoid) {
          iter->second.definition = &partition_field;
          iter->second.type = spec_field.type();
        }
      }
    }
  }

  std::vector<SchemaField> fields;
  fields.reserve(fields_by_id.size());
  for (auto& [_, field] : fields_by_id) {
    fields.emplace_back(field.definition->field_id(), std::move(field.name),
                        std::move(field.type), /*optional=*/true);
  }
  return std::make_shared<StructType>(std::move(fields));
}

Result<PartitionValues> ProjectPartitionValues(const StructType& partition_type,
                                               const PartitionSpec& spec,
                                               const PartitionValues& values) {
  ICEBERG_PRECHECK(values.num_fields() == spec.fields().size(),
                   "Partition has {} values but spec {} has {} fields",
                   values.num_fields(), spec.spec_id(), spec.fields().size());

  std::unordered_map<int32_t, size_t> positions;
  positions.reserve(spec.fields().size());
  for (size_t index = 0; index < spec.fields().size(); ++index) {
    positions.emplace(spec.fields()[index].field_id(), index);
  }

  std::vector<Literal> projected;
  projected.reserve(partition_type.fields().size());
  for (const auto& field : partition_type.fields()) {
    auto target_type = checked_pointer_cast<PrimitiveType>(field.type());
    auto position = positions.find(field.field_id());
    if (position == positions.end()) {
      projected.push_back(Literal::Null(std::move(target_type)));
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto value, values.ValueAt(position->second));
    if (value.get().IsNull()) {
      projected.push_back(Literal::Null(std::move(target_type)));
    } else if (value.get().type()->type_id() == target_type->type_id()) {
      projected.push_back(value.get());
    } else {
      ICEBERG_ASSIGN_OR_RAISE(auto coerced, value.get().CastTo(target_type));
      projected.push_back(std::move(coerced));
    }
  }
  return PartitionValues(std::move(projected));
}

Status AppendPartitionValues(ArrowArray* array, const StructType& partition_type,
                             const PartitionValues& values) {
  ICEBERG_PRECHECK(array != nullptr, "Partition Arrow array cannot be null");
  ICEBERG_PRECHECK(
      array->n_children == static_cast<int64_t>(partition_type.fields().size()),
      "Partition Arrow array has {} fields but schema has {}", array->n_children,
      partition_type.fields().size());
  ICEBERG_PRECHECK(values.num_fields() == partition_type.fields().size(),
                   "Partition has {} values but schema has {} fields",
                   values.num_fields(), partition_type.fields().size());

  for (size_t index = 0; index < values.num_fields(); ++index) {
    ICEBERG_ASSIGN_OR_RAISE(auto value, values.ValueAt(index));
    ICEBERG_RETURN_UNEXPECTED(AppendLiteral(array->children[index], value.get()));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

namespace {

class LiveFilesIterator : public Stream<LiveFile> {
 public:
  LiveFilesIterator(std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
                    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs,
                    std::vector<ManifestFile> manifests, std::vector<std::string> columns)
      : io_(std::move(io)),
        schema_(std::move(schema)),
        specs_(std::move(specs)),
        manifests_(std::move(manifests)),
        columns_(std::move(columns)) {}

 protected:
  Result<std::optional<LiveFile>> NextImpl() override {
    while (entry_index_ == entries_.size()) {
      entries_.clear();
      entry_index_ = 0;
      if (manifest_index_ == manifests_.size()) {
        return std::nullopt;
      }
      const auto& manifest = manifests_[manifest_index_++];
      auto spec = specs_.find(manifest.partition_spec_id);
      ICEBERG_CHECK(spec != specs_.end(),
                    "Cannot find partition spec {} for manifest '{}'",
                    manifest.partition_spec_id, manifest.manifest_path);
      ICEBERG_PRECHECK(spec->second != nullptr, "Partition spec {} is null",
                       manifest.partition_spec_id);
      current_spec_ = spec->second;
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(manifest, io_, schema_, specs_));
      if (!columns_.empty()) {
        reader->Select(columns_);
      }
      ICEBERG_ASSIGN_OR_RAISE(entries_, reader->LiveEntries());
    }
    auto& entry = entries_[entry_index_++];
    ICEBERG_PRECHECK(entry.data_file != nullptr,
                     "Manifest contains an entry with no data file");
    return LiveFile{.file = std::move(entry.data_file),
                    .spec = current_spec_,
                    .snapshot_id = entry.snapshot_id};
  }

 private:
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Schema> schema_;
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_;
  std::vector<ManifestFile> manifests_;
  std::vector<std::string> columns_;
  std::vector<ManifestEntry> entries_;
  std::shared_ptr<PartitionSpec> current_spec_;
  size_t manifest_index_ = 0;
  size_t entry_index_ = 0;
};

}  // namespace

Result<std::unique_ptr<Stream<LiveFile>>> LiveFiles(
    const TableMetadata& metadata, const std::shared_ptr<FileIO>& io,
    const std::shared_ptr<Snapshot>& snapshot, std::vector<std::string> columns) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs;
  for (const auto& spec : metadata.partition_specs) {
    ICEBERG_PRECHECK(spec != nullptr, "Partition spec cannot be null");
    specs.emplace(spec->spec_id(), spec);
  }
  std::vector<ManifestFile> manifests;
  if (snapshot != nullptr) {
    SnapshotReader snapshot_reader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_manifests, snapshot_reader.Manifests(io));
    manifests.assign(snapshot_manifests.begin(), snapshot_manifests.end());
  }
  return std::make_unique<LiveFilesIterator>(io, std::move(schema), std::move(specs),
                                             std::move(manifests), std::move(columns));
}

}  // namespace iceberg::internal
