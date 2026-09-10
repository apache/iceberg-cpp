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

#include "iceberg/inspect/position_deletes_table.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <format>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/deletes/dv_util_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/type_util.h"

namespace iceberg {
namespace {

using PartitionMapping = std::vector<std::optional<size_t>>;

struct PositionDeletesSchema {
  std::shared_ptr<Schema> schema;
  std::shared_ptr<StructType> partition_type;
  std::unordered_map<int32_t, PartitionMapping> partition_mappings;
};

struct UnifiedPartitionField {
  int32_t partition_field_id;
  int32_t source_id;
  std::shared_ptr<Transform> transform;
  SchemaField field;
};

TableIdentifier MakePositionDeletesTableName(const TableIdentifier& source_name) {
  return TableIdentifier{.ns = source_name.ns,
                         .name = source_name.name + ".position_deletes"};
}

Status CollectFieldIds(const Type& type, std::unordered_set<int32_t>& field_ids) {
  if (!type.is_nested()) {
    return {};
  }
  for (const auto& field : static_cast<const NestedType&>(type).fields()) {
    if (!field_ids.insert(field.field_id()).second) {
      return InvalidSchema("Duplicate field ID {} in position_deletes schema",
                           field.field_id());
    }
    ICEBERG_RETURN_UNEXPECTED(CollectFieldIds(*field.type(), field_ids));
  }
  return {};
}

Result<int32_t> TakeFreshFieldId(int64_t& candidate,
                                 std::unordered_set<int32_t>& used_ids) {
  constexpr int64_t kMaxFieldId = std::numeric_limits<int32_t>::max();
  for (int pass = 0; pass < 2; ++pass) {
    while (candidate <= kMaxFieldId) {
      const auto field_id = static_cast<int32_t>(candidate++);
      if (used_ids.insert(field_id).second) {
        return field_id;
      }
    }
    candidate = 1;
  }
  return InvalidSchema("No field ID is available for position_deletes partition fields");
}

Result<std::optional<SchemaField>> ResolvePartitionField(
    const PartitionField& partition_field, const Schema& current_schema) {
  ICEBERG_ASSIGN_OR_RAISE(auto source_field,
                          current_schema.FindFieldById(partition_field.source_id()));
  if (!source_field.has_value()) {
    return std::nullopt;
  }
  return SchemaField::MakeOptional(
      partition_field.field_id(), std::string(partition_field.name()),
      partition_field.transform()->ResultType(source_field->get().type()));
}

Result<PositionDeletesSchema> MakePositionDeletesSchema(const Table& table) {
  ICEBERG_ASSIGN_OR_RAISE(auto table_schema, table.schema());

  std::vector<std::shared_ptr<PartitionSpec>> specs = table.metadata()->partition_specs;
  std::ranges::sort(specs, {}, [](const auto& spec) {
    return spec == nullptr ? std::numeric_limits<int32_t>::min() : spec->spec_id();
  });
  std::ranges::reverse(specs);

  std::unordered_map<int32_t, UnifiedPartitionField> fields_by_partition_id;
  for (const auto& spec : specs) {
    ICEBERG_PRECHECK(spec != nullptr, "Partition spec cannot be null");
    for (const auto& partition_field : spec->fields()) {
      ICEBERG_PRECHECK(
          partition_field.transform()->transform_type() != TransformType::kUnknown,
          "Cannot build position_deletes partition type for unknown "
          "transform on field {}",
          partition_field.field_id());
      ICEBERG_ASSIGN_OR_RAISE(auto field,
                              ResolvePartitionField(partition_field, *table_schema));
      if (!field.has_value()) {
        continue;
      }
      auto state = UnifiedPartitionField{
          .partition_field_id = partition_field.field_id(),
          .source_id = partition_field.source_id(),
          .transform = partition_field.transform(),
          .field = std::move(*field),
      };
      auto [existing_it, inserted] =
          fields_by_partition_id.try_emplace(partition_field.field_id(), state);
      if (inserted) {
        continue;
      }

      auto& existing = existing_it->second;
      if (existing.source_id != partition_field.source_id()) {
        return InvalidSchema("Partition field ID {} has conflicting source IDs {} and {}",
                             partition_field.field_id(), existing.source_id,
                             partition_field.source_id());
      }
      const bool existing_void =
          existing.transform->transform_type() == TransformType::kVoid;
      const bool current_void =
          partition_field.transform()->transform_type() == TransformType::kVoid;
      if (!existing_void && !current_void &&
          *existing.transform != *partition_field.transform()) {
        return InvalidSchema(
            "Partition field ID {} has incompatible transforms {} and {}",
            partition_field.field_id(), existing.transform->ToString(),
            partition_field.transform()->ToString());
      }
      if (!existing_void && !current_void &&
          *existing.field.type() != *state.field.type()) {
        return InvalidSchema("Partition field ID {} has incompatible types {} and {}",
                             partition_field.field_id(),
                             existing.field.type()->ToString(),
                             state.field.type()->ToString());
      }
      if (existing_void && !current_void) {
        existing.transform = std::move(state.transform);
        existing.field = existing.field.WithType(state.field.type());
      }
    }
  }

  std::vector<UnifiedPartitionField> unified_partition_fields;
  unified_partition_fields.reserve(fields_by_partition_id.size());
  for (auto& [_, field] : fields_by_partition_id) {
    unified_partition_fields.push_back(std::move(field));
  }
  std::ranges::sort(unified_partition_fields, {},
                    &UnifiedPartitionField::partition_field_id);

  constexpr std::array<int32_t, 8> kMetadataFieldIds{
      MetadataColumns::kDeleteFilePathColumnId,
      MetadataColumns::kDeleteFilePosColumnId,
      MetadataColumns::kDeleteFileRowColumnId,
      MetadataColumns::kPartitionColumnId,
      MetadataColumns::kSpecIdColumnId,
      MetadataColumns::kFilePathColumnId,
      MetadataColumns::kContentOffsetColumnId,
      MetadataColumns::kContentSizeInBytesColumnId,
  };
  std::unordered_set<int32_t> used_ids(kMetadataFieldIds.begin(),
                                       kMetadataFieldIds.end());
  std::unordered_set<int32_t> current_schema_ids;
  ICEBERG_RETURN_UNEXPECTED(CollectFieldIds(*table_schema, current_schema_ids));
  for (int32_t field_id : current_schema_ids) {
    if (!used_ids.insert(field_id).second) {
      return InvalidSchema("Table field ID {} conflicts with a metadata field ID",
                           field_id);
    }
  }
  for (const auto& historical_schema : table.metadata()->schemas) {
    ICEBERG_PRECHECK(historical_schema != nullptr, "Table schema cannot be null");
    std::unordered_set<int32_t> historical_ids;
    ICEBERG_RETURN_UNEXPECTED(CollectFieldIds(*historical_schema, historical_ids));
    used_ids.insert(historical_ids.begin(), historical_ids.end());
  }

  int64_t next_field_id =
      std::max<int64_t>(1, static_cast<int64_t>(table.metadata()->last_column_id) + 1);
  std::vector<SchemaField> partition_fields;
  partition_fields.reserve(unified_partition_fields.size());
  for (const auto& field : unified_partition_fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto field_id, TakeFreshFieldId(next_field_id, used_ids));
    partition_fields.push_back(SchemaField::MakeOptional(
        field_id, field.field.name(), field.field.type(), field.field.doc()));
  }

  auto partition_type = std::make_shared<StructType>(partition_fields);
  std::unordered_map<int32_t, PartitionMapping> partition_mappings;
  for (const auto& spec : specs) {
    PartitionMapping mapping(partition_fields.size());
    for (size_t output_idx = 0; output_idx < partition_fields.size(); ++output_idx) {
      for (size_t input_idx = 0; input_idx < spec->fields().size(); ++input_idx) {
        if (spec->fields()[input_idx].field_id() ==
                unified_partition_fields[output_idx].partition_field_id &&
            spec->fields()[input_idx].source_id() ==
                unified_partition_fields[output_idx].source_id) {
          mapping[output_idx] = input_idx;
          break;
        }
      }
    }
    partition_mappings.emplace(spec->spec_id(), std::move(mapping));
  }

  std::vector<SchemaField> fields{
      MetadataColumns::kDeleteFilePath,
      MetadataColumns::kDeleteFilePos,
      SchemaField::MakeOptional(
          MetadataColumns::kDeleteFileRowColumnId,
          MetadataColumns::kDeleteFileRowFieldName,
          std::make_shared<StructType>(std::vector<SchemaField>(
              table_schema->fields().begin(), table_schema->fields().end())),
          MetadataColumns::kDeleteFileRowDoc),
  };
  if (!partition_fields.empty()) {
    fields.push_back(SchemaField::MakeRequired(
        MetadataColumns::kPartitionColumnId, "partition", partition_type,
        "Partition that position delete row belongs to"));
  }
  fields.push_back(
      SchemaField::MakeRequired(MetadataColumns::kSpecIdColumnId, "spec_id", int32(),
                                "Spec ID used to track the file containing a row"));
  fields.push_back(
      SchemaField::MakeRequired(MetadataColumns::kFilePathColumnId, "delete_file_path",
                                string(), "Path of the file in which a row is stored"));

  if (table.metadata()->format_version >= 3) {
    fields.push_back(SchemaField::MakeOptional(
        MetadataColumns::kContentOffsetColumnId, "content_offset", int64(),
        "The offset in the DV where the content starts"));
    fields.push_back(SchemaField::MakeOptional(
        MetadataColumns::kContentSizeInBytesColumnId, "content_size_in_bytes", int64(),
        "The length in bytes of the DV blob"));
  }

  auto schema = std::make_shared<Schema>(std::move(fields));
  ICEBERG_RETURN_UNEXPECTED(schema->HighestFieldId());
  return PositionDeletesSchema{
      .schema = std::move(schema),
      .partition_type = std::move(partition_type),
      .partition_mappings = std::move(partition_mappings),
  };
}

template <typename T>
Result<const T*> GetLiteralValue(const Literal& literal, const Type& expected_type) {
  if (const auto* value = std::get_if<T>(&literal.value())) {
    return value;
  }
  return InvalidArrowData("Partition value has type {} but metadata schema expects {}",
                          literal.type()->ToString(), expected_type.ToString());
}

Status AppendLiteralValue(ArrowArray* array, const Literal& literal,
                          const std::shared_ptr<Type>& type) {
  if (literal.IsNull()) {
    return AppendNull(array);
  }

  const Literal* value = &literal;
  std::optional<Literal> promoted;
  if (*literal.type() != *type) {
    if (!IsPromotionAllowed(literal.type(), type)) {
      return InvalidArrowData(
          "Partition value has type {} but metadata schema expects {}",
          literal.type()->ToString(), type->ToString());
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto casted, literal.CastTo(std::static_pointer_cast<PrimitiveType>(type)));
    promoted.emplace(std::move(casted));
    value = &*promoted;
  }

  switch (type->type_id()) {
    case TypeId::kBoolean: {
      ICEBERG_ASSIGN_OR_RAISE(auto bool_value, GetLiteralValue<bool>(*value, *type));
      return AppendBoolean(array, *bool_value);
    }
    case TypeId::kInt:
    case TypeId::kDate: {
      ICEBERG_ASSIGN_OR_RAISE(auto int_value, GetLiteralValue<int32_t>(*value, *type));
      return AppendInt(array, *int_value);
    }
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs: {
      ICEBERG_ASSIGN_OR_RAISE(auto long_value, GetLiteralValue<int64_t>(*value, *type));
      return AppendInt(array, *long_value);
    }
    case TypeId::kFloat: {
      ICEBERG_ASSIGN_OR_RAISE(auto float_value, GetLiteralValue<float>(*value, *type));
      return AppendDouble(array, *float_value);
    }
    case TypeId::kDouble: {
      ICEBERG_ASSIGN_OR_RAISE(auto double_value, GetLiteralValue<double>(*value, *type));
      return AppendDouble(array, *double_value);
    }
    case TypeId::kString: {
      ICEBERG_ASSIGN_OR_RAISE(auto string_value,
                              GetLiteralValue<std::string>(*value, *type));
      return AppendString(array, *string_value);
    }
    case TypeId::kFixed:
    case TypeId::kBinary: {
      ICEBERG_ASSIGN_OR_RAISE(auto bytes_value,
                              GetLiteralValue<std::vector<uint8_t>>(*value, *type));
      return AppendBytes(array, *bytes_value);
    }
    case TypeId::kDecimal: {
      ICEBERG_ASSIGN_OR_RAISE(auto decimal_value,
                              GetLiteralValue<Decimal>(*value, *type));
      return AppendBytes(array, decimal_value->ToBytes());
    }
    case TypeId::kUuid: {
      ICEBERG_ASSIGN_OR_RAISE(auto uuid_value, GetLiteralValue<Uuid>(*value, *type));
      return AppendBytes(array, uuid_value->bytes());
    }
    default:
      return InvalidArrowData("Unsupported partition type: {}", type->ToString());
  }
}

Status AppendPartition(ArrowArray* array, const StructType& partition_type,
                       const PartitionValues& partition,
                       const PartitionMapping& mapping) {
  ICEBERG_PRECHECK(std::cmp_equal(array->n_children, mapping.size()),
                   "Partition builder does not match metadata table schema");
  for (size_t output_idx = 0; output_idx < mapping.size(); ++output_idx) {
    if (!mapping[output_idx].has_value()) {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(array->children[output_idx]));
      continue;
    }
    const size_t input_idx = *mapping[output_idx];
    ICEBERG_PRECHECK(input_idx < partition.num_fields(),
                     "Partition values do not match partition spec");
    ICEBERG_ASSIGN_OR_RAISE(auto literal, partition.ValueAt(input_idx));
    ICEBERG_RETURN_UNEXPECTED(
        AppendLiteralValue(array->children[output_idx], literal.get(),
                           partition_type.fields()[output_idx].type()));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

Status AppendOptionalInt(ArrowArray* array, const std::optional<int64_t>& value) {
  return value.has_value() ? AppendInt(array, *value) : AppendNull(array);
}

struct OutputLayout {
  explicit OutputLayout(bool has_partition, bool is_v3)
      : partition(has_partition ? std::optional<size_t>(3) : std::nullopt),
        spec_id(has_partition ? 4 : 3),
        delete_file_path(spec_id + 1),
        content_offset(is_v3 ? std::optional<size_t>(delete_file_path + 1)
                             : std::nullopt),
        content_size(is_v3 ? std::optional<size_t>(delete_file_path + 2) : std::nullopt) {
  }

  std::optional<size_t> partition;
  size_t spec_id;
  size_t delete_file_path;
  std::optional<size_t> content_offset;
  std::optional<size_t> content_size;
};

struct DeletedRowValue {
  const ArrowSchema* schema;
  const ArrowArray* array;
  const ArrowArrayView* view;
  int64_t row_index;
};

Status AppendPositionDeleteRow(
    ArrowRowBuilder& builder, const OutputLayout& layout, const DataFile& delete_file,
    std::string_view data_file_path, int64_t pos, int32_t spec_id,
    const StructType& partition_type,
    const std::unordered_map<int32_t, PartitionMapping>& partition_mappings,
    const DeletedRowValue* deleted_row) {
  ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(0), data_file_path));
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(1), pos));
  if (deleted_row == nullptr) {
    ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(2)));
  } else {
    ICEBERG_RETURN_UNEXPECTED(AppendArrayValue(*deleted_row->schema, *deleted_row->array,
                                               *deleted_row->view, deleted_row->row_index,
                                               builder.column(2)));
  }

  if (layout.partition.has_value()) {
    auto mapping = partition_mappings.find(spec_id);
    ICEBERG_PRECHECK(mapping != partition_mappings.end(),
                     "Partition spec ID {} not found", spec_id);
    ICEBERG_RETURN_UNEXPECTED(AppendPartition(builder.column(*layout.partition),
                                              partition_type, delete_file.partition,
                                              mapping->second));
  }

  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(layout.spec_id), spec_id));
  ICEBERG_RETURN_UNEXPECTED(
      AppendString(builder.column(layout.delete_file_path), delete_file.file_path));
  if (layout.content_offset.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(AppendOptionalInt(builder.column(*layout.content_offset),
                                                delete_file.content_offset));
    ICEBERG_RETURN_UNEXPECTED(AppendOptionalInt(builder.column(*layout.content_size),
                                                delete_file.content_size_in_bytes));
  }
  return builder.FinishRow();
}

SchemaField ClearPositionDeleteReadDefaults(SchemaField field) {
  return field.WithInitialDefault(nullptr).WithWriteDefault(nullptr);
}

std::shared_ptr<Type> MakePositionDeleteReadType(const std::shared_ptr<Type>& type) {
  switch (type->type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = static_cast<const StructType&>(*type);
      std::vector<SchemaField> fields;
      fields.reserve(struct_type.fields().size());
      for (const auto& field : struct_type.fields()) {
        fields.push_back(ClearPositionDeleteReadDefaults(
                             field.WithType(MakePositionDeleteReadType(field.type())))
                             .AsOptional());
      }
      return std::make_shared<StructType>(std::move(fields));
    }
    case TypeId::kList: {
      const auto& list_type = static_cast<const ListType&>(*type);
      return std::make_shared<ListType>(
          ClearPositionDeleteReadDefaults(list_type.element().WithType(
              MakePositionDeleteReadType(list_type.element().type()))));
    }
    case TypeId::kMap: {
      const auto& map_type = static_cast<const MapType&>(*type);
      return std::make_shared<MapType>(
          ClearPositionDeleteReadDefaults(map_type.key()),
          ClearPositionDeleteReadDefaults(map_type.value().WithType(
              MakePositionDeleteReadType(map_type.value().type()))));
    }
    default:
      return type;
  }
}

std::shared_ptr<Schema> PositionDeleteFileSchema(const Schema& table_schema,
                                                 bool include_row) {
  std::vector<SchemaField> fields{
      MetadataColumns::kDeleteFilePath,
      MetadataColumns::kDeleteFilePos,
  };
  if (include_row) {
    std::vector<SchemaField> row_fields;
    row_fields.reserve(table_schema.fields().size());
    for (const auto& field : table_schema.fields()) {
      row_fields.push_back(ClearPositionDeleteReadDefaults(
                               field.WithType(MakePositionDeleteReadType(field.type())))
                               .AsOptional());
    }
    fields.push_back(SchemaField::MakeRequired(
        MetadataColumns::kDeleteFileRowColumnId, MetadataColumns::kDeleteFileRowFieldName,
        std::make_shared<StructType>(std::move(row_fields)),
        MetadataColumns::kDeleteFileRowDoc));
  }
  return std::make_shared<Schema>(std::move(fields));
}

struct PositionDeleteReader {
  std::unique_ptr<Reader> reader;
  bool has_row;
};

Result<PositionDeleteReader> OpenPositionDeleteFile(const DataFile& file,
                                                    const Schema& table_schema,
                                                    const std::shared_ptr<FileIO>& io) {
  ICEBERG_PRECHECK(file.file_format == FileFormatType::kParquet,
                   "Unsupported position delete format: {}", ToString(file.file_format));
  auto reader = ReaderFactoryRegistry::Open(
      file.file_format, ReaderOptions{
                            .path = file.file_path,
                            .length = static_cast<size_t>(file.file_size_in_bytes),
                            .io = io,
                            .projection = PositionDeleteFileSchema(table_schema, true),
                        });
  if (reader.has_value()) {
    return PositionDeleteReader{.reader = std::move(*reader), .has_row = true};
  }

  const auto missing_row_message = std::format("Missing required field with id: {}",
                                               MetadataColumns::kDeleteFileRowColumnId);
  if (reader.error().kind != ErrorKind::kInvalidSchema ||
      reader.error().message != missing_row_message) {
    return std::unexpected(reader.error());
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto without_row,
      ReaderFactoryRegistry::Open(
          file.file_format,
          ReaderOptions{
              .path = file.file_path,
              .length = static_cast<size_t>(file.file_size_in_bytes),
              .io = io,
              .projection = PositionDeleteFileSchema(table_schema, false),
          }));
  return PositionDeleteReader{.reader = std::move(without_row), .has_row = false};
}

Status AppendParquetDeletes(
    ArrowRowBuilder& builder, const OutputLayout& layout,
    const std::shared_ptr<DataFile>& delete_file, int32_t spec_id,
    const StructType& partition_type,
    const std::unordered_map<int32_t, PartitionMapping>& partition_mappings,
    const Schema& table_schema, const std::shared_ptr<FileIO>& io) {
  ICEBERG_ASSIGN_OR_RAISE(auto delete_reader,
                          OpenPositionDeleteFile(*delete_file, table_schema, io));
  auto& reader = delete_reader.reader;
  ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader->Schema());
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);
  const int64_t expected_columns = delete_reader.has_row ? 3 : 2;
  ICEBERG_PRECHECK(arrow_schema.n_children == expected_columns,
                   "Position delete reader returned {} columns, expected {}",
                   arrow_schema.n_children, expected_columns);

  ArrowArrayView array_view;
  internal::ArrowArrayViewGuard view_guard(&array_view);
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewInitFromSchema(&array_view, &arrow_schema, &error), error);

  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto batch_opt, reader->Next());
    if (!batch_opt.has_value()) {
      break;
    }

    auto& batch = *batch_opt;
    internal::ArrowArrayGuard batch_guard(&batch);
    ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
        ArrowArrayViewSetArray(&array_view, &batch, &error), error);
    ICEBERG_PRECHECK(batch.n_children == expected_columns,
                     "Position delete batch has {} columns, expected {}",
                     batch.n_children, expected_columns);

    const auto* path_view = array_view.children[0];
    const auto* pos_view = array_view.children[1];
    if (ArrowArrayViewComputeNullCount(path_view) != 0 ||
        ArrowArrayViewComputeNullCount(pos_view) != 0) {
      return InvalidArrowData(
          "position delete file has null values in required pos/file_path columns");
    }
    if (delete_reader.has_row &&
        ArrowArrayViewComputeNullCount(array_view.children[2]) != 0) {
      return InvalidArrowData("position delete file has null row values");
    }

    const int64_t* positions = pos_view->buffer_views[1].data.as_int64 + pos_view->offset;
    for (int64_t row = 0; row < batch.length; ++row) {
      ICEBERG_PRECHECK(positions[row] >= 0, "Invalid negative delete position: {}",
                       positions[row]);
      const ArrowStringView path = ArrowArrayViewGetStringUnsafe(path_view, row);
      std::optional<DeletedRowValue> deleted_row;
      if (delete_reader.has_row) {
        deleted_row.emplace(DeletedRowValue{
            .schema = arrow_schema.children[2],
            .array = batch.children[2],
            .view = array_view.children[2],
            .row_index = row,
        });
      }
      ICEBERG_RETURN_UNEXPECTED(AppendPositionDeleteRow(
          builder, layout, *delete_file,
          std::string_view(path.data, static_cast<size_t>(path.size_bytes)),
          positions[row], spec_id, partition_type, partition_mappings,
          deleted_row ? &*deleted_row : nullptr));
    }
  }

  return reader->Close();
}

Status AppendDeletionVector(
    ArrowRowBuilder& builder, const OutputLayout& layout,
    const std::shared_ptr<DataFile>& delete_file, int32_t spec_id,
    const StructType& partition_type,
    const std::unordered_map<int32_t, PartitionMapping>& partition_mappings,
    const std::shared_ptr<FileIO>& io) {
  ICEBERG_PRECHECK(delete_file->referenced_data_file.has_value(),
                   "Deletion vector requires a referenced data file");
  ICEBERG_ASSIGN_OR_RAISE(auto positions, DVUtil::ReadDV(delete_file, io));

  Status status = {};
  positions.ForEach([&](int64_t pos) {
    if (status.has_value()) {
      status = AppendPositionDeleteRow(builder, layout, *delete_file,
                                       *delete_file->referenced_data_file, pos, spec_id,
                                       partition_type, partition_mappings, nullptr);
    }
  });
  return status;
}

Result<ArrowArray> ProjectResult(ArrowArray array, const Schema& input_schema,
                                 const Schema& projected_schema) {
  if (input_schema == projected_schema) {
    return array;
  }

  internal::ArrowArrayGuard array_guard(&array);
  ICEBERG_ASSIGN_OR_RAISE(
      auto projection,
      ProjectionContext::Make(input_schema, projected_schema,
                              ProjectionContext::ResolveProjectBatchFunction()));
  std::vector<int32_t> rows(static_cast<size_t>(array.length));
  std::iota(rows.begin(), rows.end(), 0);
  array_guard.Release();
  return ProjectBatch(&array, rows, projection);
}

}  // namespace

PositionDeletesTable::PositionDeletesTable(
    std::shared_ptr<Table> table, std::shared_ptr<Schema> schema,
    std::shared_ptr<StructType> partition_type,
    std::unordered_map<int32_t, PartitionMapping> partition_mappings)
    : MetadataTable(table, MakePositionDeletesTableName(table->name()),
                    std::move(schema)),
      partition_type_(std::move(partition_type)),
      partition_mappings_(std::move(partition_mappings)) {}

PositionDeletesTable::~PositionDeletesTable() = default;

Result<std::unique_ptr<PositionDeletesTable>> PositionDeletesTable::Make(
    std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  ICEBERG_ASSIGN_OR_RAISE(auto schema, MakePositionDeletesSchema(*table));
  return std::unique_ptr<PositionDeletesTable>(new PositionDeletesTable(
      std::move(table), std::move(schema.schema), std::move(schema.partition_type),
      std::move(schema.partition_mappings)));
}

Result<ArrowArray> PositionDeletesTable::Scan(const Schema& projected_schema) {
  ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(*schema()));
  const bool has_partition = !partition_type_->fields().empty();
  const bool is_v3 = source_table()->metadata()->format_version >= 3;
  const OutputLayout layout(has_partition, is_v3);

  if (source_table()->metadata()->current_snapshot_id != kInvalidSnapshotId) {
    ICEBERG_ASSIGN_OR_RAISE(auto current_snapshot, source_table()->current_snapshot());
    SnapshotCache cache(current_snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, cache.DeleteManifests(source_table()->io()));
    ICEBERG_ASSIGN_OR_RAISE(auto table_schema, source_table()->schema());
    ICEBERG_ASSIGN_OR_RAISE(auto specs_ref, source_table()->specs());
    const auto& specs = specs_ref.get();

    for (const auto& manifest : manifests) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto reader,
          ManifestReader::Make(manifest, source_table()->io(), table_schema, specs));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
      for (const auto& entry : entries) {
        ICEBERG_PRECHECK(entry.data_file != nullptr,
                         "Manifest entry must have a data file");
        const auto& file = entry.data_file;
        if (file->content != DataFile::Content::kPositionDeletes) {
          continue;
        }
        const int32_t spec_id =
            file->partition_spec_id.value_or(manifest.partition_spec_id);
        if (file->IsDeletionVector()) {
          ICEBERG_RETURN_UNEXPECTED(
              AppendDeletionVector(builder, layout, file, spec_id, *partition_type_,
                                   partition_mappings_, source_table()->io()));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AppendParquetDeletes(
              builder, layout, file, spec_id, *partition_type_, partition_mappings_,
              *table_schema, source_table()->io()));
        }
      }
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto result, std::move(builder).Finish());
  return ProjectResult(std::move(result), *schema(), projected_schema);
}

}  // namespace iceberg
