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

#include "iceberg/data/delete_filter.h"

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/result.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/struct_like_set.h"

namespace iceberg {

namespace {

std::optional<size_t> FindFieldIndexById(std::span<const SchemaField> fields,
                                         int32_t field_id) {
  for (size_t pos = 0; pos < fields.size(); ++pos) {
    if (fields[pos].field_id() == field_id) {
      return pos;
    }
  }
  return std::nullopt;
}

Result<size_t> RequireFieldIndexById(std::span<const SchemaField> fields,
                                     int32_t field_id, std::string_view context) {
  auto pos = FindFieldIndexById(fields, field_id);
  if (pos.has_value()) {
    return pos.value();
  }
  return InvalidSchema("Cannot find field id {} in {}", field_id, context);
}

// Views a source row through the equality-delete key schema: fields are selected by
// field id, then exposed by position so StructLikeSet can compare only delete keys.
class ProjectedStructLike : public StructLike {
 public:
  struct ProjectedField;
  using ProjectedSubFields = std::vector<ProjectedField>;

  struct ProjectedField {
    int32_t field_id;
    size_t source_field_pos;
    std::shared_ptr<const ProjectedSubFields> nested_projected_fields;
  };

  explicit ProjectedStructLike(std::shared_ptr<const ProjectedSubFields> projected_fields)
      : projected_fields_(std::move(projected_fields)) {
    nested_projected_structs_.reserve(projected_fields_->size());
    for (const auto& projected_field : *projected_fields_) {
      nested_projected_structs_.push_back(
          projected_field.nested_projected_fields == nullptr
              ? nullptr
              : std::make_shared<ProjectedStructLike>(
                    projected_field.nested_projected_fields));
    }
  }

  /// \brief Build field-id based positions from the source row to the equality keys.
  ///
  /// \param source_type the schema of wrapped rows
  /// \param target_type the key schema used by the equality-delete set
  static Result<std::shared_ptr<const ProjectedSubFields>> BuildProjection(
      const StructType& source_type, const StructType& target_type) {
    ProjectedSubFields projected_fields;
    projected_fields.reserve(target_type.fields().size());
    for (const auto& target_field : target_type.fields()) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto source_field_pos,
          RequireFieldIndexById(source_type.fields(), target_field.field_id(),
                                "source projection"));
      const auto& source_field = source_type.fields()[source_field_pos];

      std::shared_ptr<const ProjectedSubFields> nested_projected_fields;
      if (*source_field.type() != *target_field.type()) {
        if (target_field.type()->type_id() == TypeId::kStruct &&
            source_field.type()->type_id() == TypeId::kStruct) {
          ICEBERG_ASSIGN_OR_RAISE(
              nested_projected_fields,
              BuildProjection(
                  internal::checked_cast<const StructType&>(*source_field.type()),
                  internal::checked_cast<const StructType&>(*target_field.type())));
        } else if (target_field.type()->is_nested()) {
          return NotSupported("Cannot project partial non-struct equality field id {}",
                              target_field.field_id());
        }
      }

      projected_fields.push_back(ProjectedField{
          .field_id = target_field.field_id(),
          .source_field_pos = source_field_pos,
          .nested_projected_fields = std::move(nested_projected_fields),
      });
    }
    return std::make_shared<const ProjectedSubFields>(std::move(projected_fields));
  }

  void Wrap(const StructLike& row) {
    owned_row_.reset();
    row_ = &row;
  }

  void Wrap(std::shared_ptr<StructLike> row) {
    owned_row_ = std::move(row);
    row_ = owned_row_.get();
  }

  Result<Scalar> GetField(size_t pos) const override {
    ICEBERG_PRECHECK(row_ != nullptr, "ProjectedStructLike has no wrapped row");
    if (pos >= projected_fields_->size()) {
      return InvalidArgument("Projected field index {} out of range (size: {})", pos,
                             projected_fields_->size());
    }

    const auto& projected_field = (*projected_fields_)[pos];
    ICEBERG_ASSIGN_OR_RAISE(auto scalar,
                            row_->GetField(projected_field.source_field_pos));
    if (projected_field.nested_projected_fields == nullptr ||
        std::holds_alternative<std::monostate>(scalar)) {
      return scalar;
    }

    if (!std::holds_alternative<std::shared_ptr<StructLike>>(scalar)) {
      return InvalidSchema("Expected struct field id {} while projecting equality row",
                           projected_field.field_id);
    }

    auto child = std::get<std::shared_ptr<StructLike>>(std::move(scalar));
    if (child == nullptr) {
      return Scalar{std::monostate{}};
    }

    auto projected_struct = nested_projected_structs_[pos];
    projected_struct->Wrap(std::move(child));
    return Scalar{std::static_pointer_cast<StructLike>(std::move(projected_struct))};
  }

  size_t num_fields() const override { return projected_fields_->size(); }

 private:
  std::shared_ptr<StructLike> owned_row_;
  const StructLike* row_ = nullptr;
  std::shared_ptr<const ProjectedSubFields> projected_fields_;
  std::vector<std::shared_ptr<ProjectedStructLike>> nested_projected_structs_;
};

Status ValidateEqualityIds(const DataFile& delete_file) {
  if (delete_file.equality_ids.empty()) {
    return InvalidArgument("Equality delete file '{}' has no equality field ids",
                           delete_file.file_path);
  }
  return {};
}

SchemaField WithType(const SchemaField& field, std::shared_ptr<Type> type) {
  return SchemaField{field.field_id(), std::string(field.name()), std::move(type),
                     field.optional(), std::string(field.doc())};
}

std::shared_ptr<Type> SortStructFieldsById(const std::shared_ptr<Type>& type) {
  if (type->type_id() != TypeId::kStruct) {
    return type;
  }

  const auto& struct_type = internal::checked_cast<const StructType&>(*type);
  auto source_fields = struct_type.fields();
  std::vector<std::shared_ptr<Type>> sorted_types;
  sorted_types.reserve(source_fields.size());
  bool changed = false;
  for (const auto& field : source_fields) {
    auto sorted_type = SortStructFieldsById(field.type());
    changed = changed || sorted_type != field.type();
    sorted_types.push_back(std::move(sorted_type));
  }

  const bool needs_sort =
      !std::ranges::is_sorted(source_fields, {}, &SchemaField::field_id);
  if (!changed && !needs_sort) {
    return type;
  }

  std::vector<SchemaField> fields;
  fields.reserve(source_fields.size());
  for (size_t pos = 0; pos < source_fields.size(); ++pos) {
    const auto& field = source_fields[pos];
    fields.push_back(sorted_types[pos] == field.type()
                         ? field
                         : WithType(field, std::move(sorted_types[pos])));
  }

  if (needs_sort) {
    std::ranges::sort(fields, {}, &SchemaField::field_id);
  }
  return std::make_shared<StructType>(std::move(fields));
}

void SortFieldsById(std::vector<SchemaField>& fields) {
  for (auto& field : fields) {
    auto sorted_type = SortStructFieldsById(field.type());
    if (sorted_type != field.type()) {
      field = WithType(field, std::move(sorted_type));
    }
  }
  std::ranges::sort(fields, {}, &SchemaField::field_id);
}

Result<std::vector<SchemaField>> ProjectFieldsById(
    const Schema& schema, const std::set<int32_t>& selected_ids) {
  std::unordered_set<int32_t> unordered_ids(selected_ids.begin(), selected_ids.end());
  ICEBERG_ASSIGN_OR_RAISE(auto projected_schema, schema.Project(unordered_ids));
  std::vector<SchemaField> fields(projected_schema->fields().begin(),
                                  projected_schema->fields().end());
  return fields;
}

Result<std::vector<SchemaField>> ProjectEqualityKeyFields(
    const Schema& schema, const std::set<int32_t>& selected_ids) {
  ICEBERG_ASSIGN_OR_RAISE(auto fields, ProjectFieldsById(schema, selected_ids));
  // Equality-delete keys keep the projected struct shape; fields are sorted by id
  // within each struct level, not flattened by nested leaf ids.
  SortFieldsById(fields);
  return fields;
}

bool ContainsFieldId(const SchemaField& field, int32_t field_id);

bool ContainsFieldId(const Type& type, int32_t field_id) {
  if (!type.is_nested()) {
    return false;
  }
  const auto& nested = internal::checked_cast<const NestedType&>(type);
  return std::ranges::any_of(nested.fields(), [field_id](const SchemaField& field) {
    return ContainsFieldId(field, field_id);
  });
}

bool ContainsFieldId(const SchemaField& field, int32_t field_id) {
  return field.field_id() == field_id || ContainsFieldId(*field.type(), field_id);
}

Status ValidateEqualityProjectionField(int32_t field_id, const SchemaField& field) {
  if (field.field_id() == field_id) {
    if (!field.type()->is_primitive()) {
      return InvalidArgument(
          "Equality delete field id {} must reference a primitive field", field_id);
    }
    return {};
  }

  switch (field.type()->type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = internal::checked_cast<const StructType&>(*field.type());
      for (const auto& child : struct_type.fields()) {
        if (ContainsFieldId(child, field_id)) {
          return ValidateEqualityProjectionField(field_id, child);
        }
      }
      break;
    }
    case TypeId::kList:
    case TypeId::kMap:
      if (ContainsFieldId(*field.type(), field_id)) {
        return InvalidArgument("Equality delete field id {} must not be nested in {}",
                               field_id, ToString(field.type()->type_id()));
      }
      break;
    default:
      break;
  }

  return InvalidSchema("Cannot find equality delete field id {} in projection field {}",
                       field_id, field.field_id());
}

Result<std::optional<DeleteFilter::FieldLookupResult>> LookupFieldInSchema(
    const Schema& schema, int32_t field_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto field, schema.FindFieldById(field_id));
  if (!field.has_value()) {
    return std::nullopt;
  }
  if (!field->get().type()->is_primitive()) {
    return InvalidArgument("Equality delete field id {} must reference a primitive field",
                           field_id);
  }

  std::set<int32_t> selected_ids = {field_id};
  ICEBERG_ASSIGN_OR_RAISE(auto projected_fields, ProjectFieldsById(schema, selected_ids));
  if (projected_fields.empty()) {
    return InvalidSchema("Cannot project field id {} from lookup schema", field_id);
  }
  if (projected_fields.size() != 1) {
    return InvalidSchema("Expected one top-level projection for field id {} but got {}",
                         field_id, projected_fields.size());
  }

  return DeleteFilter::FieldLookupResult{
      .field = field.value().get(),
      .projection_field = std::move(projected_fields[0]),
  };
}

Result<bool> MergeField(SchemaField& existing, const SchemaField& required) {
  if (existing.field_id() != required.field_id()) {
    return InvalidSchema("Cannot merge field id {} with field id {}", existing.field_id(),
                         required.field_id());
  }

  if (*existing.type() == *required.type() || !required.type()->is_nested()) {
    return false;
  }

  if (existing.type()->type_id() == TypeId::kStruct &&
      required.type()->type_id() == TypeId::kStruct) {
    const auto& existing_struct =
        internal::checked_cast<const StructType&>(*existing.type());
    std::vector<SchemaField> fields(existing_struct.fields().begin(),
                                    existing_struct.fields().end());
    const auto& required_struct =
        internal::checked_cast<const StructType&>(*required.type());

    bool changed = false;
    for (const auto& required_child : required_struct.fields()) {
      auto existing_pos = FindFieldIndexById(fields, required_child.field_id());
      if (existing_pos.has_value()) {
        ICEBERG_ASSIGN_OR_RAISE(auto child_changed,
                                MergeField(fields[existing_pos.value()], required_child));
        changed = changed || child_changed;
      } else {
        fields.push_back(required_child);
        changed = true;
      }
    }

    if (!changed) {
      return false;
    }
    existing = SchemaField(existing.field_id(), std::string(existing.name()),
                           std::make_shared<StructType>(std::move(fields)),
                           existing.optional(), std::string(existing.doc()));
    return true;
  }

  return InvalidArgument(
      "Cannot merge non-struct nested field id {} into delete projection",
      required.field_id());
}

Result<bool> MergeProjectionField(std::vector<SchemaField>& fields,
                                  const SchemaField& required_projection) {
  auto existing_pos = FindFieldIndexById(fields, required_projection.field_id());
  if (existing_pos.has_value()) {
    return MergeField(fields[existing_pos.value()], required_projection);
  }

  fields.push_back(required_projection);
  return true;
}

void AddIdOnce(std::vector<int32_t>& ids, std::unordered_set<int32_t>& seen,
               int32_t field_id) {
  if (seen.insert(field_id).second) {
    ids.push_back(field_id);
  }
}

}  // namespace

struct DeleteFilter::EqDeleteGroup {
  std::unique_ptr<ProjectedStructLike> row_projection;
  std::unique_ptr<UncheckedStructLikeSet> delete_set;
};

Result<DeleteFilter::FieldLookup> DeleteFilter::MakeFieldLookup(
    std::shared_ptr<Schema> table_schema,
    std::span<const std::shared_ptr<Schema>> schemas) {
  ICEBERG_PRECHECK(table_schema != nullptr, "Table schema must not be null");

  std::vector<std::shared_ptr<Schema>> lookup_schemas;
  lookup_schemas.reserve(schemas.size() + 1);
  const int32_t current_schema_id = table_schema->schema_id();
  lookup_schemas.push_back(std::move(table_schema));

  std::vector<std::shared_ptr<Schema>> sorted_fallback_schemas;
  sorted_fallback_schemas.reserve(schemas.size());
  for (const auto& schema : schemas) {
    ICEBERG_PRECHECK(schema != nullptr, "Schema must not be null");
    if (schema->schema_id() != current_schema_id) {
      sorted_fallback_schemas.push_back(schema);
    }
  }

  // Search fallback schemas from latest to oldest so the highest schema_id wins.
  std::ranges::stable_sort(sorted_fallback_schemas, [](const auto& lhs, const auto& rhs) {
    return lhs->schema_id() > rhs->schema_id();
  });

  std::unordered_set<int32_t> seen_schema_ids;
  seen_schema_ids.insert(current_schema_id);
  for (const auto& schema : sorted_fallback_schemas) {
    if (seen_schema_ids.insert(schema->schema_id()).second) {
      lookup_schemas.push_back(schema);
    }
  }

  return [lookup_schemas = std::move(lookup_schemas)](
             int32_t field_id) -> Result<std::optional<FieldLookupResult>> {
    for (const auto& schema : lookup_schemas) {
      ICEBERG_ASSIGN_OR_RAISE(auto field, LookupFieldInSchema(*schema, field_id));
      if (field.has_value()) {
        return field;
      }
    }
    return std::nullopt;
  };
}

Result<DeleteFilter::FieldLookup> DeleteFilter::MakeFieldLookup(
    std::shared_ptr<TableMetadata> table_metadata) {
  ICEBERG_PRECHECK(table_metadata != nullptr, "Table metadata must not be null");

  ICEBERG_ASSIGN_OR_RAISE(auto table_schema, table_metadata->Schema());
  return MakeFieldLookup(std::move(table_schema), table_metadata->schemas);
}

Result<std::unique_ptr<DeleteFilter>> DeleteFilter::Make(
    std::string file_path, std::span<const std::shared_ptr<DataFile>> delete_files,
    std::shared_ptr<Schema> table_schema, std::shared_ptr<Schema> requested_schema,
    std::shared_ptr<FileIO> io, bool need_row_pos_col,
    std::shared_ptr<DeleteCounter> counter) {
  ICEBERG_ASSIGN_OR_RAISE(auto field_lookup, MakeFieldLookup(table_schema));
  return Make(std::move(file_path), delete_files, std::move(requested_schema),
              std::move(io), std::move(field_lookup), need_row_pos_col,
              std::move(counter));
}

Result<std::unique_ptr<DeleteFilter>> DeleteFilter::Make(
    std::string file_path, std::span<const std::shared_ptr<DataFile>> delete_files,
    std::shared_ptr<TableMetadata> table_metadata,
    std::shared_ptr<Schema> requested_schema, std::shared_ptr<FileIO> io,
    bool need_row_pos_col, std::shared_ptr<DeleteCounter> counter) {
  ICEBERG_PRECHECK(table_metadata != nullptr, "Table metadata must not be null");

  ICEBERG_ASSIGN_OR_RAISE(auto field_lookup, MakeFieldLookup(std::move(table_metadata)));
  return Make(std::move(file_path), delete_files, std::move(requested_schema),
              std::move(io), std::move(field_lookup), need_row_pos_col,
              std::move(counter));
}

Result<std::unique_ptr<DeleteFilter>> DeleteFilter::Make(
    std::string file_path, std::span<const std::shared_ptr<DataFile>> delete_files,
    std::shared_ptr<Schema> table_schema, std::shared_ptr<Schema> requested_schema,
    std::shared_ptr<FileIO> io, std::span<const std::shared_ptr<Schema>> schemas,
    bool need_row_pos_col, std::shared_ptr<DeleteCounter> counter) {
  ICEBERG_ASSIGN_OR_RAISE(auto field_lookup, MakeFieldLookup(table_schema, schemas));
  return Make(std::move(file_path), delete_files, std::move(requested_schema),
              std::move(io), std::move(field_lookup), need_row_pos_col,
              std::move(counter));
}

Result<std::unique_ptr<DeleteFilter>> DeleteFilter::Make(
    std::string file_path, std::span<const std::shared_ptr<DataFile>> delete_files,
    std::shared_ptr<Schema> requested_schema, std::shared_ptr<FileIO> io,
    FieldLookup field_lookup, bool need_row_pos_col,
    std::shared_ptr<DeleteCounter> counter) {
  ICEBERG_PRECHECK(requested_schema != nullptr, "Requested schema must not be null");
  ICEBERG_PRECHECK(field_lookup != nullptr, "Field lookup must not be null");
  ICEBERG_PRECHECK(delete_files.empty() || io != nullptr,
                   "FileIO must not be null when delete files are present");

  auto filter = std::unique_ptr<DeleteFilter>(
      new DeleteFilter(std::move(file_path), std::move(requested_schema), std::move(io),
                       std::move(field_lookup), need_row_pos_col, std::move(counter)));
  ICEBERG_RETURN_UNEXPECTED(filter->Init(delete_files));
  return filter;
}

DeleteFilter::DeleteFilter(std::string file_path,
                           std::shared_ptr<Schema> requested_schema,
                           std::shared_ptr<FileIO> io, FieldLookup field_lookup,
                           bool need_row_pos_col, std::shared_ptr<DeleteCounter> counter)
    : file_path_(std::move(file_path)),
      requested_schema_(std::move(requested_schema)),
      field_lookup_(std::move(field_lookup)),
      need_row_pos_col_(need_row_pos_col),
      counter_(std::move(counter)),
      delete_loader_(std::move(io)) {}

DeleteFilter::~DeleteFilter() = default;

Status DeleteFilter::Init(std::span<const std::shared_ptr<DataFile>> delete_files) {
  for (const auto& delete_file : delete_files) {
    ICEBERG_PRECHECK(delete_file != nullptr, "Delete file must not be null");

    switch (delete_file->content) {
      case DataFile::Content::kPositionDeletes:
        pos_deletes_.push_back(delete_file);
        break;
      case DataFile::Content::kEqualityDeletes:
        ICEBERG_RETURN_UNEXPECTED(ValidateEqualityIds(*delete_file));
        eq_deletes_.push_back(delete_file);
        break;
      case DataFile::Content::kData:
        return InvalidArgument("Expected delete file but got data file '{}'",
                               delete_file->file_path);
      default:
        return InvalidArgument("Unknown delete file content type {}",
                               static_cast<int>(delete_file->content));
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(required_schema_, ComputeRequiredSchema());

  // Pre-compute _pos column position for reuse
  pos_field_position_ = FindFieldIndexById(required_schema_->fields(),
                                           MetadataColumns::kFilePositionColumnId);

  return {};
}

Result<std::shared_ptr<Schema>> DeleteFilter::ComputeRequiredSchema() const {
  if (!HasPositionDeletes() && !HasEqualityDeletes()) {
    return requested_schema_;
  }

  std::vector<int32_t> required_ids;
  std::unordered_set<int32_t> seen_required_ids;
  if (HasPositionDeletes() && need_row_pos_col_) {
    AddIdOnce(required_ids, seen_required_ids, MetadataColumns::kFilePositionColumnId);
  }

  for (const auto& delete_file : eq_deletes_) {
    for (int32_t field_id : delete_file->equality_ids) {
      AddIdOnce(required_ids, seen_required_ids, field_id);
    }
  }

  std::vector<SchemaField> fields(requested_schema_->fields().begin(),
                                  requested_schema_->fields().end());
  bool changed = false;

  for (int32_t field_id : required_ids) {
    if (field_id == MetadataColumns::kFilePositionColumnId ||
        field_id == MetadataColumns::kIsDeletedColumnId) {
      // These columns do not exist in the table schema and will be handled later.
      continue;
    }

    // Top-level primitive fields already cover equality-delete needs. Nested fields
    // still need lookup so we can validate/merge the required subfield projection.
    auto existing_pos = FindFieldIndexById(fields, field_id);
    if (existing_pos.has_value() && !fields[existing_pos.value()].type()->is_nested()) {
      continue;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto lookup, field_lookup_(field_id));
    if (!lookup.has_value()) {
      return InvalidArgument("Cannot find equality delete field id {}", field_id);
    }
    ICEBERG_RETURN_UNEXPECTED(
        ValidateEqualityProjectionField(field_id, lookup->projection_field));

    ICEBERG_ASSIGN_OR_RAISE(auto merged,
                            MergeProjectionField(fields, lookup->projection_field));
    changed = changed || merged;
  }

  const bool needs_pos =
      HasPositionDeletes() && need_row_pos_col_ &&
      !FindFieldIndexById(fields, MetadataColumns::kFilePositionColumnId).has_value();
  if (needs_pos) {
    fields.push_back(MetadataColumns::kRowPosition);
    changed = true;
  }

  if (!changed) {
    return requested_schema_;
  }

  return std::make_shared<Schema>(std::move(fields));
}

const std::shared_ptr<Schema>& DeleteFilter::RequiredSchema() const {
  return required_schema_;
}

bool DeleteFilter::HasPositionDeletes() const { return !pos_deletes_.empty(); }

bool DeleteFilter::HasEqualityDeletes() const { return !eq_deletes_.empty(); }

Status DeleteFilter::EnsurePositionDeletesLoaded() const {
  if (!HasPositionDeletes()) {
    return {};
  }

  std::lock_guard lock(pos_mutex_);
  if (pos_loaded_) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(pos_index_,
                          delete_loader_.LoadPositionDeletes(pos_deletes_, file_path_));
  pos_loaded_ = true;
  return {};
}

Status DeleteFilter::EnsureEqualityDeletesLoaded() const {
  if (!HasEqualityDeletes()) {
    return {};
  }

  std::lock_guard lock(eq_mutex_);
  if (eq_loaded_) {
    return {};
  }

  std::map<std::set<int32_t>, std::vector<std::shared_ptr<DataFile>>> files_by_ids;
  for (const auto& delete_file : eq_deletes_) {
    // equality_ids were already validated in Init, build the grouping key directly.
    std::set<int32_t> ids(delete_file->equality_ids.begin(),
                          delete_file->equality_ids.end());
    files_by_ids[std::move(ids)].push_back(delete_file);
  }

  std::vector<std::unique_ptr<EqDeleteGroup>> groups;
  groups.reserve(files_by_ids.size());

  for (auto& [field_ids, files] : files_by_ids) {
    ICEBERG_ASSIGN_OR_RAISE(auto fields,
                            ProjectEqualityKeyFields(*required_schema_, field_ids));
    auto equality_type = std::make_shared<StructType>(std::move(fields));

    ICEBERG_ASSIGN_OR_RAISE(auto row_projection, ProjectedStructLike::BuildProjection(
                                                     *required_schema_, *equality_type));
    auto project_row = std::make_unique<ProjectedStructLike>(std::move(row_projection));
    ICEBERG_ASSIGN_OR_RAISE(auto delete_set,
                            delete_loader_.LoadEqualityDeletes(files, *equality_type));
    groups.push_back(std::make_unique<EqDeleteGroup>(EqDeleteGroup{
        .row_projection = std::move(project_row),
        .delete_set = std::move(delete_set),
    }));
  }

  eq_groups_ = std::move(groups);
  eq_loaded_ = true;
  return {};
}

const std::shared_ptr<Schema>& DeleteFilter::ExpectedSchema() const {
  return requested_schema_;
}

void DeleteFilter::IncrementDeleteCount(int64_t count) {
  if (counter_ != nullptr) {
    counter_->Increment(count);
  }
}

Result<const PositionDeleteIndex*> DeleteFilter::DeletedRowPositions() const {
  if (!HasPositionDeletes()) {
    return nullptr;
  }
  ICEBERG_RETURN_UNEXPECTED(EnsurePositionDeletesLoaded());
  return &pos_index_;
}

Result<std::function<Result<bool>(const StructLike&)>> DeleteFilter::EqDeletedRowFilter()
    const {
  if (!HasEqualityDeletes()) {
    // No equality deletes: every row is alive.
    return [](const StructLike&) -> Result<bool> { return true; };
  }
  ICEBERG_RETURN_UNEXPECTED(EnsureEqualityDeletesLoaded());
  std::lock_guard lock(eq_mutex_);
  if (!eq_deleted_row_filter_cache_) {
    eq_deleted_row_filter_cache_ = [this](const StructLike& row) -> Result<bool> {
      for (const auto& group : eq_groups_) {
        auto& projected_row = *group->row_projection;
        projected_row.Wrap(row);
        ICEBERG_ASSIGN_OR_RAISE(auto matched, group->delete_set->Contains(projected_row));
        if (matched) {
          return false;
        }
      }
      return true;
    };
  }
  return eq_deleted_row_filter_cache_;
}

Result<std::function<Result<bool>(const StructLike&)>>
DeleteFilter::FindEqualityDeleteRows() const {
  if (!HasEqualityDeletes()) {
    // No equality deletes: no row is deleted.
    return [](const StructLike&) -> Result<bool> { return false; };
  }
  ICEBERG_ASSIGN_OR_RAISE(auto alive_filter, EqDeletedRowFilter());
  return [alive_filter = std::move(alive_filter)](const StructLike& row) -> Result<bool> {
    ICEBERG_ASSIGN_OR_RAISE(auto alive, alive_filter(row));
    return !alive;
  };
}

Result<AliveRowSelection> DeleteFilter::ComputeAliveRows(const ArrowSchema& batch_schema,
                                                         const ArrowArray& batch) const {
  ICEBERG_PRECHECK(batch.length >= 0, "Batch length must be non-negative");

  ICEBERG_RETURN_UNEXPECTED(EnsurePositionDeletesLoaded());
  ICEBERG_RETURN_UNEXPECTED(EnsureEqualityDeletesLoaded());

  AliveRowSelection result;
  if (batch.length == 0) {
    return result;
  }

  ICEBERG_PRECHECK(
      batch.length <= static_cast<int64_t>(std::numeric_limits<int32_t>::max()),
      "Batch length {} exceeds int32_t row index capacity", batch.length);
  result.indices.reserve(static_cast<size_t>(batch.length));
  ICEBERG_ASSIGN_OR_RAISE(auto row, ArrowArrayStructLike::Make(batch_schema, batch));

  for (int64_t i = 0; i < batch.length; ++i) {
    if (i > 0) {
      ICEBERG_RETURN_UNEXPECTED(row->Reset(i));
    }

    bool deleted = false;
    if (pos_field_position_.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(auto pos_scalar,
                              row->GetField(pos_field_position_.value()));
      auto* pos = std::get_if<int64_t>(&pos_scalar);
      if (pos == nullptr) {
        return InvalidArrowData("Position delete filtering requires non-null int64 _pos");
      }
      deleted = pos_index_.IsDeleted(*pos);
    }

    if (!deleted) {
      for (const auto& eq_group : eq_groups_) {
        auto& projected_row = *eq_group->row_projection;
        projected_row.Wrap(*row);
        ICEBERG_ASSIGN_OR_RAISE(auto matched,
                                eq_group->delete_set->Contains(projected_row));
        if (matched) {
          deleted = true;
          break;
        }
      }
    }

    if (!deleted) {
      result.indices.push_back(static_cast<int32_t>(i));
    } else if (counter_ != nullptr) {
      counter_->Increment();
    }
  }

  return result;
}

}  // namespace iceberg
