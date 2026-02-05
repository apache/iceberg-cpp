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

#pragma once

/// \file iceberg/update/update_schema.h
/// API for schema evolution.

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

namespace iceberg {

/// \brief API for schema evolution.
///
/// When committing, these changes will be applied to the current table metadata.
/// Commit conflicts will not be resolved and will result in a CommitFailed error.
///
/// TODO(Guotao Yu): Add support for V3 default values when adding columns. Currently, all
/// added columns use null as the default value, but Iceberg V3 supports custom
/// default values for new columns.
class ICEBERG_EXPORT UpdateSchema : public PendingUpdate {
 public:
  static Result<std::shared_ptr<UpdateSchema>> Make(
      std::shared_ptr<Transaction> transaction);

  ~UpdateSchema() override;

  /// \brief Allow incompatible changes to the schema.
  ///
  /// Incompatible changes can cause failures when attempting to read older data files.
  /// For example, adding a required column and attempting to read data files without
  /// that column will cause a failure. However, if there are no data files that are
  /// not compatible with the change, it can be allowed.
  ///
  /// This option allows incompatible changes to be made to a schema. This should be
  /// used when the caller has validated that the change will not break. For example,
  /// if a column is added as optional but always populated and data older than the
  /// column addition has been deleted from the table, this can be used with
  /// RequireColumn() to mark the column required.
  ///
  /// \return Reference to this for method chaining.
  UpdateSchema& AllowIncompatibleChanges();

  /// \brief Add a new optional top-level column with documentation.
  ///
  /// Because "." may be interpreted as a column path separator or may be used in
  /// field names, it is not allowed in names passed to this method. To add to nested
  /// structures or to add fields with names that contain ".", use AddColumn(parent,
  /// name, type, doc).
  ///
  /// If type is a nested type, its field IDs are reassigned when added to the
  /// existing schema.
  ///
  /// The added column will be optional with a null default value.
  ///
  /// \param name Name for the new column.
  /// \param type Type for the new column.
  /// \param doc Documentation string for the new column.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name contains ".".
  UpdateSchema& AddColumn(std::string_view name, std::shared_ptr<Type> type,
                          std::string_view doc = "");

  /// \brief Add a new optional column to a nested struct with documentation.
  ///
  /// The parent name is used to find the parent using Schema::FindFieldByName(). If
  /// the parent name is null or empty, the new column will be added to the root as a
  /// top-level column. If parent identifies a struct, a new column is added to that
  /// struct. If it identifies a list, the column is added to the list element struct,
  /// and if it identifies a map, the new column is added to the map's value struct.
  ///
  /// The given name is used to name the new column and names containing "." are not
  /// handled differently.
  ///
  /// If type is a nested type, its field IDs are reassigned when added to the
  /// existing schema.
  ///
  /// The added column will be optional with a null default value.
  ///
  /// \param parent Name of the parent struct to which the column will be added.
  /// \param name Name for the new column.
  /// \param type Type for the new column.
  /// \param doc Documentation string for the new column.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if parent doesn't identify a struct.
  UpdateSchema& AddColumn(std::optional<std::string_view> parent, std::string_view name,
                          std::shared_ptr<Type> type, std::string_view doc = "");

  /// \brief Add a new required top-level column with documentation.
  ///
  /// Adding a required column without a default is an incompatible change that can
  /// break reading older data. To suppress exceptions thrown when an incompatible
  /// change is detected, call AllowIncompatibleChanges().
  ///
  /// Because "." may be interpreted as a column path separator or may be used in
  /// field names, it is not allowed in names passed to this method. To add to nested
  /// structures or to add fields with names that contain ".", use
  /// AddRequiredColumn(parent, name, type, doc).
  ///
  /// If type is a nested type, its field IDs are reassigned when added to the
  /// existing schema.
  ///
  /// \param name Name for the new column.
  /// \param type Type for the new column.
  /// \param doc Documentation string for the new column.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name contains ".".
  UpdateSchema& AddRequiredColumn(std::string_view name, std::shared_ptr<Type> type,
                                  std::string_view doc = "");

  /// \brief Add a new required column to a nested struct with documentation.
  ///
  /// Adding a required column without a default is an incompatible change that can
  /// break reading older data. To suppress exceptions thrown when an incompatible
  /// change is detected, call AllowIncompatibleChanges().
  ///
  /// The parent name is used to find the parent using Schema::FindFieldByName(). If
  /// the parent name is null or empty, the new column will be added to the root as a
  /// top-level column. If parent identifies a struct, a new column is added to that
  /// struct. If it identifies a list, the column is added to the list element struct,
  /// and if it identifies a map, the new column is added to the map's value struct.
  ///
  /// The given name is used to name the new column and names containing "." are not
  /// handled differently.
  ///
  /// If type is a nested type, its field IDs are reassigned when added to the
  /// existing schema.
  ///
  /// \param parent Name of the parent struct to which the column will be added.
  /// \param name Name for the new column.
  /// \param type Type for the new column.
  /// \param doc Documentation string for the new column.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if parent doesn't identify a struct.
  UpdateSchema& AddRequiredColumn(std::optional<std::string_view> parent,
                                  std::string_view name, std::shared_ptr<Type> type,
                                  std::string_view doc = "");

  /// \brief Rename a column in the schema.
  ///
  /// The name is used to find the column to rename using Schema::FindFieldByName().
  ///
  /// The new name may contain "." and such names are not parsed or handled
  /// differently.
  ///
  /// Columns may be updated and renamed in the same schema update.
  ///
  /// \param name Name of the column to rename.
  /// \param new_name Replacement name for the column.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name doesn't identify a column in the
  /// schema or if
  ///       this change conflicts with other additions, renames, or updates.
  UpdateSchema& RenameColumn(std::string_view name, std::string_view new_name);

  /// \brief Update a column in the schema to a new primitive type.
  ///
  /// The name is used to find the column to update using Schema::FindFieldByName().
  ///
  /// Only updates that widen types are allowed.
  ///
  /// Columns may be updated and renamed in the same schema update.
  ///
  /// \param name Name of the column to update.
  /// \param new_type Replacement type for the column (must be primitive).
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name doesn't identify a column in the
  /// schema or if
  ///       this change introduces a type incompatibility or if it conflicts with
  ///       other additions, renames, or updates.
  UpdateSchema& UpdateColumn(std::string_view name,
                             std::shared_ptr<PrimitiveType> new_type);

  /// \brief Update the documentation string for a column.
  ///
  /// The name is used to find the column to update using Schema::FindFieldByName().
  ///
  /// \param name Name of the column to update the documentation string for.
  /// \param new_doc Replacement documentation string for the column.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name doesn't identify a column in the
  /// schema or if
  ///       the column will be deleted.
  UpdateSchema& UpdateColumnDoc(std::string_view name, std::string_view new_doc);

  /// \brief Update a column to be optional.
  ///
  /// \param name Name of the column to mark optional.
  /// \return Reference to this for method chaining.
  UpdateSchema& MakeColumnOptional(std::string_view name);

  /// \brief Update a column to be required.
  ///
  /// This is an incompatible change that can break reading older data. This method
  /// will result in an exception unless AllowIncompatibleChanges() has been called.
  ///
  /// \param name Name of the column to mark required.
  /// \return Reference to this for method chaining.
  UpdateSchema& RequireColumn(std::string_view name);

  /// \brief Delete a column in the schema.
  ///
  /// The name is used to find the column to delete using Schema::FindFieldByName().
  ///
  /// \param name Name of the column to delete.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name doesn't identify a column in the
  /// schema or if
  ///       this change conflicts with other additions, renames, or updates.
  UpdateSchema& DeleteColumn(std::string_view name);

  /// \brief Move a column from its current position to the start of the schema or its
  /// parent struct.
  ///
  /// \param name Name of the column to move.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name doesn't identify a column in the
  /// schema or if
  ///       this change conflicts with other changes.
  UpdateSchema& MoveFirst(std::string_view name);

  /// \brief Move a column from its current position to directly before a reference
  /// column.
  ///
  /// The name is used to find the column to move using Schema::FindFieldByName(). If
  /// the name identifies a nested column, it can only be moved within the nested
  /// struct that contains it.
  ///
  /// \param name Name of the column to move.
  /// \param before_name Name of the reference column.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name doesn't identify a column in the
  /// schema or if
  ///       this change conflicts with other changes.
  UpdateSchema& MoveBefore(std::string_view name, std::string_view before_name);

  /// \brief Move a column from its current position to directly after a reference
  /// column.
  ///
  /// The name is used to find the column to move using Schema::FindFieldByName(). If
  /// the name identifies a nested column, it can only be moved within the nested
  /// struct that contains it.
  ///
  /// \param name Name of the column to move.
  /// \param after_name Name of the reference column.
  /// \return Reference to this for method chaining.
  /// \note InvalidArgument will be reported if name doesn't identify a column in the
  /// schema or if
  ///       this change conflicts with other changes.
  UpdateSchema& MoveAfter(std::string_view name, std::string_view after_name);

  /// \brief Applies all field additions and updates from the provided new schema to
  /// the existing schema to create a union schema.
  ///
  /// For fields with same canonical names in both schemas it is required that the
  /// widen types is supported using UpdateColumn(). Differences in type are ignored
  /// if the new type is narrower than the existing type (e.g. long to int, double to
  /// float).
  ///
  /// Only supports turning a previously required field into an optional one if it is
  /// marked optional in the provided new schema using MakeColumnOptional().
  ///
  /// Only supports updating existing field docs with fields docs from the provided
  /// new schema using UpdateColumnDoc().
  ///
  /// \param new_schema A schema used in conjunction with the existing schema to
  ///        create a union schema.
  /// \return Reference to this for method chaining.
  /// \note InvalidState will be reported if it encounters errors during provided schema
  /// traversal. \note InvalidArgument will be reported if name doesn't identify a column
  /// in the schema or if
  ///       this change introduces a type incompatibility or if it conflicts with
  ///       other additions, renames, or updates.
  UpdateSchema& UnionByNameWith(std::shared_ptr<Schema> new_schema);

  /// \brief Set the identifier fields given a set of field names.
  ///
  /// Because identifier fields are unique, duplicated names will be ignored. See
  /// Schema::identifier_field_ids() to learn more about Iceberg identifier.
  ///
  /// \param names Names of the columns to set as identifier fields.
  /// \return Reference to this for method chaining.
  UpdateSchema& SetIdentifierFields(const std::span<std::string_view>& names);

  /// \brief Determines if the case of schema needs to be considered when comparing
  /// column names.
  ///
  /// \param case_sensitive When false case is not considered in column name
  ///        comparisons.
  /// \return Reference to this for method chaining.
  UpdateSchema& CaseSensitive(bool case_sensitive);

  /// \brief Represents a column move operation within a struct (internal use only).
  struct Move {
    enum class MoveType { kFirst, kBefore, kAfter };

    int32_t field_id;
    int32_t reference_field_id;  // Only used for kBefore and kAfter
    MoveType type;

    static Move First(int32_t field_id);

    static Move Before(int32_t field_id, int32_t reference_field_id);

    static Move After(int32_t field_id, int32_t reference_field_id);
  };

  Kind kind() const final { return Kind::kUpdateSchema; }

  struct ApplyResult {
    std::shared_ptr<Schema> schema;
    int32_t new_last_column_id;
    std::unordered_map<std::string, std::string> updated_props;
  };

  /// \brief Apply the pending changes to the original schema and return the result.
  ///
  /// This does not result in a permanent update.
  ///
  /// \return The result Schema and last column id when all pending updates are applied.
  Result<ApplyResult> Apply();

 private:
  explicit UpdateSchema(std::shared_ptr<Transaction> transaction);

  /// \brief Internal implementation for adding a column with full control.
  ///
  /// \param parent Optional parent field name (nullopt for top-level).
  /// \param name Name for the new column.
  /// \param is_optional Whether the column is optional.
  /// \param type Type for the new column.
  /// \param doc Optional documentation string.
  /// \return Reference to this for method chaining.
  UpdateSchema& AddColumnInternal(std::optional<std::string_view> parent,
                                  std::string_view name, bool is_optional,
                                  std::shared_ptr<Type> type, std::string_view doc);

  /// \brief Internal implementation for updating column requirement (optional/required).
  ///
  /// \param name Name of the column to update.
  /// \param is_optional Whether the column should be optional (true) or required (false).
  /// \return Reference to this for method chaining.
  UpdateSchema& UpdateColumnRequirementInternal(std::string_view name, bool is_optional);

  /// \brief Assign a new column ID and increment the counter.
  int32_t AssignNewColumnId();

  /// \brief Find a field by name using case-sensitive or case-insensitive search.
  Result<std::optional<std::reference_wrapper<const SchemaField>>> FindField(
      std::string_view name) const;

  /// \brief Find a field for update operations, considering pending changes.
  ///
  /// This method checks both the original schema and pending updates/additions to
  /// return the most current view of a field. Used by operations that need to work
  /// with fields that may have been added or modified in the same transaction.
  ///
  /// \param name Name of the field to find.
  /// \return The field if found in schema or pending changes, nullopt otherwise.
  Result<std::optional<std::reference_wrapper<const SchemaField>>> FindFieldForUpdate(
      std::string_view name) const;

  /// \brief Normalize a field name based on case sensitivity setting.
  ///
  /// If case_sensitive_ is true, returns the name as-is.
  /// If case_sensitive_ is false, returns the lowercase version of the name.
  ///
  /// \param name The field name to normalize.
  /// \return The normalized field name.
  std::string CaseSensitivityAwareName(std::string_view name) const;

  /// \brief Find a field ID for move operations.
  Result<int32_t> FindFieldIdForMove(std::string_view name) const;

  /// \brief Internal implementation for recording a move operation.
  UpdateSchema& MoveInternal(std::string_view name, const Move& move);

  // Internal state
  std::shared_ptr<Schema> schema_;
  int32_t last_column_id_;
  bool allow_incompatible_changes_{false};
  bool case_sensitive_{true};
  std::vector<std::string> identifier_field_names_;

  // Tracking changes
  // field ID -> parent field ID
  std::unordered_map<int32_t, int32_t> id_to_parent_;
  // field IDs to delete
  std::unordered_set<int32_t> deletes_;
  // field ID -> updated field
  std::unordered_map<int32_t, std::shared_ptr<SchemaField>> updates_;
  // parent ID -> added child IDs
  std::unordered_map<int32_t, std::vector<int32_t>> parent_to_added_ids_;
  // full name -> field ID for added fields
  std::unordered_map<std::string, int32_t> added_name_to_id_;
  // parent ID -> move operations
  std::unordered_map<int32_t, std::vector<Move>> moves_;
};

}  // namespace iceberg
