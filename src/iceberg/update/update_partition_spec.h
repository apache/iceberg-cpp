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

/// \file iceberg/update/update_partition_spec.h
/// API for partition spec evolution.

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief API for partition spec evolution.
///
/// When committing, these changes will be applied to the current table metadata.
/// Commit conflicts will not be resolved and will result in a CommitFailed error.
class ICEBERG_EXPORT UpdatePartitionSpec : public PendingUpdate {
 public:
  /// \brief Construct an UpdatePartitionSpec for the specified table.
  ///
  /// \param identifier The table identifier.
  /// \param catalog The catalog.
  /// \param base The base table metadata.
  UpdatePartitionSpec(TableIdentifier identifier, std::shared_ptr<Catalog> catalog,
                      std::shared_ptr<TableMetadata> base);

  ~UpdatePartitionSpec() override;

  /// \brief Set whether column resolution in the source schema should be case sensitive.
  UpdatePartitionSpec& CaseSensitive(bool is_case_sensitive);

  /// \brief Add a new partition field from a source column.
  ///
  /// The partition field will be created as an identity partition field for the given
  /// source column, with the same name as the source column.
  ///
  /// \param source_name Source column name in the table schema.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddField(const std::string& source_name);

  /// \brief Add a new partition field from an unbound term.
  ///
  /// The partition field will use the term's transform or the identity transform if
  /// the term is a reference.
  ///
  /// \param term The unbound term representing the partition transform.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddField(std::shared_ptr<UnboundTerm<BoundReference>> term);

  /// \brief Add a new partition field from an unbound transform term.
  ///
  /// \param term The unbound transform term.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddField(std::shared_ptr<UnboundTerm<BoundTransform>> term);

  /// \brief Add a new partition field with a custom name.
  ///
  /// \param name Name for the partition field.
  /// \param term The unbound term representing the partition transform.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddField(std::optional<std::string> name,
                                std::shared_ptr<UnboundTerm<BoundReference>> term);

  /// \brief Add a new partition field with a custom name from an unbound transform.
  ///
  /// \param name Name for the partition field.
  /// \param term The unbound transform term.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddField(std::optional<std::string> name,
                                std::shared_ptr<UnboundTerm<BoundTransform>> term);

  /// \brief Remove a partition field by name.
  ///
  /// \param name Name of the partition field to remove.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& RemoveField(const std::string& name);

  /// \brief Remove a partition field by its transform term.
  ///
  /// The partition field with the same transform and source reference will be removed.
  /// If the term is a reference and does not have a transform, the identity transform
  /// is used.
  ///
  /// \param term The unbound term representing the partition transform to remove.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& RemoveField(std::shared_ptr<UnboundTerm<BoundReference>> term);

  /// \brief Remove a partition field by its transform term.
  ///
  /// The partition field with the same transform and source reference will be removed.
  ///
  /// \param term The unbound transform term.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& RemoveField(std::shared_ptr<UnboundTerm<BoundTransform>> term);

  /// \brief Rename a field in the partition spec.
  ///
  /// \param name Name of the partition field to rename.
  /// \param new_name Replacement name for the partition field.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& RenameField(const std::string& name, const std::string& new_name);

  /// \brief Sets that the new partition spec will NOT be set as the default.
  ///
  /// The default behavior is to set the new spec as the default partition spec.
  ///
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddNonDefaultSpec();

  /// \brief Apply the pending changes and validate them.
  ///
  /// The resulting partition spec can be retrieved using GetAppliedSpec() after
  /// a successful Apply().
  ///
  /// \return Status::OK if the changes are valid, or an error.
  Status Apply() override;

  /// \brief Get the applied partition spec after a successful Apply().
  ///
  /// \return The applied partition spec, or an error if Apply() hasn't been called
  ///         successfully.
  Result<std::shared_ptr<PartitionSpec>> GetAppliedSpec() const;

  /// \brief Apply and commit the pending changes to the table.
  ///
  /// \return Status::OK if the commit was successful, or an error.
  Status Commit() override;

 private:
  /// \brief Pair of source ID and transform string for indexing.
  using TransformKey = std::pair<int32_t, std::string>;

  /// \brief Hash function for TransformKey.
  struct TransformKeyHash {
    size_t operator()(const TransformKey& key) const {
      return std::hash<int32_t>{}(key.first) ^
             (std::hash<std::string>{}(key.second) << 1);
    }
  };

  /// \brief Assign a new partition field ID.
  int32_t AssignFieldId();

  /// \brief Recycle or create a partition field.
  ///
  /// In V2, searches for a similar partition field in historical specs.
  /// If not found or in V1, creates a new PartitionField.
  PartitionField RecycleOrCreatePartitionField(int32_t source_id,
                                               std::shared_ptr<Transform> transform,
                                               const std::string* name);

  /// \brief Internal implementation of AddField with resolved source ID and transform.
  UpdatePartitionSpec& AddFieldInternal(const std::string* name, int32_t source_id,
                                        std::shared_ptr<Transform> transform);

  /// \brief Generate a partition field name from the source and transform.
  std::string GeneratePartitionName(int32_t source_id,
                                    const std::shared_ptr<Transform>& transform) const;

  /// \brief Check if a transform is a time-based transform.
  static bool IsTimeTransform(const std::shared_ptr<Transform>& transform);

  /// \brief Check if a partition field uses void transform.
  static bool IsVoidTransform(const PartitionField& field);

  /// \brief Check for redundant time-based partition fields.
  void CheckForRedundantAddedPartitions(const PartitionField& field);

  /// \brief Handle rewriting a delete-and-add operation for the same field.
  UpdatePartitionSpec& RewriteDeleteAndAddField(const PartitionField& existing,
                                                const std::string* name);

  /// \brief Internal helper to remove a field by transform key.
  UpdatePartitionSpec& RemoveFieldByTransform(const TransformKey& key,
                                              const std::string& term_str);

  /// \brief Index the spec fields by name.
  static std::unordered_map<std::string, PartitionField> IndexSpecByName(
      const PartitionSpec& spec);

  /// \brief Index the spec fields by (source_id, transform) pair.
  static std::unordered_map<TransformKey, PartitionField, TransformKeyHash>
  IndexSpecByTransform(const PartitionSpec& spec);

  TableIdentifier identifier_;
  std::shared_ptr<Catalog> catalog_;
  std::shared_ptr<TableMetadata> base_metadata_;

  // Configuration
  int32_t format_version_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  bool case_sensitive_{true};
  bool set_as_default_{true};
  int32_t last_assigned_partition_id_;

  // Indexes for existing fields
  std::unordered_map<std::string, PartitionField> name_to_field_;
  std::unordered_map<TransformKey, PartitionField, TransformKeyHash> transform_to_field_;

  // Pending changes
  std::vector<PartitionField> adds_;
  std::unordered_map<int32_t, PartitionField> added_time_fields_;
  std::unordered_map<TransformKey, PartitionField, TransformKeyHash>
      transform_to_added_field_;
  std::unordered_map<std::string, PartitionField> name_to_added_field_;
  std::unordered_set<int32_t> deletes_;
  std::unordered_map<std::string, std::string> renames_;

  // Applied result
  std::shared_ptr<PartitionSpec> applied_spec_;
};

}  // namespace iceberg
