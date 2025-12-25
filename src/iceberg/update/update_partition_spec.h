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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

/// \brief API for partition spec evolution.
///
/// When committing, these changes will be applied to the current table metadata.
/// Commit conflicts will not be resolved and will result in a CommitFailed error.
class ICEBERG_EXPORT UpdatePartitionSpec : public PendingUpdate {
 public:
  static Result<std::shared_ptr<UpdatePartitionSpec>> Make(
      std::shared_ptr<Transaction> transaction);

  ~UpdatePartitionSpec() override;

  /// \brief Set whether column resolution in the source schema should be case sensitive.
  UpdatePartitionSpec& CaseSensitive(bool is_case_sensitive);

  /// \brief Add a new partition field from a source column.
  ///
  /// The partition field will use identity transform on the source column,
  /// and use source column name as the partition field name.
  ///
  /// \param source_name Source column name in the table schema.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddField(std::string_view source_name);

  /// \brief Add a new partition field with a custom name.
  ///
  /// \param term The term representing the source column, should be unbound.
  /// \param part_name Name for the partition field.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddField(const std::shared_ptr<Term>& term,
                                std::string_view part_name = "");

  /// \brief Remove a partition field by name.
  ///
  /// \param name Name of the partition field to remove.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& RemoveField(std::string_view name);

  /// \brief Remove a partition field by its source term.
  ///
  /// The partition field with the same transform and source reference will be removed.
  /// If the term is a reference and does not have a transform, identity transform
  /// is used.
  ///
  /// \param term The term representing the source column, should be unbound.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& RemoveField(const std::shared_ptr<Term>& term);

  /// \brief Rename a field in the partition spec.
  ///
  /// \param name Name of the partition field to rename.
  /// \param new_name Replacement name for the partition field.
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& RenameField(std::string_view name, std::string new_name);

  /// \brief Sets that the new partition spec will NOT be set as the default.
  ///
  /// The default behavior is to set the new spec as the default partition spec.
  ///
  /// \return Reference to this for method chaining.
  UpdatePartitionSpec& AddNonDefaultSpec();

  Kind kind() const final { return Kind::kUpdatePartitionSpec; }

  struct ApplyResult {
    std::shared_ptr<PartitionSpec> spec;
    bool set_as_default;
  };
  Result<ApplyResult> Apply();

 private:
  explicit UpdatePartitionSpec(std::shared_ptr<Transaction> transaction);

  /// \brief Pair of source ID and transform string for indexing.
  using TransformKey = std::pair<int32_t, std::string>;

  /// \brief Hash function for TransformKey.
  struct TransformKeyHash {
    size_t operator()(const TransformKey& key) const {
      return 31 * std::hash<int32_t>{}(key.first) + std::hash<std::string>{}(key.second);
    }
  };

  /// \brief Assign a new partition field ID.
  int32_t AssignFieldId();

  /// \brief Recycle or create a partition field.
  ///
  /// In V2 it searches for a similar partition field in historical partition specs. Tries
  /// to match on source field ID, transform type and target name (optional). If not found
  /// or in V1 cases it creates a new PartitionField.
  ///
  /// \param source_id The source field ID.
  /// \param transform The transform function.
  /// \param name The target partition field name, if specified.
  /// \return The recycled or newly created partition field.
  PartitionField RecycleOrCreatePartitionField(int32_t source_id,
                                               std::shared_ptr<Transform> transform,
                                               std::string_view name);

  /// \brief Internal implementation of AddField with resolved source ID and transform.
  UpdatePartitionSpec& AddFieldInternal(std::string_view name, int32_t source_id,
                                        const std::shared_ptr<Transform>& transform);

  /// \brief Generate a partition field name from the source and transform.
  Result<std::string> GeneratePartitionName(
      int32_t source_id, const std::shared_ptr<Transform>& transform) const;

  /// \brief Check if a transform is a time-based transform.
  static bool IsTimeTransform(const std::shared_ptr<Transform>& transform);

  /// \brief Check if a partition field uses void transform.
  static bool IsVoidTransform(const PartitionField& field);

  /// \brief Check for redundant time-based partition fields.
  void CheckForRedundantAddedPartitions(const PartitionField& field);

  /// \brief Handle rewriting a delete-and-add operation for the same field.
  UpdatePartitionSpec& RewriteDeleteAndAddField(const PartitionField& existing,
                                                std::string_view name);

  /// \brief Internal helper to remove a field by transform key.
  UpdatePartitionSpec& RemoveFieldByTransform(const TransformKey& key,
                                              std::string_view term_str);

  /// \brief Index the spec fields by name.
  static std::unordered_map<std::string, const PartitionField*, StringHash, StringEqual>
  IndexSpecByName(const PartitionSpec& spec);

  /// \brief Index the spec fields by (source_id, transform) pair.
  static std::unordered_map<TransformKey, const PartitionField*, TransformKeyHash>
  IndexSpecByTransform(const PartitionSpec& spec);

  /// \brief Build index of historical partition fields for efficient recycling (V2+).
  void BuildHistoricalFieldsIndex();

  // Configuration
  int32_t format_version_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  bool case_sensitive_{true};
  bool set_as_default_{true};
  int32_t last_assigned_partition_id_;

  // Indexes for existing fields
  std::unordered_map<std::string, const PartitionField*, StringHash, StringEqual>
      name_to_field_;
  std::unordered_map<TransformKey, const PartitionField*, TransformKeyHash>
      transform_to_field_;

  // Index for historical partition fields (V2+ only) for efficient recycling.
  // Maps (source_id, transform_string) -> PartitionField from all historical specs.
  std::unordered_map<TransformKey, PartitionField, TransformKeyHash> historical_fields_;

  // Pending changes
  std::vector<PartitionField> adds_;
  std::unordered_set<std::string, StringHash, StringEqual> added_field_names_;
  std::unordered_map<int32_t, std::string> added_time_fields_;
  std::unordered_map<TransformKey, std::string, TransformKeyHash>
      transform_to_added_field_;
  std::unordered_set<int32_t> deletes_;
  std::unordered_map<std::string, std::string, StringHash, StringEqual> renames_;
};

}  // namespace iceberg
