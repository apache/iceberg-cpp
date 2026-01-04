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

/// \file iceberg/partition_spec.h
/// Partition specs for Iceberg tables.

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/partition_field.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"
#include "iceberg/util/lazy.h"

namespace iceberg {

/// \brief A partition spec for a Table.
///
/// A partition spec is a list of partition fields, along with a unique integer ID.  A
/// Table may have different partition specs over its lifetime due to partition spec
/// evolution.
class ICEBERG_EXPORT PartitionSpec : public util::Formattable {
 public:
  static constexpr int32_t kInitialSpecId = 0;
  /// \brief The start ID for partition field.  It is only used to generate
  /// partition field id for v1 metadata where it is tracked.
  static constexpr int32_t kLegacyPartitionDataIdStart = 1000;
  static constexpr int32_t kInvalidPartitionFieldId = -1;

  /// \brief Get an unsorted partition spec singleton.
  static const std::shared_ptr<PartitionSpec>& Unpartitioned();

  /// \brief Get the spec ID.
  int32_t spec_id() const;

  /// \brief Get a list view of the partition fields.
  std::span<const PartitionField> fields() const;

  /// \brief Get the partition type binding to the input schema.
  Result<std::unique_ptr<StructType>> PartitionType(const Schema& schema) const;

  /// \brief Get the partition path for the given partition data.
  Result<std::string> PartitionPath(const PartitionValues& data) const;

  /// \brief Returns true if this spec is equivalent to the other, with partition field
  /// ids ignored. That is, if both specs have the same number of fields, field order,
  /// field name, source columns, and transforms.
  bool CompatibleWith(const PartitionSpec& other) const;

  std::string ToString() const override;

  int32_t last_assigned_field_id() const { return last_assigned_field_id_; }

  friend bool operator==(const PartitionSpec& lhs, const PartitionSpec& rhs) {
    return lhs.Equals(rhs);
  }

  /// \brief Validates the partition spec against a schema.
  /// \param schema The schema to validate against.
  /// \param allow_missing_fields Whether to skip validation for partition fields whose
  /// source columns have been dropped from the schema.
  /// \return Error status if the partition spec is invalid.
  Status Validate(const Schema& schema, bool allow_missing_fields) const;

  // \brief Validates the partition field names are unique within the partition spec and
  // schema.
  static Status ValidatePartitionName(const Schema& schema, const PartitionSpec& spec);

  /// \brief Get the partition fields by source ID.
  /// \param source_id The id of the source field.
  /// \return The partition fields by source ID, or NotFound if the source field is not
  /// found.
  using PartitionFieldRef = std::reference_wrapper<const PartitionField>;
  Result<std::vector<PartitionFieldRef>> GetFieldsBySourceId(int32_t source_id) const;

  /// \brief Create a PartitionSpec binding to a schema.
  /// \param schema The schema to bind the partition spec to.
  /// \param spec_id The spec ID.
  /// \param fields The partition fields.
  /// \param allowMissingFields Whether to skip validation for partition fields whose
  /// source columns have been dropped from the schema.
  /// \param last_assigned_field_id The last assigned field ID assigned to ensure new
  /// fields get unique IDs.
  /// \return A Result containing the partition spec or an error.
  static Result<std::unique_ptr<PartitionSpec>> Make(
      const Schema& schema, int32_t spec_id, std::vector<PartitionField> fields,
      bool allow_missing_fields,
      std::optional<int32_t> last_assigned_field_id = std::nullopt);

  /// \brief Create a PartitionSpec without binding to a schema.
  /// \param spec_id The spec ID.
  /// \param fields The partition fields.
  /// \param last_assigned_field_id The last assigned field ID assigned to ensure new
  /// fields get unique IDs.
  /// \return A Result containing the partition spec or an error.
  /// \note This method does not check whether the sort fields are valid for any schema.
  static Result<std::unique_ptr<PartitionSpec>> Make(
      int32_t spec_id, std::vector<PartitionField> fields,
      std::optional<int32_t> last_assigned_field_id = std::nullopt);

  static bool HasSequentialFieldIds(const PartitionSpec& spec);

 private:
  /// \brief Create a new partition spec.
  ///
  /// \param spec_id The spec ID.
  /// \param fields The partition fields.
  /// \param last_assigned_field_id The last assigned field ID. If not provided, it will
  /// be calculated from the fields.
  PartitionSpec(int32_t spec_id, std::vector<PartitionField> fields,
                std::optional<int32_t> last_assigned_field_id = std::nullopt);

  /// \brief Compare two partition specs for equality.
  bool Equals(const PartitionSpec& other) const;

  /// \brief Validates that there are no redundant partition fields in the spec.
  static Status ValidateRedundantPartitions(const PartitionSpec& spec);

  using SourceIdToFieldsMap = std::unordered_map<int32_t, std::vector<PartitionFieldRef>>;
  static Result<SourceIdToFieldsMap> InitSourceIdToFieldsMap(const PartitionSpec&);

  const int32_t spec_id_;
  std::vector<PartitionField> fields_;
  int32_t last_assigned_field_id_;
  Lazy<InitSourceIdToFieldsMap> source_id_to_fields_;
};

}  // namespace iceberg
