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
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/partition_field.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

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
  Result<std::unique_ptr<StructType>> PartitionType(const Schema&);

  std::string ToString() const override;

  int32_t last_assigned_field_id() const { return last_assigned_field_id_; }

  friend bool operator==(const PartitionSpec& lhs, const PartitionSpec& rhs) {
    return lhs.Equals(rhs);
  }

  /// \brief Validates the partition spec against a schema.
  /// \param schema The schema to validate against.
  /// \param allowMissingFields Whether to skip validation for partition fields whose
  /// source columns have been dropped from the schema.
  /// \return Error status if the partition spec is invalid.
  Status Validate(const Schema& schema, bool allow_missing_fields) const;

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

 private:
  /// \brief Create a new partition spec.
  ///
  /// \param schema The table schema.
  /// \param spec_id The spec ID.
  /// \param fields The partition fields.
  /// \param last_assigned_field_id The last assigned field ID. If not provided, it will
  /// be calculated from the fields.
  PartitionSpec(int32_t spec_id, std::vector<PartitionField> fields,
                std::optional<int32_t> last_assigned_field_id = std::nullopt);

  /// \brief Compare two partition specs for equality.
  bool Equals(const PartitionSpec& other) const;

  const int32_t spec_id_;
  std::vector<PartitionField> fields_;
  int32_t last_assigned_field_id_;
};

}  // namespace iceberg
