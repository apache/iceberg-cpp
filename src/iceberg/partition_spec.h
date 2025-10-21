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
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/partition_field.h"
#include "iceberg/result.h"
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

  /// \brief Create a new partition spec.
  ///
  /// \param schema The table schema.
  /// \param spec_id The spec ID.
  /// \param fields The partition fields.
  /// \param last_assigned_field_id The last assigned field ID. If not provided, it will
  /// be calculated from the fields.
  PartitionSpec(std::shared_ptr<Schema> schema, int32_t spec_id,
                std::vector<PartitionField> fields,
                std::optional<int32_t> last_assigned_field_id = std::nullopt);

  /// \brief Get an unsorted partition spec singleton.
  static const std::shared_ptr<PartitionSpec>& Unpartitioned();

  /// \brief Get the table schema
  const std::shared_ptr<Schema>& schema() const;

  /// \brief Get the spec ID.
  int32_t spec_id() const;

  /// \brief Get a list view of the partition fields.
  std::span<const PartitionField> fields() const;

  /// \brief Get the partition type.
  Result<std::shared_ptr<StructType>> PartitionType();

  std::string ToString() const override;

  int32_t last_assigned_field_id() const { return last_assigned_field_id_; }

  friend bool operator==(const PartitionSpec& lhs, const PartitionSpec& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two partition specs for equality.
  bool Equals(const PartitionSpec& other) const;

  std::shared_ptr<Schema> schema_;
  const int32_t spec_id_;
  std::vector<PartitionField> fields_;
  int32_t last_assigned_field_id_;

  // FIXME: use similar lazy initialization pattern as in StructType
  std::mutex mutex_;
  std::shared_ptr<StructType> partition_type_;
};

}  // namespace iceberg
