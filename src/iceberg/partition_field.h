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

/// \file iceberg/partition_field.h
/// A partition field in a partition spec

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief a field with its transform.
class ICEBERG_EXPORT PartitionField : public util::Formattable {
 public:
  /// \brief Construct a field.
  /// \param[in] source_id The source field ID.
  /// \param[in] field_id The partition field ID.
  /// \param[in] name The partition field name.
  /// \param[in] transform The transform function.
  PartitionField(int32_t source_id, int32_t field_id, std::string name,
                 std::shared_ptr<Transform> transform);

  /// \brief Get the source field ID.
  int32_t source_id() const;

  /// \brief Get the partition field ID.
  int32_t field_id() const;

  /// \brief Get the partition field name.
  std::string_view name() const;

  /// \brief Get the transform type.
  std::shared_ptr<Transform> const& transform() const;

  std::string ToString() const override;

  friend bool operator==(const PartitionField& lhs, const PartitionField& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two fields for equality.
  [[nodiscard]] bool Equals(const PartitionField& other) const;

  int32_t source_id_;
  int32_t field_id_;
  std::string name_;
  std::shared_ptr<Transform> transform_;
};

}  // namespace iceberg
