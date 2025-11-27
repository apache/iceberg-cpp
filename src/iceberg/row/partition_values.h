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

/// \file iceberg/row/partition_values.h
/// Wrapper classes for partition value related data structures.

#include <functional>
#include <span>
#include <utility>

#include "iceberg/expression/literal.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/row/struct_like.h"

namespace iceberg {

/// \brief StructLike wrapper for a vector of literals that represent partition values.
class ICEBERG_EXPORT PartitionValues : public StructLike {
 public:
  PartitionValues() = default;
  explicit PartitionValues(std::vector<Literal> values) : values_(std::move(values)) {}
  explicit PartitionValues(Literal value) : values_({std::move(value)}) {}

  PartitionValues(const PartitionValues& other) : values_(other.values_) {}
  PartitionValues& operator=(const PartitionValues& other);

  PartitionValues(PartitionValues&&) noexcept = default;
  PartitionValues& operator=(PartitionValues&&) noexcept = default;

  ~PartitionValues() override = default;

  Result<Scalar> GetField(size_t pos) const override;

  size_t num_fields() const override { return values_.size(); }

  /// \brief Get the partition field value at the given position.
  /// \param pos The position of the field in the struct.
  /// \return A reference to the partition field value.
  Result<std::reference_wrapper<const Literal>> ValueAt(size_t pos) const;

  /// \brief Add a value to the partition values.
  /// \param value The value to add.
  void AddValue(Literal value) { values_.emplace_back(std::move(value)); }

  /// \brief Reset the partition values.
  /// \param values The values to reset to.
  void Reset(std::vector<Literal> values) { values_ = std::move(values); }

  std::span<const Literal> values() const { return values_; }

  bool operator==(const PartitionValues& other) const;

 private:
  std::vector<Literal> values_;
};

}  // namespace iceberg
