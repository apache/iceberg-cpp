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

#include "iceberg/partition_spec.h"

#include <algorithm>
#include <format>
#include <memory>
#include <ranges>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

namespace iceberg {

PartitionSpec::PartitionSpec(int32_t spec_id, std::vector<PartitionField> fields,
                             std::optional<int32_t> last_assigned_field_id)
    : spec_id_(spec_id), fields_(std::move(fields)) {
  if (last_assigned_field_id) {
    last_assigned_field_id_ = last_assigned_field_id.value();
  } else if (fields_.empty()) {
    last_assigned_field_id_ = kLegacyPartitionDataIdStart - 1;
  } else {
    last_assigned_field_id_ = std::ranges::max(fields_, {}, [](const auto& field) {
                                return field.field_id();
                              }).field_id();
  }
}

const std::shared_ptr<PartitionSpec>& PartitionSpec::Unpartitioned() {
  static const std::shared_ptr<PartitionSpec> unpartitioned =
      std::make_shared<PartitionSpec>(kInitialSpecId, std::vector<PartitionField>{},
                                      kLegacyPartitionDataIdStart - 1);
  return unpartitioned;
}

int32_t PartitionSpec::spec_id() const { return spec_id_; }

std::span<const PartitionField> PartitionSpec::fields() const { return fields_; }

Result<std::unique_ptr<StructType>> PartitionSpec::PartitionType(const Schema& schema) {
  if (fields_.empty()) {
    return std::make_unique<StructType>(std::vector<SchemaField>{});
  }

  std::vector<SchemaField> partition_fields;
  for (const auto& partition_field : fields_) {
    // Get the source field from the original schema by source_id
    ICEBERG_ASSIGN_OR_RAISE(auto source_field,
                            schema.FindFieldById(partition_field.source_id()));
    if (!source_field.has_value()) {
      // TODO(xiao.dong) when source field is missing,
      // should return an error or just use UNKNOWN type
      return InvalidSchema("Cannot find source field for partition field:{}",
                           partition_field.field_id());
    }
    auto source_field_type = source_field.value().get().type();
    // Bind the transform to the source field type to get the result type
    ICEBERG_ASSIGN_OR_RAISE(auto transform_function,
                            partition_field.transform()->Bind(source_field_type));

    auto result_type = transform_function->ResultType();

    // Create the partition field with the transform result type
    // Partition fields are always optional (can be null)
    partition_fields.emplace_back(partition_field.field_id(),
                                  std::string(partition_field.name()),
                                  std::move(result_type),
                                  /*optional=*/true);
  }

  return std::make_unique<StructType>(std::move(partition_fields));
}

std::string PartitionSpec::ToString() const {
  std::string repr = std::format("partition_spec[spec_id<{}>,\n", spec_id_);
  for (const auto& field : fields_) {
    std::format_to(std::back_inserter(repr), "  {}\n", field);
  }
  repr += "]";
  return repr;
}

bool PartitionSpec::Equals(const PartitionSpec& other) const {
  return spec_id_ == other.spec_id_ && fields_ == other.fields_;
}

}  // namespace iceberg
