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

#include <functional>
#include <stack>

#include "iceberg/type.h"

/// \file iceberg/util/type_util.h
/// Utility functions for Iceberg types.

namespace iceberg {

/// \brief Index parent field IDs for all fields in a struct hierarchy.
/// \param root_struct The root struct type to analyze
/// \return A map from field ID to its parent struct field ID
/// \note This function assumes the input StructType has already been validated:
///       - All field IDs must be non-negative
///       - All field IDs must be unique across the entire schema hierarchy
///       If the struct is part of a Schema, these invariants are enforced by
///       StructType::InitFieldById which checks for duplicate field IDs.
static std::unordered_map<int32_t, int32_t> indexParents(const StructType& root_struct) {
  std::unordered_map<int32_t, int32_t> id_to_parent;
  std::stack<int32_t> parent_id_stack;

  // Recursive function to visit and build parent relationships
  std::function<void(const Type&)> visit = [&](const Type& type) -> void {
    switch (type.type_id()) {
      case TypeId::kStruct:
      case TypeId::kList:
      case TypeId::kMap: {
        const auto& nested_type = static_cast<const NestedType&>(type);
        for (const auto& field : nested_type.fields()) {
          if (!parent_id_stack.empty()) {
            id_to_parent[field.field_id()] = parent_id_stack.top();
          }
          parent_id_stack.push(field.field_id());
          visit(*field.type());
          parent_id_stack.pop();
        }
        break;
      }

      default:
        break;
    }
  };

  visit(root_struct);
  return id_to_parent;
}

}  // namespace iceberg
