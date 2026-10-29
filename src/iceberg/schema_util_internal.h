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

#include <map>

#include "iceberg/schema_util.h"

namespace iceberg {

// Fix `from` field of `FieldProjection` to use pruned field index.
inline void PruneFieldProjection(FieldProjection& field_projection) {
  std::map<size_t, size_t> local_index_to_pruned_index;
  for (const auto& child_projection : field_projection.children) {
    if (child_projection.kind == FieldProjection::Kind::kProjected) {
      local_index_to_pruned_index.emplace(std::get<1>(child_projection.from), 0);
    }
  }
  for (size_t pruned_index = 0; auto& [_, value] : local_index_to_pruned_index) {
    value = pruned_index++;
  }
  for (auto& child_projection : field_projection.children) {
    if (child_projection.kind == FieldProjection::Kind::kProjected) {
      child_projection.from =
          local_index_to_pruned_index.at(std::get<1>(child_projection.from));
    }
  }
}

}  // namespace iceberg
