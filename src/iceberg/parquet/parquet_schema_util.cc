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

#include <parquet/schema.h>

#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg::parquet {

Result<SchemaProjection> Project(const Schema& expected_schema,
                                 const ::parquet::arrow::SchemaManifest& parquet_schema) {
  return NotImplemented("NYI");
}

Result<std::vector<int>> SelectedColumnIndices(const SchemaProjection& projection) {
  return NotImplemented("NYI");
}

bool HasFieldIds(const ::parquet::schema::NodePtr& node) {
  if (node->field_id() >= 0) {
    return true;
  }

  if (node->is_group()) {
    auto group_node = internal::checked_pointer_cast<::parquet::schema::GroupNode>(node);
    for (int i = 0; i < group_node->field_count(); i++) {
      if (HasFieldIds(group_node->field(i))) {
        return true;
      }
    }
  }

  return false;
}

}  // namespace iceberg::parquet
