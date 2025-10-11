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

#include "iceberg/update_requirements.h"

#include "iceberg/metadata_update.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

std::vector<std::unique_ptr<UpdateRequirement>> UpdateRequirements::ForCreateTable(
    const std::vector<std::unique_ptr<MetadataUpdate>>& metadata_updates) {
  // Create context for table creation (no base metadata)
  UpdateRequirementsContext context(nullptr, false);

  // Add requirement that table does not exist
  context.AddRequirement(std::make_unique<AssertTableDoesNotExist>());

  // Let each metadata update generate its requirements
  for (const auto& update : metadata_updates) {
    update->GenerateRequirements(context);
  }

  return context.Build();
}

std::vector<std::unique_ptr<UpdateRequirement>> UpdateRequirements::ForReplaceTable(
    const TableMetadata& base,
    const std::vector<std::unique_ptr<MetadataUpdate>>& metadata_updates) {
  // Create context for table replacement (is_replace = true)
  UpdateRequirementsContext context(&base, true);

  // Add requirement that UUID matches
  context.AddRequirement(std::make_unique<AssertTableUUID>(base.table_uuid));

  // Let each metadata update generate its requirements
  for (const auto& update : metadata_updates) {
    update->GenerateRequirements(context);
  }

  return context.Build();
}

std::vector<std::unique_ptr<UpdateRequirement>> UpdateRequirements::ForUpdateTable(
    const TableMetadata& base,
    const std::vector<std::unique_ptr<MetadataUpdate>>& metadata_updates) {
  // Create context for table update (is_replace = false)
  UpdateRequirementsContext context(&base, false);

  // Add requirement that UUID matches
  context.AddRequirement(std::make_unique<AssertTableUUID>(base.table_uuid));

  // Let each metadata update generate its requirements
  for (const auto& update : metadata_updates) {
    update->GenerateRequirements(context);
  }

  return context.Build();
}

}  // namespace iceberg
