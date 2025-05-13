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

#include "iceberg/metadata_columns.h"

#include <unordered_map>

namespace iceberg {

namespace {

using MetadataColumnMap = std::unordered_map<std::string_view, const SchemaField*>;

const MetadataColumnMap& GetMetadataColumnMap() {
  static const MetadataColumnMap kMetadataColumnMap = {
      {MetadataColumns::kFilePath.name(), &MetadataColumns::kFilePath},
      {MetadataColumns::kRowPosition.name(), &MetadataColumns::kRowPosition},
      {MetadataColumns::kIsDeleted.name(), &MetadataColumns::kIsDeleted},
      {MetadataColumns::kSpecId.name(), &MetadataColumns::kSpecId},
      {MetadataColumns::kRowId.name(), &MetadataColumns::kRowId},
      {MetadataColumns::kLastUpdatedSequenceNumber.name(),
       &MetadataColumns::kLastUpdatedSequenceNumber}};
  return kMetadataColumnMap;
}

const std::set<int32_t>& GetMetadataFieldIdSet() {
  static const std::set<int32_t> kMetadataFieldIds = {
      MetadataColumns::kFilePath.field_id(),
      MetadataColumns::kRowPosition.field_id(),
      MetadataColumns::kIsDeleted.field_id(),
      MetadataColumns::kSpecId.field_id(),
      MetadataColumns::kPartitionColumnId,
      MetadataColumns::kRowId.field_id(),
      MetadataColumns::kLastUpdatedSequenceNumber.field_id()};
  return kMetadataFieldIds;
}

}  // namespace

const std::set<int32_t>& MetadataColumns::MetadataFieldIds() {
  return GetMetadataFieldIdSet();
}

bool MetadataColumns::IsMetadataColumn(std::string_view name) {
  return name == kPartitionColumnName ||
         GetMetadataColumnMap().find(name) != GetMetadataColumnMap().end();
}

bool MetadataColumns::IsMetadataColumn(int32_t id) {
  return GetMetadataFieldIdSet().find(id) != GetMetadataFieldIdSet().end();
}

Result<const SchemaField*> MetadataColumns::MetadataColumn(std::string_view name) {
  const auto& metadata_column_map = GetMetadataColumnMap();
  const auto it = metadata_column_map.find(name);
  if (it == metadata_column_map.cend()) {
    return InvalidArgument("Unknown metadata column: {}", name);
  }
  return it->second;
}

}  // namespace iceberg
