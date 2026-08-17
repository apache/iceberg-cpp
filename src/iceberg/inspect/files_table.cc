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

#include "iceberg/inspect/files_table.h"

#include <memory>
#include <utility>

#include "iceberg/inspect/metadata_table_stream_internal.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

FilesTable::FilesTable(std::shared_ptr<Table> table, std::shared_ptr<Schema> schema,
                       std::shared_ptr<Schema> table_schema,
                       std::shared_ptr<StructType> partition_type)
    : TimeTravelMetadataTable(std::move(table)),
      schema_(std::move(schema)),
      table_schema_(std::move(table_schema)),
      partition_type_(std::move(partition_type)) {}

FilesTable::~FilesTable() = default;

const std::shared_ptr<Schema>& FilesTable::schema() const { return schema_; }

Result<std::unique_ptr<FilesTable>> FilesTable::Make(std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  ICEBERG_ASSIGN_OR_RAISE(auto table_schema, table->schema());
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, internal::UnifiedPartitionType(*table));
  ICEBERG_ASSIGN_OR_RAISE(auto schema,
                          internal::FilesTableSchema(*table_schema, partition_type));
  return std::unique_ptr<FilesTable>(new FilesTable(std::move(table), std::move(schema),
                                                    std::move(table_schema),
                                                    std::move(partition_type)));
}

Result<ArrowArrayStream> FilesTable::ScanSnapshot(
    const SnapshotSelection& snapshot_selection) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, internal::ResolveMetadataTableSnapshot(
                                             *source_table(), snapshot_selection));
  ICEBERG_ASSIGN_OR_RAISE(auto files, internal::LoadLiveFiles(*source_table(), snapshot));
  auto schema = schema_;
  auto table_schema = table_schema_;
  auto partition_type = partition_type_;
  return internal::MakeMetadataTableStream(
      *schema_, std::move(files),
      [schema = std::move(schema), table_schema = std::move(table_schema),
       partition_type = std::move(partition_type)](ArrowRowBuilder& builder,
                                                   const internal::LiveFile& file) {
        return internal::AppendDataFile(builder, *schema, *table_schema, *partition_type,
                                        file);
      });
}

}  // namespace iceberg
