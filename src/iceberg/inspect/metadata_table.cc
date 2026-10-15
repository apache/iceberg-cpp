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

#include "iceberg/inspect/metadata_table.h"

#include <memory>
#include <utility>

#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

Result<std::shared_ptr<Schema>> SelectColumns(const std::shared_ptr<Schema>& schema,
                                              const ColumnSelection& columns) {
  if (!columns.names.has_value()) {
    return schema;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto projected_schema,
                          schema->Select(*columns.names, columns.case_sensitive));
  return std::shared_ptr<Schema>(std::move(projected_schema));
}

}  // namespace

MetadataTable::MetadataTable(std::shared_ptr<Table> source_table)
    : source_table_(std::move(source_table)) {}

MetadataTable::~MetadataTable() = default;

bool MetadataTable::supports_time_travel() const noexcept { return false; }

const std::shared_ptr<Table>& MetadataTable::source_table() const {
  return source_table_;
}

Result<ArrowArrayStream> MetadataTable::Scan() { return ScanProjection(*schema()); }

Result<ArrowArrayStream> MetadataTable::ScanColumns(const ColumnSelection& columns) {
  ICEBERG_ASSIGN_OR_RAISE(auto projected_schema, SelectColumns(schema(), columns));
  return ScanProjection(*projected_schema);
}

TimeTravelMetadataTable::TimeTravelMetadataTable(std::shared_ptr<Table> source_table)
    : MetadataTable(std::move(source_table)) {}

TimeTravelMetadataTable::~TimeTravelMetadataTable() = default;

bool TimeTravelMetadataTable::supports_time_travel() const noexcept { return true; }

Result<ArrowArrayStream> TimeTravelMetadataTable::Scan(
    const SnapshotSelection& snapshot_selection) {
  return ScanSnapshot(snapshot_selection, *schema());
}

Result<ArrowArrayStream> TimeTravelMetadataTable::ScanColumns(
    const SnapshotSelection& snapshot_selection, const ColumnSelection& columns) {
  ICEBERG_ASSIGN_OR_RAISE(auto projected_schema, SelectColumns(schema(), columns));
  return ScanSnapshot(snapshot_selection, *projected_schema);
}

Result<ArrowArrayStream> TimeTravelMetadataTable::ScanProjection(
    const Schema& projected_schema) {
  return ScanSnapshot(SnapshotSelection{}, projected_schema);
}

}  // namespace iceberg
