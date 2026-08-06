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

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "iceberg/inspect/metadata_table.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {
class ArrowRowBuilder;
}

namespace iceberg::internal {

struct LiveFile {
  std::shared_ptr<DataFile> file;
  std::shared_ptr<PartitionSpec> spec;
  std::optional<int64_t> snapshot_id;
};

/// \brief Resolve a time-travel selection to a snapshot.
Result<std::shared_ptr<Snapshot>> ResolveMetadataTableSnapshot(
    const Table& table, const SnapshotSelection& selection);

/// \brief Build the Java-compatible union of active partition fields across all specs.
Result<std::shared_ptr<StructType>> UnifiedPartitionType(const Table& table);

/// \brief Build the files metadata table schema for a table.
Result<std::shared_ptr<Schema>> FilesTableSchema(
    const Schema& table_schema, const std::shared_ptr<StructType>& partition_type);

/// \brief Project values written with one spec into the table-wide partition type.
Result<PartitionValues> ProjectPartitionValues(const StructType& partition_type,
                                               const PartitionSpec& spec,
                                               const PartitionValues& values);

/// \brief Append partition values to an Arrow struct builder.
Status AppendPartitionValues(ArrowArray* array, const StructType& partition_type,
                             const PartitionValues& values);

/// \brief Append a data-file row using the files metadata table schema.
Status AppendDataFile(ArrowRowBuilder& builder, const Schema& schema,
                      const Schema& table_schema, const StructType& partition_type,
                      const LiveFile& live_file);

/// \brief Read all live files in the selected snapshot.
Result<std::vector<LiveFile>> LoadLiveFiles(const Table& table,
                                            const std::shared_ptr<Snapshot>& snapshot);

}  // namespace iceberg::internal
