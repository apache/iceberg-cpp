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
#include <string>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_scan.h"
#include "iceberg/inspect/metadata_scan_task.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/stream.h"

namespace iceberg {
class ArrowRowBuilder;
}

namespace iceberg::internal {

/// A task retaining the captured metadata, projection, and manifest IO.
class SnapshotMetadataTask : public MetadataScanTask {
 public:
  using ReadRowsFunction = Result<ArrowArrayStream> (*)(const MetadataScanContext&,
                                                        const std::shared_ptr<FileIO>&,
                                                        int64_t);

  SnapshotMetadataTask(MetadataScanContext context, ReadRowsFunction read_rows)
      : context(std::move(context)), read_rows_(read_rows) {}

  Result<ArrowArrayStream> Open(int64_t batch_size) const {
    return read_rows_(context, context.io, batch_size);
  }

  TaskKind task_kind() const override { return TaskKind::kSnapshot; }
  MetadataTableKind table_kind() const override { return context.kind; }

  const MetadataScanContext context;

 private:
  ReadRowsFunction read_rows_;
};

ICEBERG_EXPORT Result<std::unique_ptr<MetadataScan>> MakeSnapshotMetadataScan(
    MetadataScanContext context, SnapshotMetadataTask::ReadRowsFunction read_rows);

struct LiveFile {
  std::shared_ptr<DataFile> file;
  std::shared_ptr<PartitionSpec> spec;
  std::optional<int64_t> snapshot_id;
};

/// \brief Resolve a time-travel selection to a snapshot.
ICEBERG_EXPORT Result<std::shared_ptr<Snapshot>> ResolveMetadataTableSnapshot(
    const TableMetadata& metadata, const MetadataScanContext& selection);

/// \brief Build the Java-compatible union of active partition fields across all specs.
ICEBERG_EXPORT Result<std::shared_ptr<StructType>> UnifiedPartitionType(
    const TableMetadata& metadata);

/// \brief Project values written with one spec into the table-wide partition type.
ICEBERG_EXPORT Result<PartitionValues> ProjectPartitionValues(
    const StructType& partition_type, const PartitionSpec& spec,
    const PartitionValues& values);

/// \brief Append a primitive partition value or decoded metric bound.
ICEBERG_EXPORT Status AppendLiteral(ArrowArray* array, const Literal& literal);

/// \brief Append partition values to an Arrow struct builder.
ICEBERG_EXPORT Status AppendPartitionValues(ArrowArray* array,
                                            const StructType& partition_type,
                                            const PartitionValues& values);

/// \brief Iterate over live files, reading one manifest at a time.
/// An empty column selection reads all data-file fields. Each manifest's entries
/// are materialized by ManifestReader, independently of the Arrow batch size.
ICEBERG_EXPORT Result<std::unique_ptr<Stream<LiveFile>>> LiveFiles(
    const TableMetadata& metadata, const std::shared_ptr<FileIO>& io,
    const std::shared_ptr<Snapshot>& snapshot, std::vector<std::string> columns = {});

}  // namespace iceberg::internal
