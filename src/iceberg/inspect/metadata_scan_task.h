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

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base class for metadata table scan tasks.
///
/// Metadata tables expose table metadata (snapshots, history, etc.) as queryable
/// Arrow tables. A task is a unit of work opened by MetadataScanTaskReader.
class ICEBERG_EXPORT MetadataScanTask {
 public:
  /// \brief Task category, determines how the reader opens the task.
  enum class TaskKind {
    /// StaticMetadataTask reads in-memory table metadata (snapshots, history).
    kStatic,
  };

  virtual ~MetadataScanTask() = default;

  /// \brief Return how this task is read.
  virtual TaskKind task_kind() const = 0;

  /// \brief Return the metadata table whose rows this task produces.
  virtual MetadataTableKind table_kind() const = 0;
};

/// \brief Task for static metadata tables (snapshots, history).
///
/// Holds the metadata captured for a table scan; readers produce Arrow batches
/// on demand. The metadata and its nested objects must not be modified while
/// this task or its streams are in use.
class ICEBERG_EXPORT StaticMetadataTask : public MetadataScanTask {
 public:
  /// \brief Create a static metadata task.
  ///
  /// \param table_kind The metadata table kind (kSnapshots or kHistory)
  /// \param metadata The table metadata to scan
  /// \param projected_schema The planned output schema
  /// \return A task, or an error for null metadata or an unsupported table kind.
  static Result<std::shared_ptr<StaticMetadataTask>> Make(
      MetadataTableKind table_kind, std::shared_ptr<const TableMetadata> metadata,
      std::shared_ptr<Schema> projected_schema);

  TaskKind task_kind() const override { return TaskKind::kStatic; }

  MetadataTableKind table_kind() const override { return table_kind_; }

  /// \brief Return the captured metadata and its shared ownership.
  const std::shared_ptr<const TableMetadata>& metadata() const { return metadata_; }

  /// \brief Return the output schema captured when this task was planned.
  const std::shared_ptr<Schema>& projected_schema() const { return projected_schema_; }

 private:
  StaticMetadataTask(MetadataTableKind table_kind,
                     std::shared_ptr<const TableMetadata> metadata,
                     std::shared_ptr<Schema> projected_schema);

  const MetadataTableKind table_kind_;
  const std::shared_ptr<const TableMetadata> metadata_;
  const std::shared_ptr<Schema> projected_schema_;
};

}  // namespace iceberg
