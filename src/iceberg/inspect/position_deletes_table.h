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

/// \file iceberg/inspect/position_deletes_table.h
/// \brief Define the position_deletes metadata table.

#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Metadata table that expands physical position-delete storage into rows.
class ICEBERG_EXPORT PositionDeletesTable : public MetadataTable {
 public:
  /// \brief Create a position_deletes metadata table for a source table.
  /// \param table Source table whose current snapshot will be inspected.
  /// \return A metadata table or an error if its schema cannot be constructed.
  static Result<std::unique_ptr<PositionDeletesTable>> Make(std::shared_ptr<Table> table);

  ~PositionDeletesTable() override;

  Kind kind() const noexcept override { return Kind::kPositionDeletes; }

  const std::shared_ptr<Schema>& schema() const override { return schema_; }

  /// \brief Materialize current position deletes as an Arrow stream.
  /// Recreate this metadata table if the source schema, format, or partition specs
  /// change.
  Result<ArrowArrayStream> Scan() override;

  /// \brief Scan with a projection of complete top-level fields.
  Result<ArrowArrayStream> Scan(const Schema& projected_schema);

 private:
  PositionDeletesTable(
      std::shared_ptr<Table> table, std::shared_ptr<Schema> schema,
      std::shared_ptr<StructType> partition_type,
      std::unordered_map<int32_t, std::vector<std::optional<size_t>>> partition_mappings);

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<const TableMetadata> source_metadata_;
  std::shared_ptr<StructType> partition_type_;
  std::unordered_map<int32_t, std::vector<std::optional<size_t>>> partition_mappings_;
};

}  // namespace iceberg
