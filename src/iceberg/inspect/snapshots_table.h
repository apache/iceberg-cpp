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
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Snapshots metadata table.
class ICEBERG_EXPORT SnapshotsTable : public MetadataTable {
 public:
  static Result<std::unique_ptr<SnapshotsTable>> Make(std::shared_ptr<Table> table);

  ~SnapshotsTable() override;

  Kind kind() const noexcept override { return Kind::kSnapshots; }

 private:
  explicit SnapshotsTable(std::shared_ptr<Table> table);
};

}  // namespace iceberg
