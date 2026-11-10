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

/// \file iceberg/data/position_delete_update.h
/// Table-aware position delete writing and commit.

#include <cstdint>
#include <memory>
#include <string_view>

#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

namespace iceberg {

/// \brief Writes and commits position deletes using the table format.
///
/// Format v2 tables produce Parquet position delete files. Format v3 tables
/// produce deletion vectors and merge previous file-scoped position deletes.
class ICEBERG_DATA_EXPORT PositionDeleteUpdate : public ErrorCollector {
 public:
  ~PositionDeleteUpdate() override;

  /// \brief Create a position delete update for a table.
  static Result<std::unique_ptr<PositionDeleteUpdate>> Make(std::shared_ptr<Table> table);

  /// \brief Add a deleted row position for a live data file.
  PositionDeleteUpdate& Delete(std::string_view data_file_path, int64_t pos);

  /// \brief Write and commit all added position deletes.
  Status Commit();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit PositionDeleteUpdate(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg
