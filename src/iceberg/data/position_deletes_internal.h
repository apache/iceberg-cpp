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
#include <set>
#include <string>
#include <vector>

#include "iceberg/deletes/dv_writer.h"
#include "iceberg/iceberg_data_export.h"

namespace iceberg::internal {

// Group absolute row positions by data file using a task from the original scan.
struct FilePositionDeletes {
  std::shared_ptr<FileScanTask> task;
  std::vector<int64_t> positions;
};

// Write one group per target file. Read previous deletes with the planned scan's
// FileIO and write new outputs with the table's FileIO. The caller owns output_paths
// until commit and validates its RowDelta against the snapshot used for planning.
ICEBERG_DATA_EXPORT Result<DeleteWriteResult> WritePositionDeletes(
    const Table& table, const std::shared_ptr<FileIO>& scan_io,
    std::vector<FilePositionDeletes> files, std::set<std::string>& output_paths);

}  // namespace iceberg::internal
