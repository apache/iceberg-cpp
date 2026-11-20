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
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/stream.h"

namespace iceberg::internal {

struct LiveFile {
  std::shared_ptr<DataFile> file;
  std::shared_ptr<PartitionSpec> spec;
  std::optional<int64_t> snapshot_id;
};

/// \brief Iterate over live files, reading one manifest at a time.
/// An empty column selection reads all data-file fields. Each manifest's entries
/// are materialized by ManifestReader, independently of the Arrow batch size.
ICEBERG_EXPORT Result<std::unique_ptr<Stream<LiveFile>>> LiveFiles(
    const TableMetadata& metadata, const std::shared_ptr<FileIO>& io,
    const std::shared_ptr<Snapshot>& snapshot, std::vector<std::string> columns = {});

}  // namespace iceberg::internal
