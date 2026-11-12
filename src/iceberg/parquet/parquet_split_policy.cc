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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <parquet/file_reader.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/result.h"
#include "iceberg/scan_task_planning_internal.h"

namespace iceberg::parquet {

namespace {

// Manifest split offsets are usable when they are present, strictly increasing, and
// inside the file. Usable offsets are returned without reading the file.
Result<std::vector<int64_t>> ManifestSplitBoundaries(const DataFile& file) {
  std::vector<int64_t> boundaries;
  int64_t previous = 0;
  for (int64_t offset : file.split_offsets) {
    if (offset <= previous || offset >= file.file_size_in_bytes) {
      return std::vector<int64_t>{};
    }
    boundaries.push_back(offset);
    previous = offset;
  }
  return boundaries;
}

class ParquetSplitPolicy final : public internal::SplitPolicy {
 public:
  Result<std::vector<int64_t>> SplitBoundaries(const std::shared_ptr<FileIO>& io,
                                               const DataFile& file) override {
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_boundaries, ManifestSplitBoundaries(file));
    if (!manifest_boundaries.empty()) {
      return manifest_boundaries;
    }

    // Manifest offsets are missing or unusable: inspect the footer once and use the
    // canonical row-group starts. Boundaries that differ from read offsets cost
    // balance, not correctness, because reads assign row groups by midpoint.
    ICEBERG_ASSIGN_OR_RAISE(auto input, arrow::OpenArrowInputStream(io, file.file_path));
    auto reader = ::parquet::ParquetFileReader::Open(input);
    auto metadata = reader->metadata();
    std::vector<int64_t> boundaries;
    boundaries.reserve(metadata->num_row_groups());
    for (int i = 0; i < metadata->num_row_groups(); ++i) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto start, RowGroupStart(*metadata->RowGroup(i), file.file_size_in_bytes));
      boundaries.push_back(start);
    }
    return boundaries;
  }
};

}  // namespace

void RegisterSplitPolicy() {
  internal::SplitPolicyRegistry::Register(FileFormatType::kParquet,
                                          std::make_unique<ParquetSplitPolicy>());
}

}  // namespace iceberg::parquet
