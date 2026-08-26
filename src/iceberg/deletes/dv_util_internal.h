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

/// \file iceberg/deletes/dv_util_internal.h
/// Internal deletion vector helpers.

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

struct DeletionVectorMergeGroup {
  std::string referenced_data_file;
  std::vector<std::shared_ptr<DataFile>> delete_files;
  std::shared_ptr<PartitionSpec> spec;
  PartitionValues partition;
};

class ICEBERG_EXPORT DVUtil {
 public:
  static Result<std::vector<std::shared_ptr<DataFile>>> MergeAndWriteDVs(
      std::span<const DeletionVectorMergeGroup> groups, std::string_view output_path,
      const std::shared_ptr<FileIO>& io);

  static Result<PositionDeleteIndex> ReadDV(const std::shared_ptr<DataFile>& delete_file,
                                            const std::shared_ptr<FileIO>& io);

  static Result<puffin::BlobMetadata> WriteDVBlob(puffin::PuffinWriter& writer,
                                                  std::string_view referenced_data_file,
                                                  PositionDeleteIndex& positions);

  static Result<std::shared_ptr<DataFile>> MakeDVDataFile(
      std::string_view path, int64_t file_size, std::string_view referenced_data_file,
      const PartitionValues& partition, int64_t cardinality,
      const std::shared_ptr<PartitionSpec>& spec,
      const puffin::BlobMetadata& blob_metadata);
};

}  // namespace iceberg
