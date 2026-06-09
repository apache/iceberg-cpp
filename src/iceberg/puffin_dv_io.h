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

/// \file iceberg/puffin_dv_io.h
/// Deletion vector merge hooks.

#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

struct ICEBERG_EXPORT DeletionVectorMergeGroup {
  std::string referenced_data_file;
  std::vector<std::shared_ptr<DataFile>> delete_files;
  std::shared_ptr<PartitionSpec> spec;
  PartitionValues partition;
};

class ICEBERG_EXPORT PuffinDVIO {
 public:
  virtual ~PuffinDVIO();

  virtual Result<std::vector<std::shared_ptr<DataFile>>> MergeAndWriteDVs(
      std::span<const DeletionVectorMergeGroup> groups, std::string_view output_path,
      const std::shared_ptr<FileIO>& io) = 0;
};

using PuffinDVIOFactory = std::function<Result<std::shared_ptr<PuffinDVIO>>()>;

struct ICEBERG_EXPORT PuffinDVIORegistry {
  explicit PuffinDVIORegistry(PuffinDVIOFactory factory);

  static PuffinDVIOFactory& GetFactory();

  static Result<std::vector<std::shared_ptr<DataFile>>> MergeAndWriteDVs(
      std::span<const DeletionVectorMergeGroup> groups, std::string_view output_path,
      const std::shared_ptr<FileIO>& io);
};

}  // namespace iceberg
