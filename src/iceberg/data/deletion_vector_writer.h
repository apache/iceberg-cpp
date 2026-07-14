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

/// \file iceberg/data/deletion_vector_writer.h
/// Writer that emits deletion vectors as `deletion-vector-v1` blobs in a Puffin file.

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "iceberg/data/writer.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Options for creating a DeletionVectorWriter.
struct ICEBERG_DATA_EXPORT DeletionVectorWriterOptions {
  std::string path;
  std::shared_ptr<FileIO> io;
  /// Loads existing deletes for a data file to merge, if any.
  std::function<Result<std::optional<PositionDeleteIndex>>(std::string_view)>
      load_previous_deletes;
};

/// \brief A deletion vector file writer.
class ICEBERG_DATA_EXPORT DeletionVectorWriter {
 public:
  ~DeletionVectorWriter();

  /// \brief Create a new DeletionVectorWriter.
  static Result<std::unique_ptr<DeletionVectorWriter>> Make(
      DeletionVectorWriterOptions options);

  /// \brief Mark a row position as deleted for the given data file.
  Status Delete(std::string_view referenced_data_file, int64_t pos,
                const std::shared_ptr<PartitionSpec>& spec,
                const PartitionValues& partition);

  /// \brief Mark all positions in the given index as deleted for a data file.
  Status Delete(std::string_view referenced_data_file,
                const PositionDeleteIndex& positions,
                const std::shared_ptr<PartitionSpec>& spec,
                const PartitionValues& partition);

  /// \brief Write all accumulated deletion vectors to the Puffin file and close.
  Status Close();

  /// \brief The result of writing; valid only after Close().
  Result<WriteResult> Metadata();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit DeletionVectorWriter(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg
