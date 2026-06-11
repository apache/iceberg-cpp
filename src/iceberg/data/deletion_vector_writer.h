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
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/data/writer.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Options for creating a DeletionVectorWriter.
struct ICEBERG_DATA_EXPORT DeletionVectorWriterOptions {
  /// Output Puffin file location.
  std::string path;
  /// FileIO used to create the Puffin file.
  std::shared_ptr<FileIO> io;
  /// File-level Puffin properties (e.g. "created-by").
  std::unordered_map<std::string, std::string> properties;
  /// Hook to load previously written deletes for a data file so they can be
  /// merged into the new deletion vector. The returned index carries its source
  /// delete files (via PositionDeleteIndex::delete_files()); file-scoped ones
  /// are reported as rewritten. Return nullptr when there are none. Required: a
  /// DV must replace any existing deletes for its data file.
  std::function<Result<std::shared_ptr<PositionDeleteIndex>>(std::string_view)>
      load_previous_deletes;
};

/// \brief Writes one or more deletion vectors into a single Puffin file.
///
/// Each referenced data file gets its own `deletion-vector-v1` blob with its own
/// partition spec and partition. After Close(), Metadata() returns the produced
/// delete files plus the referenced and rewritten delete files.
class ICEBERG_DATA_EXPORT DeletionVectorWriter {
 public:
  ~DeletionVectorWriter();

  DeletionVectorWriter(const DeletionVectorWriter&) = delete;
  DeletionVectorWriter& operator=(const DeletionVectorWriter&) = delete;

  /// \brief Create a new DeletionVectorWriter.
  static Result<std::unique_ptr<DeletionVectorWriter>> Make(
      DeletionVectorWriterOptions options);

  /// \brief Mark a row position as deleted for the given data file.
  Status Delete(std::string_view referenced_data_file, int64_t pos,
                std::shared_ptr<PartitionSpec> spec, PartitionValues partition);

  /// \brief Mark all positions in the given index as deleted for a data file.
  Status Delete(std::string_view referenced_data_file,
                const PositionDeleteIndex& positions, std::shared_ptr<PartitionSpec> spec,
                PartitionValues partition);

  /// \brief Write all accumulated deletion vectors to the Puffin file and close.
  Status Close();

  /// \brief The result of writing; valid only after Close().
  Result<DeleteWriteResult> Metadata();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit DeletionVectorWriter(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg
