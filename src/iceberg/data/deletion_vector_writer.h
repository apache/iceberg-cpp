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
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/data/writer.h"
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
  /// Partition spec the referenced data files belong to (optional).
  std::shared_ptr<PartitionSpec> spec;
  /// Partition the referenced data files belong to.
  PartitionValues partition;
  /// File-level Puffin properties (e.g. "created-by").
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Writes one or more deletion vectors into a single Puffin file.
///
/// Each referenced data file gets its own `deletion-vector-v1` blob. After
/// Close(), Metadata() returns one DataFile per blob, each carrying the
/// content_offset/content_size_in_bytes and referenced_data_file required to
/// register the deletion vector in a manifest.
///
/// \note All referenced data files are assumed to belong to the single
/// partition supplied in the options.
class ICEBERG_DATA_EXPORT DeletionVectorWriter {
 public:
  ~DeletionVectorWriter();

  DeletionVectorWriter(const DeletionVectorWriter&) = delete;
  DeletionVectorWriter& operator=(const DeletionVectorWriter&) = delete;

  /// \brief Create a new DeletionVectorWriter.
  static Result<std::unique_ptr<DeletionVectorWriter>> Make(
      DeletionVectorWriterOptions options);

  /// \brief Mark a row position as deleted for the given data file.
  ///
  /// Positions are accumulated per data file and serialized on Close().
  Status Delete(std::string_view referenced_data_file, int64_t pos);

  /// \brief Write all accumulated deletion vectors to the Puffin file and close it.
  Status Close();

  /// \brief Metadata for the DataFiles produced (one per referenced data file).
  /// \note Must be called after Close().
  Result<FileWriter::WriteResult> Metadata();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit DeletionVectorWriter(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg
