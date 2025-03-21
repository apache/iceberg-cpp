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

#include <string>

#include "iceberg/expected.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Interface for table operations including metadata refresh and commit
class ICEBERG_EXPORT TableOperations {
 public:
  virtual ~TableOperations() = default;

  /// \brief Return the currently loaded table metadata, without checking for updates.
  virtual std::shared_ptr<TableMetadata> Current() = 0;

  /// \brief Return the current table metadata after checking for updates.
  virtual std::shared_ptr<TableMetadata> Refresh() = 0;

  /// \brief Replace the base table metadata with a new version.
  ///
  /// This method should implement and document atomicity guarantees.
  ///
  /// Implementations must check that the base metadata is current to avoid overwriting
  /// updates. Once the atomic commit operation succeeds, implementations must not perform
  /// any operations that may fail because failure in this method cannot be distinguished
  /// from commit failure.
  ///
  /// Implementations must return Error::kCommitStateUnknown in cases where it cannot be
  /// determined if the commit succeeded or failed. For example if a network partition
  /// causes the confirmation of the commit to be lost, the implementation should return
  /// Error::kCommitStateUnknown. This is important because downstream users of this API
  /// need to know whether they can clean up the commit or not, if the state is unknown
  /// then it is not safe to remove any files. All other exceptions will be treated as if
  /// the commit has failed.
  ///
  /// \param base table metadata on which changes were based
  /// \param metadata new table metadata with updates
  virtual expected<void, Error> Commit(const TableMetadata& base,
                                       const TableMetadata& metadata) = 0;

  /// \brief Returns an FileIO to read and write table data and metadata files.
  // virtual std::shared_ptr<FileIO> io() const = 0;

  /// \brief Given the name of a metadata file, obtain the full path of that file using an
  /// appropriate base location of the implementation's choosing.
  virtual std::string MetadataFileLocation(const std::string& fileName) = 0;

  /// \brief Returns a LocationProvider that supplies locations for new new data files.
  ///
  /// \return a location provider configured for the current table state
  virtual std::shared_ptr<LocationProvider> location_provider() const = 0;

  /// \brief Return a temporary TableOperations instance that uses configuration from
  /// uncommitted metadata.
  ///
  /// This is called by transactions when uncommitted table metadata should be used; for
  /// example, to create a metadata file location based on metadata in the transaction
  /// that has not been committed.
  ///
  /// Transactions will not call `Refresh()` or `Commit()`.
  ///
  /// \param uncommittedMetadata uncommitted table metadata
  /// \return a temporary table operations that behaves like the uncommitted metadata is
  /// current
  virtual std::unique_ptr<TableOperations> Temp(
      const TableMetadata& uncommittedMetadata) = 0;

  /// \brief Create a new ID for a Snapshot
  virtual int64_t NewSnapshotId() = 0;
};

}  // namespace iceberg
