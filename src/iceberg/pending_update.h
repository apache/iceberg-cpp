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

/// \file iceberg/pending_update.h
/// API for table changes using builder pattern

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

namespace iceberg {

/// \brief Base class for table metadata changes using builder pattern
///
/// This base class allows storing different types of PendingUpdate operations
/// in the same collection (e.g., in Transaction). It provides the common Commit()
/// interface that all updates share.
///
/// This matches the Java Iceberg pattern where BaseTransaction stores a
/// List<PendingUpdate> without type parameters.
class ICEBERG_EXPORT PendingUpdate : public ErrorCollector {
 public:
  virtual ~PendingUpdate() = default;

  /// \brief Verify that the changes are valid and apply them.
  /// \return Status::OK if the changes are valid, or an error:
  ///         - ValidationFailed: if pending changes cannot be applied
  ///         - InvalidArgument: if pending changes are conflicting
  virtual Status Apply() = 0;

  /// \brief Apply and commit the pending changes to the table
  ///
  /// Changes are committed by calling the underlying table's commit operation.
  ///
  /// Once the commit is successful, the updated table will be refreshed.
  ///
  /// \return Status::OK if the commit was successful, or an error:
  ///         - ValidationFailed: if update cannot be applied to current metadata
  ///         - CommitFailed: if update cannot be committed due to conflicts
  ///         - CommitStateUnknown: if commit success state is unknown
  virtual Status Commit() = 0;

  // Non-copyable, movable
  PendingUpdate(const PendingUpdate&) = delete;
  PendingUpdate& operator=(const PendingUpdate&) = delete;
  PendingUpdate(PendingUpdate&&) noexcept = default;
  PendingUpdate& operator=(PendingUpdate&&) noexcept = default;

 protected:
  PendingUpdate() = default;
};

}  // namespace iceberg
