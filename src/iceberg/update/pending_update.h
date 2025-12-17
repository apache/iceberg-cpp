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

/// \file iceberg/update/pending_update.h
/// API for table changes using builder pattern

#include <memory>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

namespace iceberg {

/// \brief Base class for all kinds of table metadata updates.
///
/// Any created `PendingUpdate` instance is tracked by the `Transaction` instance
/// and commit is also delegated to the `Transaction` instance.
///
/// \note Implementations are expected to use builder pattern and errors
/// should be handled by the ErrorCollector base class.
class ICEBERG_EXPORT PendingUpdate : public ErrorCollector {
 public:
  enum class Kind : uint8_t {
    kUpdateProperties,
  };

  /// \brief Return the kind of this pending update.
  virtual Kind kind() const = 0;

  struct ApplyResult {
    std::vector<std::unique_ptr<TableUpdate>> updates;
  };

  /// \brief Apply the pending changes and return the uncommitted changes for validation.
  ///
  /// \note This does not result in a permanent update.
  /// \return The uncommitted changes that would be committed by calling Commit(), or an
  /// error:
  ///         - ValidationFailed: the pending changes cannot be applied to the current
  ///         metadata
  ///         - InvalidArgument: if pending changes are conflicting or invalid
  virtual Result<ApplyResult> Apply() = 0;

  /// \brief Apply the pending changes and commit.
  ///
  /// \return An OK status if the commit was successful, or an error:
  ///         - ValidationFailed: if it cannot be applied to the current table metadata.
  ///         - CommitFailed: if it cannot be committed due to conflicts.
  ///         - CommitStateUnknown: unknown status, no cleanup should be done.
  virtual Status Commit();

  // Non-copyable, movable
  PendingUpdate(const PendingUpdate&) = delete;
  PendingUpdate& operator=(const PendingUpdate&) = delete;
  PendingUpdate(PendingUpdate&&) noexcept = default;
  PendingUpdate& operator=(PendingUpdate&&) noexcept = default;

  ~PendingUpdate() override;

 protected:
  explicit PendingUpdate(std::shared_ptr<Transaction> transaction);

  std::shared_ptr<Transaction> transaction_;
};

}  // namespace iceberg
