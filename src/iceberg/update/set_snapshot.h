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

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

/// \file iceberg/update/set_snapshot.h
/// \brief Sets the current snapshot directly or by rolling back.

namespace iceberg {

/// \brief Sets the current snapshot directly or by rolling back.
class ICEBERG_EXPORT SetSnapshot : public PendingUpdate {
 public:
  static Result<std::shared_ptr<SetSnapshot>> Make(
      std::shared_ptr<Transaction> transaction);

  ~SetSnapshot() override;

  /// \brief Sets the table's current state to a specific Snapshot identified by id.
  SetSnapshot& SetCurrentSnapshot(int64_t snapshot_id);

  /// \brief Rolls back the table's state to the last Snapshot before the given timestamp.
  SetSnapshot& RollbackToTime(int64_t timestamp_ms);

  /// \brief Rollback table's state to a specific Snapshot identified by id.
  SetSnapshot& RollbackTo(int64_t snapshot_id);

  Kind kind() const final { return Kind::kSetSnapshot; }

  /// \brief Apply the pending changes and return the target snapshot ID.
  Result<int64_t> Apply();

 private:
  explicit SetSnapshot(std::shared_ptr<Transaction> transaction);

  /// \brief Find the latest snapshot whose timestamp is before the provided timestamp.
  ///
  /// \param timestamp_ms Lookup snapshots before this timestamp
  /// \return The snapshot ID that was current at the given timestamp, or nullopt
  Result<std::optional<int64_t>> FindLatestAncestorOlderThan(int64_t timestamp_ms) const;

  std::optional<int64_t> target_snapshot_id_;
  bool is_rollback_{false};
};

}  // namespace iceberg
