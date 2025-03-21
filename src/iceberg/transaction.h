
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

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A transaction for performing multiple updates to a table
class ICEBERG_EXPORT Transaction {
 public:
  virtual ~Transaction() = default;

  /// \brief Return the Table that this transaction will update
  ///
  /// \return this transaction's table
  virtual const std::shared_ptr<Table>& table() const = 0;

  /// \brief Create a new append API to add files to this table
  ///
  /// \return a new AppendFiles
  virtual std::shared_ptr<AppendFiles> NewAppend() = 0;

  /// \brief Apply the pending changes from all actions and commit
  ///
  /// May throw ValidationException if any update cannot be applied to the current table
  /// metadata. May throw CommitFailedException if the updates cannot be committed due to
  /// conflicts.
  virtual void CommitTransaction() = 0;
};

}  // namespace iceberg
