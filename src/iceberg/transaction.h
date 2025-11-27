
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
#include "iceberg/result.h"
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
  /// This method applies all pending data operations and metadata updates in the
  /// transaction and commits them to the table in a single atomic operation.
  ///
  /// \return Status::OK if the transaction was committed successfully, or an error
  ///         status if validation failed or the commit encountered conflicts
  virtual Status CommitTransaction() = 0;
};

}  // namespace iceberg
