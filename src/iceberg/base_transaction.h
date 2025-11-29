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

#include <vector>

#include "iceberg/transaction.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base class for transaction implementations
class BaseTransaction : public Transaction {
 public:
  BaseTransaction(std::shared_ptr<const Table> table, std::shared_ptr<Catalog> catalog);
  ~BaseTransaction() override = default;

  const std::shared_ptr<const Table>& table() const override;

  std::shared_ptr<PropertiesUpdate> UpdateProperties() override;

  std::shared_ptr<AppendFiles> NewAppend() override;

  Status CommitTransaction() override;

 protected:
  template <typename UpdateType, typename... Args>
  std::shared_ptr<UpdateType> RegisterUpdate(Args&&... args) {
    auto update = std::make_shared<UpdateType>(std::forward<Args>(args)...);
    pending_updates_.push_back(update);
    return update;
  }

  std::shared_ptr<const Table> table_;
  std::shared_ptr<Catalog> catalog_;
  std::vector<std::shared_ptr<PendingUpdate>> pending_updates_;
};

}  // namespace iceberg
