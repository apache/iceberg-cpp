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

#include "iceberg/update/update_location.h"

#include <memory>
#include <string>
#include <string_view>

#include "iceberg/result.h"
#include "iceberg/transaction.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<UpdateLocation>> UpdateLocation::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create UpdateLocation without a transaction");
  return std::shared_ptr<UpdateLocation>(new UpdateLocation(std::move(transaction)));
}

UpdateLocation::UpdateLocation(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {}

UpdateLocation::~UpdateLocation() = default;

UpdateLocation& UpdateLocation::SetLocation(std::string_view location) {
  ICEBERG_BUILDER_CHECK(!location.empty(), "Location cannot be empty");
  location_ = std::string(location);
  return *this;
}

Result<std::string> UpdateLocation::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  if (location_.empty()) {
    return InvalidArgument("Location must be set before applying");
  }
  return location_;
}

}  // namespace iceberg
