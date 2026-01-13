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
#include <string_view>

#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

/// \file iceberg/update/update_location.h
/// \brief Updates the table location.

namespace iceberg {

/// \brief Updating table location with a new base location.
class ICEBERG_EXPORT UpdateLocation : public PendingUpdate {
 public:
  static Result<std::shared_ptr<UpdateLocation>> Make(
      std::shared_ptr<Transaction> transaction);

  ~UpdateLocation() override;

  /// \brief Sets the new location for the table.
  ///
  /// \param location The new table location
  /// \return Reference to this for method chaining
  UpdateLocation& SetLocation(std::string_view location);

  Kind kind() const final { return Kind::kUpdateLocation; }

  /// \brief Apply the pending changes and return the new location.
  Result<std::string> Apply();

 private:
  explicit UpdateLocation(std::shared_ptr<Transaction> transaction);

  std::string location_;
};

}  // namespace iceberg
