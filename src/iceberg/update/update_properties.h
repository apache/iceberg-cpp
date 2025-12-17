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
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/iceberg_export.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

namespace iceberg {

/// \brief Updates table properties.
class ICEBERG_EXPORT UpdateProperties : public PendingUpdate {
 public:
  static Result<std::shared_ptr<UpdateProperties>> Make(
      std::shared_ptr<Transaction> transaction);

  ~UpdateProperties() override;

  /// \brief Sets a property key to a specified value.
  ///
  /// The key can not be marked for previous removal and reserved property keys will be
  /// ignored.
  UpdateProperties& Set(const std::string& key, const std::string& value);

  /// \brief Marks a property for removal.
  ///
  /// The key can not be already marked for removal.
  ///
  /// \param key The property key to remove
  /// \return Reference to this UpdateProperties for chaining
  UpdateProperties& Remove(const std::string& key);

  Kind kind() const final { return Kind::kUpdateProperties; }

  Result<ApplyResult> Apply() final;

 private:
  explicit UpdateProperties(std::shared_ptr<Transaction> transaction);

  std::unordered_map<std::string, std::string> updates_;
  std::unordered_set<std::string> removals_;
  std::optional<int8_t> format_version_;
};

}  // namespace iceberg
