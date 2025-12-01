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

/// \file iceberg/update/update_properties.h
/// Pending update for modifying table properties.

#include <string>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/file_format.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"

namespace iceberg {

class Table;

/// \brief Pending update for table properties.
class ICEBERG_EXPORT PropertiesUpdate
    : public PendingUpdateTyped<std::unordered_map<std::string, std::string>> {
 public:
  explicit PropertiesUpdate(Table* table);

  /// \brief Set or update a property value.
  PropertiesUpdate& Set(std::string key, std::string value);

  /// \brief Remove a property key.
  PropertiesUpdate& Remove(std::string key);

  /// \brief Set the default data file format.
  PropertiesUpdate& DefaultFormat(FileFormatType format);

  Result<std::unordered_map<std::string, std::string>> Apply() override;

  Status Commit() override;

 private:
  Table* table_;  // non-owning
  std::unordered_map<std::string, std::string> updates_;
  std::unordered_set<std::string> removals_;
};

}  // namespace iceberg
