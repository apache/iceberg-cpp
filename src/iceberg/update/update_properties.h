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
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/file_format.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Updates table properties.
class ICEBERG_EXPORT UpdateProperties : public PendingUpdate {
 public:
  /// \brief Constructs a UpdateProperties for the specified table.
  ///
  /// \param identifier The table identifier
  /// \param catalog The catalog containing the table
  /// \param metadata The current table metadata
  UpdateProperties(TableIdentifier identifier, std::shared_ptr<Catalog> catalog,
                   std::shared_ptr<TableMetadata> base);

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

  /// \brief Applies the property changes without committing them.
  ///
  /// Validates the pending property changes but does not commit them to the table.
  /// This method can be used to validate changes before actually committing them.
  ///
  /// \return Status::OK if the changes are valid, or an error if validation fails
  Status Apply() override;

  /// \brief Commits the property changes to the table.
  ///
  /// Validates the changes and applies them to the table through the catalog.
  ///
  /// \return OK if the changes are valid and committed successfully, or an error
  Status Commit() override;

 private:
  TableIdentifier identifier_;
  std::shared_ptr<Catalog> catalog_;
  std::shared_ptr<TableMetadata> base_metadata_;

  std::unordered_map<std::string, std::string> updates_;
  std::unordered_set<std::string> removals_;
  std::optional<int8_t> format_version_;
};

}  // namespace iceberg
