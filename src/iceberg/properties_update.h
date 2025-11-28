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
#include <vector>

#include "iceberg/file_format.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class ICEBERG_EXPORT PropertiesUpdate : public PendingUpdateTyped<void> {
 public:
  /// \brief Constructs a PropertiesUpdate for the specified table.
  ///
  /// \param identifier The table identifier
  /// \param catalog The catalog containing the table
  /// \param metadata The current table metadata
  PropertiesUpdate(TableIdentifier identifier, std::shared_ptr<Catalog> catalog,
                   std::shared_ptr<TableMetadata> metadata);

  /// \brief Sets a property key-value pair.
  ///
  /// If the key was previously marked for removal, this operation cancels the removal.
  /// Reserved properties are handled according to TableProperties rules.
  ///
  /// \param key The property key
  /// \param value The property value
  /// \return Reference to this PropertiesUpdate for chaining
  PropertiesUpdate& Set(std::string key, std::string value);

  /// \brief Marks a property for removal.
  ///
  /// If the key was previously set for update, this operation cancels the update.
  ///
  /// \param key The property key to remove
  /// \return Reference to this PropertiesUpdate for chaining
  PropertiesUpdate& Remove(std::string key);

  /// \brief Sets the default file format for the table.
  ///
  /// This is a convenience method for setting the "write.format.default" property.
  ///
  /// \param format The file format type to use as default
  /// \return Reference to this PropertiesUpdate for chaining
  PropertiesUpdate& DefaultFormat(FileFormatType format);

  /// \brief Applies the property changes without committing them.
  ///
  /// Validates the pending property changes but does not commit them to the table.
  /// This method can be used to validate changes before actually committing them.
  ///
  /// \return Status::OK if the changes are valid, or an error if validation fails
  Result<void> Apply() override;

  /// \brief Commits the property changes to the table.
  ///
  /// Validates the changes and applies them to the table through the catalog.
  ///
  /// \return OK if the changes are valid and committed successfully, or an error
  Status Commit() override;

 private:
  TableIdentifier identifier_;
  std::shared_ptr<Catalog> catalog_;
  std::shared_ptr<TableMetadata> metadata_;

  std::unordered_map<std::string, std::string> updates_;
  std::vector<std::string> removals_;
  std::optional<int8_t> format_version_;
};

}  // namespace iceberg
