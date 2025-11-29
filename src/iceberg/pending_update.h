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

/// \file iceberg/pending_update.h
/// API for table changes using builder pattern

#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief Base class for table metadata changes using builder pattern
///
/// This base class allows storing different types of PendingUpdate operations
/// in the same collection (e.g., in Transaction). It provides the common Commit()
/// interface that all updates share.
///
/// This matches the Java Iceberg pattern where BaseTransaction stores a
/// List<PendingUpdate> without type parameters.
class ICEBERG_EXPORT PendingUpdate {
 public:
  virtual ~PendingUpdate() = default;

  /// \brief Apply and commit the pending changes to the table
  ///
  /// Changes are committed by calling the underlying table's commit operation.
  ///
  /// Once the commit is successful, the updated table will be refreshed.
  ///
  /// \return Status::OK if the commit was successful, or an error:
  ///         - ValidationFailed: if update cannot be applied to current metadata
  ///         - CommitFailed: if update cannot be committed due to conflicts
  ///         - CommitStateUnknown: if commit success state is unknown
  virtual Status Commit() = 0;

  // Non-copyable, movable
  PendingUpdate(const PendingUpdate&) = delete;
  PendingUpdate& operator=(const PendingUpdate&) = delete;
  PendingUpdate(PendingUpdate&&) noexcept = default;
  PendingUpdate& operator=(PendingUpdate&&) noexcept = default;

 protected:
  PendingUpdate() = default;

  /// \brief Apply the pending changes to a TableMetadataBuilder
  ///
  /// This method applies the changes by calling builder's specific methods.
  /// The builder will automatically record corresponding TableUpdate objects.
  ///
  /// \param builder The TableMetadataBuilder to apply changes to
  /// \return Status::OK if the changes were applied successfully, or an error
  virtual Status Apply(TableMetadataBuilder& builder) = 0;

  friend class BaseTransaction;
};

/// \brief Template class for type-safe table metadata changes using builder pattern
///
/// PendingUpdateTyped extends PendingUpdate with a type-safe Apply() method that
/// returns the specific result type for each operation. Subclasses implement
/// specific types of table updates such as schema changes, property updates, or
/// snapshot-producing operations like appends and deletes.
///
/// Apply() can be used to validate and inspect the uncommitted changes before
/// committing. Commit() applies the changes and commits them to the table.
///
/// \tparam T The type of result returned by Apply()
template <typename T>
class ICEBERG_EXPORT PendingUpdateTyped : public PendingUpdate {
 public:
  ~PendingUpdateTyped() override = default;

  /// \brief Apply the pending changes and return the uncommitted result
  ///
  /// This does not result in a permanent update.
  ///
  /// \return the uncommitted changes that would be committed, or an error:
  ///         - ValidationFailed: if pending changes cannot be applied
  ///         - InvalidArgument: if pending changes are conflicting
  virtual Result<T> Apply() = 0;

 protected:
  PendingUpdateTyped() = default;

  /// \brief Apply the pending changes to a TableMetadataBuilder
  ///
  /// Default implementation: calls Apply() to get the result, then applies it
  /// to the builder using ApplyResult().
  ///
  /// \param builder The TableMetadataBuilder to apply changes to
  /// \return Status::OK if the changes were applied successfully, or an error
  Status Apply(TableMetadataBuilder& builder) override {
    auto result = Apply();
    ICEBERG_RETURN_UNEXPECTED(result);

    return ApplyResult(builder, std::move(result.value()));
  }

  /// \brief Apply the result to a TableMetadataBuilder
  ///
  /// Subclasses must implement this method to apply the result of Apply()
  /// to the builder.
  ///
  /// \param builder The TableMetadataBuilder to apply the result to
  /// \param result The result from Apply()
  /// \return Status::OK if the result was applied successfully, or an error
  virtual Status ApplyResult(TableMetadataBuilder& builder, T result) = 0;
};

/// \brief Builder for updating (set/remove) table properties
///
/// This class provides a fluent API for setting or removing table properties within a
/// transaction. Mutations are accumulated and applied atomically when the transaction
/// is committed.
struct ICEBERG_EXPORT PropertiesUpdateChanges {
  std::unordered_map<std::string, std::string> updates;
  std::vector<std::string> removals;
};

class ICEBERG_EXPORT PropertiesUpdate
    : public PendingUpdateTyped<PropertiesUpdateChanges> {
 public:
  PropertiesUpdate() = default;
  ~PropertiesUpdate() override = default;

  PropertiesUpdate(const PropertiesUpdate&) = delete;
  PropertiesUpdate& operator=(const PropertiesUpdate&) = delete;

  /// \brief Set a property key-value pair
  ///
  /// \param key The property key
  /// \param value The property value
  /// \return Reference to this builder for method chaining
  PropertiesUpdate& Set(std::string const& key, std::string const& value);

  /// \brief Remove a property key
  ///
  /// \param key The property key to remove
  /// \return Reference to this builder for method chaining
  PropertiesUpdate& Remove(std::string const& key);

  /// \brief Apply the pending changes and return the uncommitted result
  ///
  /// \return The pending property updates/removals, or an error
  Result<PropertiesUpdateChanges> Apply() override;

  /// \brief Apply and commit the pending changes to the table
  ///
  /// \return Status::OK if the commit was successful, or an error
  Status Commit() override;

 private:
  Status ApplyResult(TableMetadataBuilder& builder,
                     PropertiesUpdateChanges result) override;

  std::unordered_map<std::string, std::string> updates_;
  std::vector<std::string> removals_;
};

}  // namespace iceberg
