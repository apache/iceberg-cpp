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
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

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
/// Error Collection Pattern:
/// Builder methods in subclasses should use AddError() to collect validation
/// errors instead of returning immediately. The accumulated errors will be
/// returned by Apply() or Commit() when they are called. This allows users
/// to see all validation errors at once rather than fixing them one by one.
///
/// Example usage in a subclass:
/// \code
///   MyUpdate& SetProperty(std::string_view key, std::string_view value) {
///     if (key.empty()) {
///       AddError(ErrorKind::kInvalidArgument, "Property key cannot be empty");
///       return *this;
///     }
///     // ... continue with normal logic
///     return *this;
///   }
///
///   Result<T> Apply() override {
///     // Check for accumulated errors first
///     ICEBERG_RETURN_IF_ERROR(CheckErrors());
///     // ... proceed with apply logic
///   }
/// \endcode
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

  /// \brief Add a validation error to be returned later
  ///
  /// Errors are accumulated and will be returned by Apply() or Commit().
  /// This allows builder methods to continue and collect all errors rather
  /// than failing fast on the first error.
  ///
  /// \param kind The kind of error
  /// \param message The error message
  void AddError(ErrorKind kind, std::string message) {
    errors_.emplace_back(kind, std::move(message));
  }

  /// \brief Check if any errors have been collected
  ///
  /// \return true if there are accumulated errors
  bool HasErrors() const { return !errors_.empty(); }

  /// \brief Check for accumulated errors and return them if any exist
  ///
  /// This should be called at the beginning of Apply() or Commit() to
  /// return all accumulated validation errors.
  ///
  /// \return Status::OK if no errors, or a ValidationFailed error with
  ///         all accumulated error messages
  Status CheckErrors() const {
    if (!errors_.empty()) {
      std::string error_msg = "Validation failed due to the following errors:\n";
      for (const auto& [kind, message] : errors_) {
        error_msg += "  - " + message + "\n";
      }
      return ValidationFailed("{}", error_msg);
    }
    return {};
  }

  /// \brief Clear all accumulated errors
  ///
  /// This can be useful for resetting the error state in tests or
  /// when reusing a builder instance.
  void ClearErrors() { errors_.clear(); }

 private:
  // Error collection (since builder methods return *this and cannot throw)
  std::vector<Error> errors_;
};

}  // namespace iceberg
