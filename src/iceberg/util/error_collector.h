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

/// \file iceberg/util/error_collector.h
/// Utility for collecting validation errors in builder patterns

#include <string>
#include <vector>

#include "iceberg/result.h"

namespace iceberg {

/// \brief Utility class for collecting validation errors in builder patterns
///
/// This class provides error accumulation functionality for builders that
/// cannot throw exceptions. Builder methods can call AddError() to accumulate
/// validation errors, and CheckErrors() returns all errors at once.
///
/// This allows users to see all validation errors at once rather than fixing
/// them one by one (fail-slow instead of fail-fast).
///
/// Example usage:
/// \code
///   class MyBuilder {
///    protected:
///     ErrorCollector errors_;
///
///     MyBuilder& SetValue(int val) {
///       if (val < 0) {
///         errors_.AddError(ErrorKind::kInvalidArgument, "Value must be non-negative");
///         return *this;
///       }
///       value_ = val;
///       return *this;
///     }
///
///     Result<MyObject> Build() {
///       ICEBERG_RETURN_UNEXPECTED(errors_.CheckErrors());
///       return MyObject{value_};
///     }
///   };
/// \endcode
class ErrorCollector {
 public:
  ErrorCollector() = default;

  /// \brief Add a validation error
  ///
  /// \param kind The kind of error
  /// \param message The error message
  void AddError(ErrorKind kind, std::string message) {
    errors_.emplace_back(kind, std::move(message));
  }

  /// \brief Add an existing error object
  ///
  /// Useful when propagating errors from other components or reusing
  /// error objects without deconstructing and reconstructing them.
  ///
  /// \param err The error to add
  void AddError(Error err) { errors_.push_back(std::move(err)); }

  /// \brief Check if any errors have been collected
  ///
  /// \return true if there are accumulated errors
  bool HasErrors() const { return !errors_.empty(); }

  /// \brief Get the number of errors collected
  ///
  /// \return The count of accumulated errors
  size_t ErrorCount() const { return errors_.size(); }

  /// \brief Check for accumulated errors and return them if any exist
  ///
  /// This should be called before completing a builder operation (e.g.,
  /// in Build(), Apply(), or Commit() methods) to validate that no errors
  /// were accumulated during the builder method calls.
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

  /// \brief Get read-only access to all collected errors
  ///
  /// \return A const reference to the vector of errors
  const std::vector<Error>& Errors() const { return errors_; }

 private:
  std::vector<Error> errors_;
};

}  // namespace iceberg
