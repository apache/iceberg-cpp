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
/// Base class for collecting validation errors in builder patterns

#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

#define BUILDER_RETURN_IF_ERROR(result)                         \
  if (auto&& result_name = result; !result_name) [[unlikely]] { \
    errors_.emplace_back(std::move(result_name.error()));       \
    return *this;                                               \
  }

#define BUILDER_ASSIGN_OR_RETURN_IMPL(result_name, lhs, rexpr) \
  auto&& result_name = (rexpr);                                \
  BUILDER_RETURN_IF_ERROR(result_name)                         \
  lhs = std::move(result_name.value());

#define BUILDER_ASSIGN_OR_RETURN(lhs, rexpr)                                             \
  BUILDER_ASSIGN_OR_RETURN_IMPL(ICEBERG_ASSIGN_OR_RAISE_NAME(result_, __COUNTER__), lhs, \
                                rexpr)

/// \brief Base class for collecting validation errors in builder patterns
///
/// This class provides error accumulation functionality for builders that
/// cannot throw exceptions. Builder methods can call AddError() to accumulate
/// validation errors, and CheckErrors() returns all errors at once.
///
/// Example usage:
/// \code
///   class MyBuilder : public ErrorCollectorBase {
///    public:
///     MyBuilder& SetValue(int val) {
///       if (val < 0) {
///         return AddError(ErrorKind::kInvalidArgument, "Value must be non-negative");
///       }
///       value_ = val;
///       return *this;
///     }
///
///     Result<MyObject> Build() {
///       ICEBERG_RETURN_UNEXPECTED(CheckErrors());
///       return MyObject{value_};
///     }
///
///    private:
///     int value_ = 0;
///   };
/// \endcode
class ICEBERG_EXPORT ErrorCollector {
 public:
  ErrorCollector() = default;
  virtual ~ErrorCollector() = default;

  ErrorCollector(ErrorCollector&&) = default;
  ErrorCollector& operator=(ErrorCollector&&) = default;

  ErrorCollector(const ErrorCollector&) = default;
  ErrorCollector& operator=(const ErrorCollector&) = default;

  /// \brief Add a specific error and return reference to derived class
  ///
  /// \param self Deduced reference to the derived class instance
  /// \param kind The kind of error
  /// \param message The error message
  /// \return Reference to the derived class for method chaining
  auto& AddError(this auto& self, ErrorKind kind, std::string message) {
    self.errors_.emplace_back(kind, std::move(message));
    return self;
  }

  /// \brief Add an existing error object and return reference to derived class
  ///
  /// Useful when propagating errors from other components or reusing
  /// error objects without deconstructing and reconstructing them.
  ///
  /// \param self Deduced reference to the derived class instance
  /// \param err The error to add
  /// \return Reference to the derived class for method chaining
  auto& AddError(this auto& self, Error err) {
    self.errors_.push_back(std::move(err));
    return self;
  }

  /// \brief Check if any errors have been collected
  ///
  /// \return true if there are accumulated errors
  [[nodiscard]] bool HasErrors() const { return !errors_.empty(); }

  /// \brief Get the number of errors collected
  ///
  /// \return The count of accumulated errors
  [[nodiscard]] size_t ErrorCount() const { return errors_.size(); }

  /// \brief Check for accumulated errors and return them if any exist
  ///
  /// This should be called before completing a builder operation (e.g.,
  /// in Build(), Apply(), or Commit() methods) to validate that no errors
  /// were accumulated during the builder method calls.
  ///
  /// \return Status::OK if no errors, or a ValidationFailed error with
  ///         all accumulated error messages
  [[nodiscard]] Status CheckErrors() const {
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
  [[nodiscard]] const std::vector<Error>& Errors() const { return errors_; }

 protected:
  std::vector<Error> errors_;
};

}  // namespace iceberg
