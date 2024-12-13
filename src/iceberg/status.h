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

#include <cstring>
#include <format>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

#include "util/compare.h"
#include "util/macros.h"
#include "util/to_string_ostreamable.h"

namespace iceberg {

enum class StatusCode : char {
  OK = 0,
  IOError = 1,
  NotImplemented = 2,
  UnknownError = 127
};

/// \brief An opaque class that allows subsystems to retain
/// additional information inside the Status.
class StatusDetail {
 public:
  virtual ~StatusDetail() = default;
  /// \brief Return a unique id for the type of the StatusDetail
  /// (effectively a poor man's substitute for RTTI).
  virtual const char* type_id() const = 0;
  /// \brief Produce a human-readable description of this status.
  virtual std::string ToString() const = 0;

  bool operator==(const StatusDetail& other) const noexcept {
    return std::string(type_id()) == other.type_id() && ToString() == other.ToString();
  }
};

/// \brief Status outcome object (success or error)
///
/// The Status object is an object holding the outcome of an operation.
/// The outcome is represented as a StatusCode, either success
/// (StatusCode::OK) or an error (any other of the StatusCode enumeration values).
///
/// Additionally, if an error occurred, a specific error message is generally
/// attached.
class [[nodiscard]] Status : public util::EqualityComparable<Status>,
                             public util::ToStringOstreamable<Status> {
 public:
  // Create a success status.
  constexpr Status() noexcept : state_(nullptr) {}
  ~Status() noexcept {
    if (ICEBERG_PREDICT_FALSE(state_ != nullptr)) {
      DeleteState();
    }
  }

  Status(StatusCode code, const std::string& msg);
  /// \brief Pluggable constructor for use by sub-systems. detail cannot be null.
  Status(StatusCode code, std::string msg, std::shared_ptr<StatusDetail> detail);

  // Copy the specified status.
  inline Status(const Status& s);
  inline Status& operator=(const Status& s);

  // Move the specified status.
  inline Status(Status&& s) noexcept;
  inline Status& operator=(Status&& s) noexcept;

  inline bool Equals(const Status& other) const;

  // AND the statuses.
  inline Status operator&(const Status& s) const noexcept;
  inline Status operator&(Status&& s) const noexcept;
  inline Status& operator&=(const Status& s) noexcept;
  inline Status& operator&=(Status&& s) noexcept;

  /// Return a success status.
  static Status OK() { return Status(); }

  template <typename... Args>
  static Status FromArgs(StatusCode code, std::string_view fmt, Args&&... args) {
    return Status(code, std::vformat(fmt, std::make_format_args(args...)));
  }

  template <typename... Args>
  static Status FromDetailAndArgs(StatusCode code, std::shared_ptr<StatusDetail> detail,
                                  std::string_view fmt, Args&&... args) {
    return Status(code, std::vformat(fmt, std::make_format_args(args...)),
                  std::move(detail));
  }

  /// Return an error status when some IO-related operation failed
  template <typename... Args>
  static Status IOError(Args&&... args) {
    return Status::FromArgs(StatusCode::IOError, std::forward<Args>(args)...);
  }

  /// Return an error status when an operation or a combination of operation and
  /// data types is unimplemented
  template <typename... Args>
  static Status NotImplemented(Args&&... args) {
    return Status::FromArgs(StatusCode::NotImplemented, std::forward<Args>(args)...);
  }

  /// Return an error status for unknown errors
  template <typename... Args>
  static Status UnknownError(Args&&... args) {
    return Status::FromArgs(StatusCode::UnknownError, std::forward<Args>(args)...);
  }

  /// Return true iff the status indicates success.
  constexpr bool ok() const { return (state_ == nullptr); }

  /// Return true iff the status indicates an IO-related failure.
  constexpr bool IsIOError() const { return code() == StatusCode::IOError; }
  /// Return true iff the status indicates an unimplemented operation.
  constexpr bool IsNotImplemented() const { return code() == StatusCode::NotImplemented; }
  /// Return true iff the status indicates an unknown error.
  constexpr bool IsUnknownError() const { return code() == StatusCode::UnknownError; }

  /// \brief Return a string representation of this status suitable for printing.
  ///
  /// The string "OK" is returned for success.
  std::string ToString() const;

  /// \brief Return a string representation of the status code, without the message
  /// text or POSIX code information.
  std::string CodeAsString() const;
  static std::string CodeAsString(StatusCode);

  /// \brief Return the StatusCode value attached to this status.
  constexpr StatusCode code() const { return ok() ? StatusCode::OK : state_->code; }

  /// \brief Return the specific error message attached to this status.
  const std::string& message() const {
    static const std::string no_message = "";
    return ok() ? no_message : state_->msg;
  }

  /// \brief Return the status detail attached to this message.
  const std::shared_ptr<StatusDetail>& detail() const {
    static std::shared_ptr<StatusDetail> no_detail = nullptr;
    return state_ ? state_->detail : no_detail;
  }

  /// \brief Return a new Status copying the existing status, but
  /// updating with the existing detail.
  Status WithDetail(std::shared_ptr<StatusDetail> new_detail) const {
    return Status(code(), message(), std::move(new_detail));
  }

  /// \brief Return a new Status with changed message, copying the
  /// existing status code and detail.
  template <typename... Args>
  Status WithMessage(Args&&... args) const {
    return FromArgs(code(), std::forward<Args>(args)...).WithDetail(detail());
  }

  void Warn() const;
  void Warn(const std::string& message) const;

 private:
  struct State {
    StatusCode code;
    std::string msg;
    std::shared_ptr<StatusDetail> detail;
  };
  // OK status has a `NULL` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  State* state_;

  void DeleteState() noexcept {
    // On certain compilers, splitting off the slow path improves performance
    // significantly.
    delete state_;
    state_ = nullptr;
  }

  void CopyFrom(const Status& s);
  inline void MoveFrom(Status& s);
};

void Status::MoveFrom(Status& s) {
  if (ICEBERG_PREDICT_FALSE(state_ != nullptr)) {
    DeleteState();
  }
  state_ = s.state_;
  s.state_ = nullptr;
}

Status::Status(const Status& s) : state_{nullptr} { CopyFrom(s); }

Status& Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    CopyFrom(s);
  }
  return *this;
}

Status::Status(Status&& s) noexcept : state_(s.state_) { s.state_ = nullptr; }

Status& Status::operator=(Status&& s) noexcept {
  MoveFrom(s);
  return *this;
}

bool Status::Equals(const Status& s) const {
  if (state_ == s.state_) {
    return true;
  }

  if (ok() || s.ok()) {
    return false;
  }

  if (detail() != s.detail()) {
    if ((detail() && !s.detail()) || (!detail() && s.detail())) {
      return false;
    }
    return *detail() == *s.detail();
  }

  return code() == s.code() && message() == s.message();
}

Status Status::operator&(const Status& s) const noexcept {
  if (ok()) {
    return s;
  } else {
    return *this;
  }
}

Status Status::operator&(Status&& s) const noexcept {
  if (ok()) {
    return std::move(s);
  } else {
    return *this;
  }
}

Status& Status::operator&=(const Status& s) noexcept {
  if (ok() && !s.ok()) {
    CopyFrom(s);
  }
  return *this;
}

Status& Status::operator&=(Status&& s) noexcept {
  if (ok() && !s.ok()) {
    MoveFrom(s);
  }
  return *this;
}

}  // namespace iceberg
