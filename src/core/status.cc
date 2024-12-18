// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#include "iceberg/status.h"

#include <cassert>
#include <cstdlib>
#include <iostream>

namespace iceberg {

Status::Status(StatusCode code, const std::string& msg)
    : Status::Status(code, msg, nullptr) {}

Status::Status(StatusCode code, std::string msg, std::shared_ptr<StatusDetail> detail) {
  state_ = new State{code, std::move(msg), std::move(detail)};
}

void Status::CopyFrom(const Status& s) {
  if (ICEBERG_PREDICT_FALSE(state_ != nullptr)) {
    DeleteState();
  }
  if (s.state_ == nullptr) {
    state_ = nullptr;
  } else {
    state_ = new State(*s.state_);
  }
}

std::string Status::CodeAsString() const {
  if (state_ == nullptr) {
    return "OK";
  }
  return CodeAsString(code());
}

std::string Status::CodeAsString(StatusCode code) {
  const char* type;
  switch (code) {
    case StatusCode::OK:
      type = "OK";
      break;
    case StatusCode::OutOfMemory:
      type = "Out of memory";
      break;
    case StatusCode::TypeError:
      type = "Type error";
      break;
    case StatusCode::Invalid:
      type = "Invalid";
      break;
    case StatusCode::IOError:
      type = "IOError";
      break;
    case StatusCode::NotImplemented:
      type = "NotImplemented";
      break;
    case StatusCode::UnknownError:
      type = "Unknown error";
      break;
    default:
      type = "Unknown";
      break;
  }
  return std::string(type);
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == nullptr) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  if (state_->detail != nullptr) {
    result += ". Detail: ";
    result += state_->detail->ToString();
  }

  return result;
}

void Status::Warn() const { std::cerr << ToString(); }

void Status::Warn(const std::string& message) const {
  std::cerr << message << ": " << ToString();
}

}  // namespace iceberg
