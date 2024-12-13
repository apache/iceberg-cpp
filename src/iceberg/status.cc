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

#include "iceberg/status.h"

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <sstream>

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
  std::ostringstream oss;
  oss << CodeAsString();
  if (state_ == nullptr) {
    return oss.str();
  }
  oss << ": ";
  oss << state_->msg;
  if (state_->detail != nullptr) {
    oss << ". Detail: " << state_->detail->ToString();
  }

  return oss.str();
}

void Status::Warn() const { std::cerr << *this; }

void Status::Warn(const std::string& message) const {
  std::cerr << message << ": " << *this;
}

}  // namespace iceberg
