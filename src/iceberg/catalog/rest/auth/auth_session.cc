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

#include "iceberg/catalog/rest/auth/auth_session.h"

#include <utility>

namespace iceberg::rest::auth {

namespace {

/// \brief An empty session implementation that does nothing.
class EmptyAuthSession : public AuthSession {
 public:
  void Authenticate(
      [[maybe_unused]] std::unordered_map<std::string, std::string>& headers) override {
    // No-op: empty session does not add any authentication
  }

  void Close() override {
    // No resources to release
  }
};

}  // namespace

std::shared_ptr<AuthSession> AuthSession::Empty() {
  // Use a static local variable for thread-safe singleton initialization
  static auto empty_session = std::make_shared<EmptyAuthSession>();
  return empty_session;
}

DefaultAuthSession::DefaultAuthSession(
    std::unordered_map<std::string, std::string> headers)
    : headers_(std::move(headers)) {}

void DefaultAuthSession::Authenticate(
    std::unordered_map<std::string, std::string>& headers) {
  for (const auto& [key, value] : headers_) {
    headers.try_emplace(key, value);
  }
}

std::shared_ptr<DefaultAuthSession> DefaultAuthSession::Of(
    std::unordered_map<std::string, std::string> headers) {
  return std::make_shared<DefaultAuthSession>(std::move(headers));
}

}  // namespace iceberg::rest::auth
